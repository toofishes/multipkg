#!/usr/bin/env python2

# Copyright (c) 2009 Dan McGee.
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. The name of the author may not be used to endorse or promote products
#    derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
# NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# TODO list:
# * Support configuration in some fashion of cache directories
# * Support binding to only certain interfaces
# * Add a default mirror capability
# * Improve group awareness. We should look at sending out pings/pongs
#   and expiring hosts we don't hear from. A few UDP packages every minute
#   won't hurt anyone.
# * Allow for some fallback to known hosts. This would involve sending
#   a UDP packet directly to a host rather than the multicast address, and
#   then having that host act as a bridge.
#
#   [Machine A] <--- UDP unicast ---> [Machine B]     [Machine C]
#                                                \   /
#                                           <UDP multicast>
#                                                  |
#                                             [Machine D]
#
# * To do the above, we will probably have to beef up the data sent on
#   the wire. It might be smart to pickle something up where the following
#   keys are known:
#     - req: ping, pong, gone, search, found, notfound
#     - pkg: package name if it makes sense
#     - dest: dest address so we can determine multicast/unicast
#     - reqid: some unique ID so we don't propagate too many packets?

from __future__ import with_statement

import os
import re
import threading
import time
import sys

try:
    import cPickle as pickle
except ImportError:
    import pickle

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor, threads

from twisted.python import log

from twisted.web.resource import NoResource, Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.static import File
from twisted.web.util import Redirect

DEF_ADDR = '239.0.0.156'
DEF_PORT = 8954
MCAST = (DEF_ADDR, DEF_PORT)

cachedirs = [ "/var/cache/pacman/pkg",
              "/var/cache/makepkg/pkg" ]


class KnownServer(object):
    def __init__(self, address, contact_address=None):
        self.last = time.time()
        self.address = address
        if contact_address:
            self.contact_address = contact_address
        else:
            self.contact_address = address


class ServerCache(object):
    '''
    This class is meant to hold a cache of servers we know about *and* have
    heard from in a configured time interval. This is to ensure we have a
    relatively fresh list of servers without having to constantly poll.
    '''
    known_servers = dict()
    lock = threading.Lock()

    def __init__(self, timeout=60):
        self.timeout = timeout

    def add(self, from_addr, contact_addr=None):
        s = KnownServer(from_addr, contact_addr)
        with self.lock:
            self.known_servers[from_addr] = s

    def remove(self, from_addr, contact_addr=None):
        with self.lock:
            if from_addr in self.known_servers:
                del self.known_servers[from_addr]

    def get_current(self):
        '''
        Get a list of servers we know about and should be available. This
        method is also responsible for doing the lazy sweep of the dict for
        dead servers.
        '''
        cutoff = time.time() - self.timeout
        ret = set()
        with self.lock:
            for k in self.known_servers.keys():
                s = self.known_servers[k]
                if s.last < cutoff:
                    del self.known_servers[k]
                else:
                    ret.add(s.address)
        return ret


class PackageRequest(object):
    def __init__(self, queue, package, known):
        print("new request: %s" % package)
        self.event = threading.Event()
        self.queue = queue
        self.pkgname = package
        self.known_servers = known
        self.address = None

    def wait(self):
        self.event.wait(0.500)
        print("request done waiting, address %s servers remaining %d" \
                % (self.address, len(self.known_servers)))
        queue.remove(self)
        return self.address


class PackageRequestQueue():
    requests = []
    server_cache = ServerCache()
    lock = threading.Lock()

    def new(self, package):
        servers = self.server_cache.get_current()
        ret = PackageRequest(self, package, servers)
        with self.lock:
            self.requests.append(ret)
        return ret

    def found(self, package, address):
        with self.lock:
            for r in self.requests:
                if r.pkgname == package:
                    r.address = address
                    r.event.set()

    def notfound(self, package, address):
        with self.lock:
            for r in self.requests:
                if r.pkgname == package:
                    r.known_servers.discard(address)
                    # if we don't know of any more servers, clear the flag
                    if len(r.known_servers) == 0:
                        r.event.set()
    
    def remove(self, item):
        with self.lock:
            self.requests.remove(item)

    def add_server(self, server):
        self.server_cache.add(server)

    def remove_server(self, server):
        self.server_cache.remove(server)

    def debug(self):
        print(repr(self.requests))
        print(repr(self.server_cache.known_servers))


def find_package(pkgfile):
    for dir in cachedirs:
        if os.path.isdir(dir):
            path = os.path.join(dir, pkgfile)
            if os.path.isfile(path) and os.access(path, os.R_OK):
                return path
    return None

class MulticastPackageFinder(DatagramProtocol):
    def __init__(self, queue):
        self.queue = queue

    def build_message(self, type, dest=DEF_ADDR, pkg=None):
        message = { "type": type, "dest": dest, "pkg": pkg }
        message = pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)
        addr = (dest, DEF_PORT)
        self.transport.write(message, addr)

    def parse_message(self, message):
        try:
            parsed = pickle.loads(message)
            return parsed["type"], parsed
        except pickle.PickleError:
            return None, dict()

    def leave(self):
        self.build_message("gone")

    def ping_loop(self, dest):
        self.build_message("ping", dest)
        reactor.callLater(10, self.ping_loop, dest)

    def pong_loop(self):
        self.build_message("pong")
        reactor.callLater(50, self.pong_loop)

    def search(self, pkgfile):
        print("multicast search: %s" % pkgfile)
        self.build_message("search", pkg=pkgfile)

    def startProtocol(self):
        # join our group, don't listen to ourself, and announce our presence
        self.transport.setLoopbackMode(False)
        self.transport.joinGroup(DEF_ADDR)
        self.transport.setTTL(2)
        self.build_message("ping")
        reactor.addSystemEventTrigger("before", "shutdown", self.leave)
        reactor.callLater(30, self.pong_loop)

    def datagramReceived(self, datagram, address):
        '''
        The important stuff happens here. When we get a multicast message in,
        we want to process it and take action as necessary. Currently the
        following message types are known:
        * search - sender is looking for a file with the given name
        * found - sender has the file with the given name
        * notfound - sender does NOT have the file with the given name
        * ping - sender is looking for other clustermates
        * pong - sender is announcing presence in cluster
        * gone - sender is about to shutdown and leave cluster
        '''
        addr = address[0]
        type, contents = self.parse_message(datagram)
        print("received: %s %s" % (addr, type))
        pkg = contents.get("pkg")
        if type == "search":
            path = find_package(pkg)
            if path != None:
                self.build_message("found", pkg=pkg)
            else:
                self.build_message("notfound", pkg=pkg)
        elif type == "found":
            self.queue.found(pkg, addr)
        elif type == "notfound":
            self.queue.notfound(pkg, addr)
        elif type == "ping":
            self.queue.add_server(addr)
            self.build_message("pong")
        elif type == "pong":
            self.queue.add_server(addr)
        elif type == "gone":
            self.queue.remove_server(addr)


class MulticastPackageResource(Resource):
    def __init__(self, queue, finder):
        self.queue = queue
        self.finder = finder
        Resource.__init__(self)

    def deferred_search(self, request):
        pkgname = request.prepath[-1]
        qi = self.queue.new(pkgname)
        finder.search(pkgname)
        address = qi.wait()
        if address != None:
            print("found: %s %s" % (address, pkgname))
            resource = Redirect("http://%s:%s/cache/%s" % (address, str(DEF_PORT), pkgname))
            resource.render(request)
        else:
            print("not found: %s" % pkgname)
            resource = NoResource()
            resource.render(request)
        request.finish()
        return request

    def render_GET(self, request):
        threads.deferToThread(self.deferred_search, request)
        return NOT_DONE_YET


# package serving and searching setup
class SearchResource(Resource):
    def __init__(self, queue, finder):
        self.queue = queue
        self.finder = finder
        Resource.__init__(self)

    def is_allowed(self, path):
        '''
        There are certain things we don't want to ever share. These include
        databases and old-style package names without an architecture. You may
        ask "why not use different ports per architecture?" If we did this,
        you'd get no benefit when it comes to arch=any packages, which is the
        whole idea of them.
        '''
        if path.endswith(".db"):
            return False
        oldskool = re.compile('.*-[0-9\.]+\.pkg\.tar\.gz$')
        if oldskool.match(path):
            return False
        return True

    def getChild(self, path, request):
        # if we aren't at the last level, try to get there
        if len(request.postpath) > 0:
            return SearchResource(self.queue, self.finder)
        print("search request: %s" % path)
        if not self.is_allowed(path):
            return NoResource()
        # the local case is the easiest- check our own cache first
        localpath = find_package(path)
        if localpath != None:
            return File(localpath)
        # ok, time to multicast search
        return MulticastPackageResource(self.queue, self.finder)


class CacheResource(Resource):
    def getChild(self, path, request):
        localpath = find_package(path)
        if localpath != None:
            return File(localpath)
        return NoResource()
 

if __name__ == '__main__':
    log.startLogging(sys.stdout)

    queue = PackageRequestQueue()
    finder = MulticastPackageFinder(queue)

    httproot = Resource()
    httproot.putChild("search", SearchResource(queue, finder))
    httproot.putChild("cache", CacheResource())

    # get our various listeners, clients, etc. set up
    mcastserver = reactor.listenMulticast(DEF_PORT, finder)
    httpserver = reactor.listenTCP(DEF_PORT, Site(httproot))

    reactor.run()

# vim: set et:
