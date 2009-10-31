#!/usr/bin/env python

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
# * XXX Support multiple cache directories (but keep sane default)
# * Support binding to only certain interfaces
# * Have group awareness so we can timeout before 0.500 seconds
#   if everyone has responded
# * Add a default mirror capability

from __future__ import with_statement

import os, re, socket, threading
import sys

try:
    import cPickle as pickle
except ImportError:
    import pickle

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor, defer, threads

from twisted.application.internet import MulticastServer

from twisted.python import log

from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.static import File
from twisted.web.util import Redirect
from twisted.web.error import NoResource

DEF_ADDR = '224.0.0.156'
DEF_PORT = 8954
MCAST = (DEF_ADDR, DEF_PORT)

cachedirs = [ "/var/cache/pacman/pkg",
              "/var/cache/makepkg/pkg" ]

class PackageRequest():
    def __init__(self, queue, package):
        print "new request: %s" % package
        self.event = threading.Event()
        self.queue = queue
        self.pkgname = package
        self.address = None

    def wait(self):
        self.event.wait(0.500)
        queue.remove(self)
        return self.address


class PackageRequestQueue():
    requests = []
    lock = threading.Lock()

    def new(self, package):
        with self.lock:
            ret = PackageRequest(self, package)
            self.requests.append(ret)
            return ret

    def found(self, package, address):
        with self.lock:
            for r in self.requests:
                if r.pkgname == package:
                    r.address = address
                    r.event.set()

    def notfound(self, package, address):
        # TODO
        pass
    
    def remove(self, item):
        with self.lock:
            self.requests.remove(item)

    def debug(self):
        print repr(self.requests)

def find_package(pkgfile):
    for dir in cachedirs:
        if os.path.isdir(dir):
            path = os.path.join(dir, pkgfile)
            if os.path.isfile(path) and os.access(path, os.R_OK):
                return path
    return None

def find_remote_package(pkgfile):
    print "multicast search: %s" % pkgfile
    mcastserver.write("search:%s" % pkgfile, MCAST)

class MulticastPackageFinder(DatagramProtocol):
    def __init__(self, queue):
        self.queue = queue

    def leave(self):
        self.send("gone:")

    def startProtocol(self):
        # join our group, don't listen to ourself, and announce our presence
        self.transport.setLoopbackMode(False)
        self.transport.joinGroup(DEF_ADDR)
        self.send("ping:")
        reactor.addSystemEventTrigger("before", "shutdown", self.leave)

    def send(self, message):
        self.transport.write(message, MCAST)

    message_re = re.compile('(.*?):(.*)')
    def parse_message(self, message):
        m = self.message_re.match(message)
        if m:
            return m.groups()[0], m.groups()[1]
        else: 
            return None, None

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
        print "received: %s %s" % (address[0], datagram)
        type, contents = self.parse_message(datagram)
        if type == "search":
            path = find_package(contents)
            if path != None:
                self.send("found:%s" % contents)
            else:
                self.send("notfound:%s" % contents)
        elif type == "found":
            self.queue.found(contents, address[0])
        elif type == "notfound":
            self.queue.notfound(contents, address[0])
        elif type == "ping":
            self.send("pong:")
        elif type == "pong":
            pass
        elif type == "gone":
            pass


class MulticastPackageResource(Resource):
    def __init__(self, queue):
        self.queue = queue
        Resource.__init__(self)

    def deferred_search(self, request):
        pkgname = request.prepath[-1]
        qi = self.queue.new(pkgname)
        find_remote_package(pkgname)
        address = qi.wait()
        if address != None:
            print "found: %s %s" % (address, pkgname)
            resource = Redirect("http://%s:%s/cache/%s" % (address, str(DEF_PORT), pkgname))
            resource.render(request)
        else:
            print "not found: %s" % pkgname
            resource = NoResource()
            resource.render(request)
        request.finish()
        return request

    def render_GET(self, request):
        d = threads.deferToThread(self.deferred_search, request)
        return NOT_DONE_YET


# package serving and searching setup
class SearchResource(Resource):
    def __init__(self, queue):
        self.queue = queue
        Resource.__init__(self)

    def is_allowed(self, path):
        '''
        There are certain things we don't want to ever share. These include
        databases and old-style package names without an architecture. You may
        ask "why not use different ports per architecture?" If we did this,
        you'd get no benefit when it comes to arch=any packages, which is the
        whole idea of them.
        '''
        if path.endswith(".db.tar.gz"):
            return False
        oldskool = re.compile('.*-[0-9\.]+\.pkg\.tar\.gz$')
        if oldskool.match(path):
            return False
        return True

    def getChild(self, path, request):
        # if we aren't at the last level, try to get there
        if len(request.postpath) > 0:
            return SearchResource(self.queue)
        print "search request: %s" % path
        if not self.is_allowed(path):
            return NoResource()
        # the local case is the easiest- check our own cache first
        localpath = find_package(path)
        if localpath != None:
            return File(localpath)
        # ok, time to multicast search
        return MulticastPackageResource(self.queue)


class CacheResource(Resource):
    def getChild(self, path, request):
        localpath = find_package(path)
        if localpath != None:
            return File(localpath)
        return NoResource()
 
log.startLogging(sys.stdout)

queue = PackageRequestQueue()

httproot = Resource()
httproot.putChild("search", SearchResource(queue))
httproot.putChild("cache", CacheResource())

# get our various listeners, clients, etc. set up
mcastserver = reactor.listenMulticast(DEF_PORT, MulticastPackageFinder(queue))
httpserver = reactor.listenTCP(DEF_PORT, Site(httproot))

reactor.run()

# vim: set et:
