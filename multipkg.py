#!/usr/bin/env python


# TODO list:
# * Support multiple cache directories (but keep sane default)
# * Support binding to only certain interfaces
# * XXX Exclude databases from being found elsewhere
# * XXX Support full file paths
# * Have group awareness so we can timeout before 0.500 seconds
#   if everyone has responded
# * XXX Ignore multicast messages from self

from __future__ import with_statement

import os, re, socket, threading

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor, defer, threads

from twisted.application.internet import MulticastServer

from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.static import File
from twisted.web.util import Redirect
from twisted.web.error import NoResource

DEF_ADDR = '224.0.0.156'
DEF_PORT = 8954
MCAST = (DEF_ADDR, DEF_PORT)

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
    
    def remove(self, item):
        with self.lock:
            self.requests.remove(item)

    def debug(self):
        print repr(self.requests)

def find_package(pkgfile):
    # TODO multiple cache directories
    path = "/var/cache/pacman/pkg/%s" % pkgfile
    if os.path.isfile(path) and os.access(path, os.R_OK):
        return path
    return None

def find_remote_package(pkgfile):
    global mcastserver
    print "multicast search: %s" % pkgfile
    mcastserver.write("search:%s" % pkgfile, MCAST)

class MulticastPackageFinder(DatagramProtocol):
    def startProtocol(self):
        self.transport.setLoopbackMode(False)
        self.transport.joinGroup(DEF_ADDR)

    message_re = re.compile('(.*?):(.*)')
    def parse_message(self, message):
        m = self.message_re.match(message)
        if m:
            return m.groups()[0], m.groups()[1]
        else: 
            return None, None

    def datagramReceived(self, datagram, address):
        global queue
        print "received: %s %s" % (address[0], datagram)
        type, contents = self.parse_message(datagram)
        if type == "search":
            path = find_package(contents)
            if path != None:
                self.transport.write("yes:%s" % contents, MCAST)
            else:
                self.transport.write("no:%s" % contents, MCAST)
        elif type == "yes":
            queue.found(contents, address[0])


class MulticastPackageResource(Resource):
    def deferred_search(self, request):
        global queue
        pkgname = request.prepath[-1]
        qi = queue.new(pkgname)
        find_remote_package(pkgname)
        address = qi.wait()
        if address != None:
            print "found: %s %s" % (address, pkgname)
            resource = Redirect("http://%s:%s/cache/%s" % (address, "8080", request.prepath[-1]))
            resource.render(request)
        else:
            resource = NoResource()
            resource.render(request)
        request.finish()
        return request

    def render_GET(self, request):
        d = threads.deferToThread(self.deferred_search, request)
        return NOT_DONE_YET


# package serving and searching setup
class SearchResource(Resource):
    def is_database(self, path):
        return path.endswith(".db.tar.gz")

    def getChild(self, path, request):
        # if we aren't at the last level, try to get there
        if len(request.postpath) > 0:
            return SearchResource()
        print "search request: %s" % path
        if self.is_database(path):
            return NoResource()
        # the local case is the easiest- check our own cache first
        localpath = find_package(path)
        if localpath != None:
            return File(localpath)
        # ok, time to multicast search
        return MulticastPackageResource()

queue = PackageRequestQueue()

httproot = Resource()
httproot.putChild("cache", File("/var/cache/pacman/pkg"))
httproot.putChild("search", SearchResource())

# get our various listeners, clients, etc. set up
mcastserver = reactor.listenMulticast(DEF_PORT, MulticastPackageFinder())
reactor.listenTCP(8080, Site(httproot))

reactor.run()

# vim: set et:
