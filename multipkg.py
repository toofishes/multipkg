#!/usr/bin/env python

from __future__ import with_statement

import os, socket, threading

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor, defer

from twisted.application.internet import MulticastServer

from twisted.web.resource import Resource
from twisted.web.server import Site, NOT_DONE_YET
from twisted.web.static import File
from twisted.web.util import Redirect
from twisted.web.error import NoResource

DEF_ADDR = '224.0.0.156'
DEF_PORT = 36992

class PackageCache():
    cache = dict()
    lock = threading.Lock()

    def clear(self):
        with self.lock:
            self.cache.clear()

    def add(self, package, address):
        with self.lock:
            if package not in self.cache:
                self.cache[package] = []
            addrs = self.cache.get(package)
            if address not in addrs:
                self.cache.get(package).append(address)

    def get(self, package):
        ret = None
        with self.lock:
            if package in self.cache:
                ret = self.cache.get(package)[0]
        return ret

    def debug(self):
        print repr(self.cache)

def find_package(pkgfile):
    # TODO multiple cache directories
    path = "/var/cache/pacman/pkg/%s" % pkgfile
    if os.path.isfile(path) and os.access(path, os.R_OK):
        return path
    return None

def find_remote_package(pkgfile):
    global mcastclient
    print "looking for %s" % pkgfile
    mcastclient.write(pkgfile, (DEF_ADDR, DEF_PORT))

class MulticastPackageServer(DatagramProtocol):
    def startProtocol(self):
        print 'Started Listening'
        self.transport.joinGroup(DEF_ADDR)

    def stopProtocol(self):
        print 'Stopped Listening'

    def datagramReceived(self, datagram, address):
        print "Server Received:" + repr(datagram) + repr(address)
        path = find_package(datagram)
        if path != None:
            self.transport.write("yes:%s" % datagram, address)
        else:
            self.transport.write("no:%s" % datagram, address)


class MulticastPackageClient(DatagramProtocol):
    def datagramReceived(self, datagram, address):
        print "Client Received:" + repr(datagram) + repr(address)
        if datagram.startswith("yes:"):
            global cache
            cache.add(datagram[4:], address[0])
        cache.debug()


class MulticastPackageResource(Resource):
    def _delayedRender(self, request):
        global cache
        cache.debug()
        address = cache.get(request.prepath[-1])
        if address != None:
            print "package found at %s" % address
            resource = Redirect("http://%s:%s/cache/%s" % (address, "8080", request.prepath[-1]))
            resource.render(request)
        else:
            print "returning not found"
            resource = NoResource()
            resource.render(request)
        request.finish()
        return request

    def render_GET(self, request):
        print repr(request)
        find_remote_package(request.prepath[-1])
        d = defer.Deferred()
        d.addCallback(self._delayedRender)
        reactor.callLater(0.400, d.callback, request)
        return NOT_DONE_YET


# package serving and searching setup
class SearchResource(Resource):
    def getChild(self, path, request):
        print "req: %s" % path
        # the local case is the easiest- check our own cache first
        localpath = find_package(path)
        if localpath != None:
            #return Redirect("/cache/%s" % path)
			return File(localpath)
        # ok, time to multicast search
        return MulticastPackageResource()

cache = PackageCache()

httproot = Resource()
httproot.putChild("cache", File("/var/cache/pacman/pkg"))
httproot.putChild("search", SearchResource())

# get our various listeners, clients, etc. set up
mcastserver = reactor.listenMulticast(DEF_PORT, MulticastPackageServer())
mcastclient = reactor.listenUDP(0, MulticastPackageClient())
reactor.listenTCP(8080, Site(httproot))

reactor.run()
