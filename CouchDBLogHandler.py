"""
couchdb-log

A python logging handler using CouchDB for log storage

Requires Python 2.6+, or an earlier version with the simplejson module installed
"""

import time
import logging

#- begin socket hack -------------------
# this is necessary to get urllib2 to work on addresses which only support IPv4
# because urllib2 attempts to open the IPv6 address first, requiring a 1 sec timeout
# thus, this is a workaround for a bug in Python's stdlib!
#
# Thanks go to:
# Fellyn Silliman at Funcom for pointing out the urllib2 limitation
# Andre Holzner on StackOverflow for writing this solution
IPv4Only = True
if IPv4Only:     
    import socket
    origGetAddrInfo = socket.getaddrinfo
    
    def getAddrInfoWrapper(host, port, family=0, socktype=0, proto=0, flags=0):
        return origGetAddrInfo(host, port, socket.AF_INET, socktype, proto, flags)
    
    # replace the original socket.getaddrinfo by our version
    socket.getaddrinfo = getAddrInfoWrapper

#- /end socket hack --------------------

import urllib2 as url
from urllib import urlencode

try:
    import json
except ImportError:
    import simplejson as json


class CouchDBLogHandler(logging.Handler):
    uuids = []
    
    def __init__(self, dbname):
        logging.Handler.__init__(self, level=logging.INFO)
        self.dbname = dbname
        self.dbBasePath = "http://localhost:5984/"
        self.dbOptions = "batch=ok"
        self.dbLink = "%s/%s" % (self.dbBasePath, self.dbname)
        #self.dbLink = "http://localhost:5984/%s/" % self.dbname
        self.bulk = False
        
        # TODO: send a test query ... if we get no reply we shouldn't even bother trying to log
        result = url.urlopen(self.dbLink)
        json = result.readline()
        result.close()
        
        # TODO : do smarter validation -- right now we only care if we actually got a response back
        if not (json[0] == "{" and json[-1] == "}"):
            # self-destruct!
            pass
    
    def getUuid(self):
        try:
            return self.uuids.pop()
        except IndexError:
            count = 100
            response = url.urlopen( self.dbBasePath + "_uuids?count=%d" % count )
            output = "\n".join(response.readlines())
            self.uuids.extend( json.loads(output)["uuids"] )
            return self.uuids.pop()
    
    def emitRaw(self, message, levelname="ERROR"):
        class fakeRecord(object):
            def __init__(self):
                self.msg = message
                self.levelname = levelname
                self.asctime = time.asctime()
        
        return self.emit(fakeRecord())
    
    def emit(self, record):
        try:
            logtime = record.asctime
        except AttributeError:
            logtime = time.asctime()
    
        logmessage = record.msg
    
        # map the record data to 
        data = {
           #"_id" : self.getUuid(),
           #"_id": "1ebb4fe4f0cc33922626e126220001f0",
           #"_rev": "1-02f8671970e2dbc6ba8516fcfac1cb0d",
           "doc_type": "LogMessage",
           "level": record.levelname,
           "senderName": "Python",
           #"serverVersion": "0.1",
           #"sessionID": 1,
           "date": logtime,
           ##"message": record.message,
           "message" : logmessage,
           #"senderVersion": None,
           "categories": [
               "test",
               "logging",
               "couchdb",
           ],
        }
        
        # JSONize it
        lazyjson = json.dumps(data)
        
        response = ""
        
        if self.bulk:
            return False
        else:
            # keeping this around in case PUT is needed to create NAMED documents
            # see : http://wiki.apache.org/couchdb/HTTP_Document_API#POST
            # idea: create a UUID by md5ing the JSON we generated
            opener = url.build_opener(url.HTTPHandler)
            request = url.Request(self.dbLink + "?" + self.dbOptions, data=lazyjson)
            request.add_header('Content-Type', 'application/json')
            request.get_method = lambda: 'POST'
            #request.get_method = lambda: 'PUT'
            try:
                result = opener.open(request)
            except url.HTTPError, e:
                # fun fact: urllib2 considers all HTTP codes other than 200 and 206 as errors
                if e.code in (201, 202):
                    result = e.msg
                elif e.code == 400:
                    # bad request
                    print "tried to send bad logmessage to CouchDB:"
                    print lazyjson
                    return False
                else:
                    print lazyjson
                    print e.msg
                    raise
                
            #result = url.urlopen(self.dbLink, urlencode(data))
            
            if isinstance(result, str):
                response = result
            else:
                response = '\n'.join(result.readlines())
        
        # careful: we can't always trust the response
        if response[0] == "{" and response[-1] == "}":
            responseData = eval(response)
            if responseData["ok"]:
                return True
            else:
                print response
                return False


if __name__ == "__main__":
    handletest = CouchDBLogHandler("tutorial")
    
    msg = """nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan nyan"""
    numRecords = 1000
    
    print "running test "
    
    start = time.time()
    elapsed = 0.0
    for i in range(0,numRecords):
        emitstart = time.time()
        handletest.emitRaw(msg)
        emitend = time.time()
        elapsed += (emitend - emitstart)
        timelog = "." #% (str(emitend - emitstart))
        print timelog,
        if i > 0 and i % 80 == 0:
            print ""
    end = time.time()
    
    total = end - start
    print "done!"
    print "stresstest took %s seconds, or %d ms per record" % (str(elapsed), (elapsed / numRecords) * 1000)
    

            