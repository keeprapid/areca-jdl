
import random
import thread
from twisted.internet import reactor,defer
from PyOPC.servers.basic import BasicXDAServer

simpleitem = set()

class SimpleXDAServer(BasicXDAServer, simpleitem):
    OPCItems = tuple(simpleitem)
    Ignore_ReturnItemPath=True
    Ignore_ReturnItemName=True


if __name__ == '__main__':
    # Start the basic server
    from twisted.web import resource, server
    xdasrv = SimpleXDAServer(http_log_fn = 'http.log')
    root = resource.Resource()
    root.putChild('',xdasrv)
    site = server.Site(root)
    reactor.listenTCP(8000, site)
    reactor.run()


def changeitem(simpleitem):
    while 1:
        if len(simpleitem) == 0:
            a = ItemContainer()
            a.Name = 'simplevalue'
            a.Value = random.randint(0,99)
            s = set()
            s.add(a)
            simpleitem = tuple(s)
        else:
            s = set(simpleitem)
            for obj in s:
                if obj.Name == 'simplevalue':
                    obj.Value = random.randint(0,99)
            simpleitem = tuple(s)
        time.sleep(10)
        print simpleitem