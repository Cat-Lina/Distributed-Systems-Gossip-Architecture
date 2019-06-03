# FrontEnd.py
import Pyro4
from random import randint
import random,time
@Pyro4.expose
class FrontEnd(object):
    FEtimestamp=[0,0,0]
    updateID=0
    index=0
    serversDict={}
    length=0
    
    def __init__(self):
        FrontEnd.serversDict=self.refreshServers()

    def refreshServers(self):
        for key in ns.list(prefix="Server"):
            serveri=int(key[len("Server"):])
            if serveri not in self.serversDict.keys():
                name="PYRONAME:"+key
                s=Pyro4.Proxy(name)  
                self.serversDict[serveri]=s
        for key in list(self.serversDict):
            if (("Server"+str(key).strip()) not in ns.list()):
                self.serversDict.pop(key, None)
                print("I am popping "+"Server"+str(key).strip())
        return self.serversDict     
           
    def chooseAvailableServer(self):
        FrontEnd.myServers=self.refreshServers()
        while (True):
            try:
                index=random.choice(list(FrontEnd.serversDict.keys()))
                server=FrontEnd.serversDict[index]
                status=server.getStatus()
                print("Server",index," has status ", status)
                if (status=="Available"):
                    print("Index of chosen server is ", index)
                    return (server,index)

            except Pyro4.errors.NamingError:
                print("server", index, " can't be found")
                name="Server"+str(index)
                ns.remove(name)
                if len(ns.list(prefix="Server"))==0:
                    return "no servers available"
            except  Pyro4.errors.CommunicationError:
                print("server ", index, " seems to be down. Communication error")
                name="Server"+str(index)
                ns.remove(name)
                if len(ns.list(prefix="Server"))==0:
                    return "no servers available"
 

    def submitRating(self,userID,movieID,rating):
        chosen=self.chooseAvailableServer()
        if (chosen!="no servers available"):
            chosenServer=chosen[0]
            serverIndex=chosen[1]
            fields=[userID, movieID, rating, time.time()]
            FrontEnd.updateID+=1
            result=chosenServer.submitRating(fields,FrontEnd.FEtimestamp,serverIndex,FrontEnd.updateID)
            FrontEnd.FEtimestamp=result[0]
            outcome=result[1]
            print(FrontEnd.FEtimestamp)
            return outcome
        else:
            return chosen
    def sayHello(self, name):
        chosen=self.chooseAvailableServer()
        if (chosen!="no servers available"):
            chosenServer=chosen[0]
            chosenIndex=chosen[1]
            print("sayhello has chosen ",chosenIndex)
            return chosenServer.sayHello(name)
        return chosen

    def requestRating(self,movieID):
        print(FrontEnd.FEtimestamp)
        chosen=self.chooseAvailableServer()
        if (chosen!="no servers available"):
            chosenServer=chosen[0]
            serverIndex=chosen[1]
            return chosenServer.requestRating(movieID,FrontEnd.FEtimestamp,serverIndex)
        else:
            return chosen

    def gossip(self,serverIndex):
        myServers=self.refreshServers()
        myServers[serverIndex].gossip(serverIndex)


daemon = Pyro4.Daemon()        # make a Pyro daemon
ns = Pyro4.locateNS()          #locate the naming server
uri = daemon.register(FrontEnd)   # register the Hello as a Pyro object
ns.register("FrontEnd", uri)
print("FrontEnd running")

daemon.requestLoop()             # start the event loop of the server to wait for calls