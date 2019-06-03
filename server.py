import Pyro4
import csv
import time
from random import randint
import random
@Pyro4.expose
class Server(object):

    status=""
    unsharedUpdates=0
    stableTimeStamp=[0,0,0]
    executed={}
    timeToGossip=2
    serversDict={}

    def getStatus(self):
        rand=random.uniform(0, 1)
        if (rand<=0.1):
            self.status="Overloaded"
        elif (rand<=0.2):
            self.status="Offline"
        else:
            self.status="Available"
        return self.status

    def sayHello(self, name):
        return "Hello, {0}: " \
               "Sucessful remote invocation!".format(name)

    def __init__(self):
        Server.serversDict=self.refreshServers()
        Server.status="Available"

    def requestRating(self,movieID,FEtimestamp,serverIndex):
        Server.serversDict=self.refreshServers()
        while True:
            if Server.stableTimeStamp==FEtimestamp:
                summ=0.0
                counter=0
                with open(filename) as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if (int(row['movieId'])==movieID):
                            summ+=float(row['rating'])
                            counter+=1
                    print("timestamp when requesting rating is ", FEtimestamp)
                    if (counter!=0):
                        return summ/counter
                    else:
                        return "undefined"
            else:
                for key, value in list(Server.serversDict.items()):
                    if len(Server.serversDict)==1:
                        print("I am the only server. I am missing updates. I am setting the ts equal to frontend ts")
                        for i in range (len(FEtimestamp)):
                            Server.stableTimeStamp[i]=FEtimestamp[i]
                    if (key!=serverIndex):
                        #check if updates are required
                        if FEtimestamp[key]>Server.stableTimeStamp[key]:
                            print("I am updating from server ",key)
                            try:
                                self.getUpdates(value,key)
                            except Pyro4.errors.CommunicationError:
                                print("couldn't find the server to get update from")
                                name="Server" + str(key)
                                ns.remove(name)
                                Server.serversDict=self.refreshServers()
                                self.stableTimeStamp[key]=FEtimestamp[key]
                                print("couldn't get updates so I've set the internal ts equal to frontend ts")

    def writeToFile(self,fields):
        m = csv.reader(open('movies.csv',encoding="utf8"))
        mlines=list(m)
        movieName="unknown"
        for line in mlines:
            if (line[0]==str(fields[0])):
                movieName=line[1]
        r = csv.reader(open(filename))
        lines = list(r)
        updated=False
        for line in lines:
            if (line[0]==str(fields[0]) and line[1]==str(fields[1])):
                result="updated rating to movie "+movieName
                line[2]=fields[2]
                line[3]=fields[3]
                updated=True
                break
        if (updated==False):
            lines.append([str(fields[0]),str(fields[1]),str(fields[2]),str(fields[3])])
            result="appended rating to movie "+movieName
        writer = csv.writer(open(filename, 'w',newline=''))
        writer.writerows(lines)
        return result

    def refreshServers(self):
            for key in ns.list(prefix="Server"): #add the newly added servers in ns to self.serversDict
                serveri=int(key[len("Server"):])
                if serveri not in self.serversDict.keys(): 
                    self.serversDict[serveri]=Pyro4.Proxy("PYRONAME:"+key)
            for key in list(self.serversDict): #remove the deleted servers in ns from self.serversDict
                if (("Server"+str(key).strip()) not in ns.list()):
                    self.serversDict.pop(key, None)
                    print("I am popping "+"Server"+str(key).strip())
            return self.serversDict 

    def getExecutedUpdates(self):
        return Server.executed

    def getUpdates(self,hostserver,hostserverindex):
        hostExecuted=hostserver.getExecutedUpdates()
        try:
            for key,value in hostExecuted.items():
                if (key not in Server.executed):
                    ServerIndex = value[len(value)-1]
                    if ((value[1][ServerIndex])==Server.stableTimeStamp[ServerIndex]+1): #check one away
                        print("I am adding updateID ", key ," from ", hostserverindex)
                        fields=value[0]
                        self.writeToFile(fields)
                        Server.executed[key]=[fields, value[1] ,ServerIndex]
                        Server.stableTimeStamp[ServerIndex]+=1
        except Pyro4.errors.CommunicationError:
            ns.remove("Server"+str(hostserverindex))
        except Pyro4.errors.NamingError:
            ns.remove("Server"+str(hostserverindex))
            
    def getGossip(self,executed):
        for key, value in executed.items():
            if key not in Server.executed:
                gossipServerIndex = value[len(value)-1]
                if ((Server.stableTimeStamp[gossipServerIndex]+1)==value[1][gossipServerIndex]): #check for the update that is one away
                    print("I am adding updateID ",key, " from server ",gossipServerIndex, " via gossiping" )
                    fields=value[0]
                    self.writeToFile(fields)
                    Server.executed[key]=[fields, value[1] ,gossipServerIndex]
                    Server.stableTimeStamp[gossipServerIndex]+=1

    def gossip(self,serverIndex):
        for key, value in list(Server.serversDict.items()):
            if (key!=serverIndex):
                try:
                    status=value.getStatus()
                    print("Server",key," has status ", status)
                    if (status=="Available"):
                        value.getGossip(Server.executed)
                        print("Gossip message sent to Server", key)
                except Pyro4.errors.NamingError:
                    print("server", key , " can't be found to gossip with. Naming error")
                    ns.remove("Server" + str(key))
                    Server.serversDict=self.refreshServers()
                except Pyro4.errors.CommunicationError:
                    print("server", key , " can't be found to gossip with. Communication error")
                    ns.remove("Server" + str(key))
                    Server.serversDict=self.refreshServers()
                    
 
    def submitRating(self,fields,FEtimestamp,serverIndex,updateID):
        while True:
            self.serversDict=self.refreshServers()
            if (FEtimestamp==Server.stableTimeStamp):
                outcome=self.writeToFile(fields)
                Server.stableTimeStamp[serverIndex]+=1
                FEtimestamp[serverIndex]+=1
                Server.executed[updateID]=[fields, FEtimestamp,serverIndex]
                Server.unsharedUpdates+=1
                if (Server.unsharedUpdates==Server.timeToGossip):
                    self.gossip(serverIndex)
                    Server.unsharedUpdates=0
                print("server timestamp ", self.stableTimeStamp)
                return (FEtimestamp,outcome)
            else:
                if (FEtimestamp!=Server.stableTimeStamp):
                    print("I am requesting a force update from the replicas. I am missing updates")
                    if len(Server.serversDict)==1:
                        print("I am the only server. I am missing updates. I am setting the ts equal to frontend ts")
                        for i in range (len(FEtimestamp)):
                            Server.stableTimeStamp[i]=FEtimestamp[i]
                        continue
                    for key, value in list(Server.serversDict.items()):
                        if (key!=serverIndex):
                            #check if updates are required
                            if FEtimestamp[key]>Server.stableTimeStamp[key]:
                                print("I am updating from server ",key)
                                try:
                                    status=value.getStatus()
                                    print("Server",key, " has status ",status)
                                    if (status=="Available"):
                                        self.getUpdates(value,key)
                                    else:
                                        print("server unavailability has caused to set server ts to FE ts")
                                        self.stableTimeStamp[key]=FEtimestamp[key]
                                except Pyro4.errors.CommunicationError:
                                    print("couldn't find the server to get update from")
                                    name="Server" + str(key)
                                    ns.remove(name)
                                    Server.serversDict=self.refreshServers()
                                    self.stableTimeStamp[key]=FEtimestamp[key]
                                    print("couldn't get updates so I've set the internal ts equal to frontend ts")

PYRONAME="Server"
daemon = Pyro4.Daemon()
ns = Pyro4.locateNS()
uri = daemon.register(Server)

k=0
while (True):
    serverName="Server"+str(k)
    if serverName in ns.list(prefix="Server"):
        k+=1
    else:
        break

filename="ratings"+str(k)+".csv"
ns.register(serverName, uri)
print(serverName," running")

daemon.requestLoop()                   # start the event loop of the server to wait for calls