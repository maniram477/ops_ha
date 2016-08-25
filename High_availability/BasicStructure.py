#!/usr/bin/python
from kazoo.client import KazooClient
from kazoo.exceptions import ConnectionLoss
import kazoo
import time
import socket
zk = KazooClient(hosts='172.30.70.3:2181')
zk.start()
#zk.create("/testzoo",value=b"head",makepath=True)
host_name=socket.gethostname()
bhost_name=str.encode(host_name)

def ensureBasicStructure():
    zk.ensure_path("/testzoo")
    zk.ensure_path("/testzoo/hosts")
    zk.ensure_path("/testzoo/hosts/all")
    zk.ensure_path("/testzoo/hosts/alive")
    zk.ensure_path("/testzoo/hosts/down")
    zk.ensure_path("/testzoo/hosts/election")
    zk.ensure_path("/testzoo/hosts/instances")
    zk.ensure_path("/testzoo/hosts/instances/down_host")
    zk.ensure_path("/testzoo/hosts/instances/pending")
    zk.ensure_path("/testzoo/hosts/instances/migrated")
    zk.ensure_path("/testzoo/hosts/leader")

def createNodeinAll():
    try:
        zk.create("/testzoo/hosts/all/" + host_name,bhost_name)
    except kazoo.exceptions.NodeExistsError:
        print("Node all ready created in all")
    try:
        zk.create("/testzoo/hosts/alive/" + host_name, bhost_name, None, True)
    except kazoo.exceptions.NodeExistsError:
        print("Node all ready created in alive")

    try:
        zk.delete("/testzoo/hosts/down/" + host_name, recursive=True)
    except kazoo.exceptions.NoNodeException:
        print("Node not present in Down path")

def election_node():
    try:
        zk.create("/testzoo/hosts/election/" + host_name, bhost_name, None, True, True)
    except kazoo.exceptions.NodeExistsError:
        print("Node all ready created in election")


def listnode():
    all=zk.get_children("/testzoo/hosts/all")
    alive = zk.get_children("/testzoo/hosts/alive")
    down = zk.get_children("/testzoo/hosts/down")
    print("Inside the list node....!!!")
    return all,alive,down

def leaderCheck():
    leader_candi=zk.get_children("/testzoo/hosts/election")
    print(leader_candi)
    leader_candidate=[]
    leader=''
    for candi in leader_candi:
        candidate=candi[-10:]
        leader_candidate.append(int(candidate))
        smallvalue = min(leader_candidate)
        small = str(smallvalue).zfill(10)
    for i in leader_candi:
        if (small in i):
            leader=i
    leader=leader[0:-10]
    return(leader)

ensureing=ensureBasicStructure()
create=createNodeinAll()
election_node()

while 1:
    try:
        leader = leaderCheck()
        create = createNodeinAll()
        if (leader==host_name):
            print("i'm leader host...")
            all=zk.get_children("/testzoo/hosts/all")
            alive=zk.get_children("/testzoo/hosts/alive")
            down=zk.get_children("/testzoo/hosts/down")
            if(all==(alive+down)):
                print ("No operation....!")
            else:
                print("add the down node to Down path..!")
                down_Node=list(set(all) - set(alive))
                for node in down_Node:
                    print(node)
                    try:
                        zk.create("/testzoo/hosts/down/"+node,bhost_name, None, True)
                    except kazoo.exceptions.NodeExistsError:
                        print("Down Node all ready created:::::"+node)
        else:
            print("follower  "+host_name)
    except :
        print("Connection loss.....!")
        time.sleep(2)
        zk = KazooClient(hosts='127.0.0.1:2181')
        zk.start()
        create = createNodeinAll()
        election_node()

    time.sleep(3)
#zk.stop()