#!/usr/bin/python
from kazoo.client import KazooClient
from kazoo.exceptions import ConnectionLoss
import kazoo
import socket
import time
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
host_name=socket.gethostname()


def ensureBasicStructure(zk=zk):
    """Input - ZookeeperClient
    Output - NaN
    Function - Creates Basic Znodes common to all 
    """
    zk.ensure_path("/openstack_ha")
    zk.ensure_path("/openstack_ha/hosts")
    zk.ensure_path("/openstack_ha/hosts/all")
    zk.ensure_path("/openstack_ha/hosts/alive")
    zk.ensure_path("/openstack_ha/hosts/down")
    zk.ensure_path("/openstack_ha/hosts/election")
    zk.ensure_path("/openstack_ha/hosts/start_migration")
    zk.ensure_path("/openstack_ha/hosts/leader")
    zk.ensure_path("/openstack_ha/instances")
    zk.ensure_path("/openstack_ha/instances/down_instances")
    zk.ensure_path("/openstack_ha/instances/pending")
    zk.ensure_path("/openstack_ha/instances/migrated")
    zk.ensure_path("/openstack_ha/instances/failure")
    zk.ensure_path("/openstack_ha/hosts/time_out")


def createNodeinAll(zk=zk,host_name=host_name,scheduler_log=None):
    """Input - ZookeeperClient 
    Output - NaN
    Function - Creates Znodes specitfic to host and deletes 
    node from down Znode 
    """
    try:
        zk.create("/openstack_ha/hosts/all/" + host_name)
    except kazoo.exceptions.NodeExistsError:
        #print("Node all ready created in all")
        pass
    try:
        zk.create("/openstack_ha/hosts/alive/" + host_name, b"a value", None, True)
    except kazoo.exceptions.NodeExistsError:
        #print("Node all ready created in alive") 
        pass

    try:
        zk.delete("/openstack_ha/hosts/down/" + host_name, recursive=True)
    except kazoo.exceptions.NoNodeException:
        scheduler_log.debug("Node not present in down path")
    try:
        zk.delete("/openstack_ha/hosts/time_out/" + host_name, recursive=True)
    except kazoo.exceptions.NoNodeException:
        scheduler_log.debug("Node not present in timeout path")
    try:
        zk.delete("/openstack_ha/instances/down_host/" + host_name, recursive=True)
    except kazoo.exceptions.NoNodeException:
        scheduler_log.debug("Node not present in down_host path")
    try:
        zk.delete("/openstack_ha/instances/pending/" + host_name, recursive=True)
    except kazoo.exceptions.NoNodeException:
        scheduler_log.debug("Node not present in pending path")
    try:
        zk.delete("/openstack_ha/instances/down_instances/" + host_name, recursive=True)
    except kazoo.exceptions.NoNodeException:
        scheduler_log.debug("Node not present in down_instances path")
    try:
        zk.delete("/openstack_ha/hosts/start_migration/" + host_name, recursive=True)
    except kazoo.exceptions.NoNodeException:
        scheduler_log.debug("Node not present in start_migration path")

def imalive(zk=zk,host_name=host_name):
    """Input - ZookeeperClient , hostname
    Output - NaN
    Function - Hostname is added to /openstack_ha/hosts/alive/ Znode 
    """
    try:
        zk.create("/openstack_ha/hosts/alive/" + host_name, b"a value", None, True)
    except kazoo.exceptions.NodeExistsError:
        #print("Node all ready created in alive")    
        pass

def election_node(zk=zk,host_name=host_name):
    """Input - ZookeeperClient , hostname
    Output - NaN
    Function - Hostname is added to /openstack_ha/hosts/election/ Znode
    """
    try:
        zk.create("/openstack_ha/hosts/election/" + host_name, b"a value", None, True, True)
    except kazoo.exceptions.NodeExistsError:
        scheduler_log.debug("Node all ready created in election")


def leaderCheck(zk=zk):
    """Input - ZookeeperClient
    Output - Leader Host Name
    Function - Gets all hosts from /openstack_ha/hosts/election/ Node
    """
    leader_candi=zk.get_children("/openstack_ha/hosts/election")
    #print(leader_candi)
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
