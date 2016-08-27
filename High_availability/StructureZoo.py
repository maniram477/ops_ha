#!/usr/bin/python
from kazoo.client import KazooClient
from kazoo.exceptions import ConnectionLoss
import kazoo

def ensureBasicStructure(zk=zk):
    zk.ensure_path("/openstack_ha")
    zk.ensure_path("/openstack_ha/hosts")
    zk.ensure_path("/openstack_ha/hosts/all")
    zk.ensure_path("/openstack_ha/hosts/alive")
    zk.ensure_path("/openstack_ha/hosts/down")
    zk.ensure_path("/openstack_ha/hosts/election")
    zk.ensure_path("/openstack_ha/hosts/migration")
    zk.ensure_path("/openstack_ha/hosts/leader")
    zk.ensure_path("/openstack_ha/instances")
    zk.ensure_path("/openstack_ha/instances/down_host")
    zk.ensure_path("/openstack_ha/instances/pending")
    zk.ensure_path("/openstack_ha/instances/migrated")
    zk.ensure_path("/openstack_ha/instances/failure")


def createNodeinAll(zk=zk,host_name=host_name):
    try:
        zk.create("/openstack_ha/hosts/all/" + host_name,bhost_name)
    except kazoo.exceptions.NodeExistsError:
        print("Node all ready created in all")
    try:
        zk.create("/openstack_ha/hosts/alive/" + host_name, bhost_name, None, True)
    except kazoo.exceptions.NodeExistsError:
        print("Node all ready created in alive")

    try:
        zk.delete("/openstack_ha/hosts/down/" + host_name, recursive=True)
        zk.delete("/openstack_ha/instances/down_host/" + host_name, recursive=True)
    except kazoo.exceptions.NoNodeException:
        print("Node not present in Down path")

def election_node(zk=zk,host_name=host_name):
    try:
        zk.create("/openstack_ha/hosts/election/" + host_name, bhost_name, None, True, True)
    except kazoo.exceptions.NodeExistsError:
        print("Node all ready created in election")


def leaderCheck(zk=zk):
    leader_candi=zk.get_children("/openstack_ha/hosts/election")
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



