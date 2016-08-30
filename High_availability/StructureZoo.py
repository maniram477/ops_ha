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


def createNodeinAll(zk=zk,host_name=host_name):
    try:
        zk.create("/openstack_ha/hosts/all/" + host_name)
    except kazoo.exceptions.NodeExistsError:
        print("Node all ready created in all")
    try:
        zk.create("/openstack_ha/hosts/alive/" + host_name, b"a value", None, True)
    except kazoo.exceptions.NodeExistsError:
        print("Node all ready created in alive")

    try:
        zk.delete("/openstack_ha/hosts/down/" + host_name, recursive=True)
        zk.delete("/openstack_ha/hosts/time_out/" + host_name, recursive=True)
        zk.delete("/openstack_ha/instances/down_host/" + host_name, recursive=True)
    except kazoo.exceptions.NoNodeException:
        print("Node not present in Down path")

def election_node(zk=zk,host_name=host_name):
    try:
        zk.create("/openstack_ha/hosts/election/" + host_name, b"a value", None, True, True)
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


def instance_migration(dhosts):
    for dhost in dhosts:
            if(zk.exists("/openstack_ha/instances/down_instances" + dhost)==False):
                zk.create("/openstack_ha/instances/down_instances" + dhost, b"a value", None, True)
                for instance_obj in list_instances(dhost):
                    # Addon-Feature
                    # Can Add another check to only select instances which have HA option enabled
                    # print(instance_obj.id)
                    zk.create("/openstack_ha/instances/down_instances/" + dhost+"/"+instance_obj.id, b"a value", None, True)
                    #create instance detatils under the down_instances in zookeepr
        message_queue(dhost)

def message_queue(dhost=dhost):
    instance_list=zk.get_children("/openstack_ha/instances/down_instances/" + dhost)
    if(len(instance_list)!=0):
        pending_instances_list=zk.get_children("/openstack_ha/instances/pending/"+dhost)
        instance_list = zk.get_children("/openstack_ha/instances/down_instances/" + dhost)
        if(pending_instances_list<10):
            add_pending_instance_list=10-len(pending_instances_list)
            for i in range(add_pending_instance_list):
                try:
                    zk.create("/openstack_ha/instances/pending/" + dhost+"/"+instance_list[i])
                    zk.delete("/openstack_ha/instances/down_instances/" + dhost + "/" + instance_list[i],recursive=True)
                    migrate.apply_async((instance_list[i],), queue='mars', countdown=wait_time)
                except Exception as e:
                    print(e)
    else:
        if (zk.exists("/openstack_ha/hosts/down/" + dhost) == False):
            zk.create("/openstack_ha/hosts/down/" + dhost, b"a value", None, True)
