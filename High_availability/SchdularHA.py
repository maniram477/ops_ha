#!/usr/bin/python
#scheduler
from celery import Celery
from common_functions import *
from ha_agent import migrate,fence
from celery.utils.log import get_task_logger
from zookeeperStruct import  *
#log = get_task_logger(__name__)
#import nova Exceptions
celery = Celery('hascheduler')
celery.config_from_object('config')

from kazoo.client import KazooClient
import kazoo
import socket
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
host_name=socket.gethostname()
#"""Import ensureBasicStructure()--It ensure the DataStructure in Zookeeper and createNode()--It ensure the node is alive. Functions are from new_basicstrut """
BasicStruct=ensureBasicStructure(zk=zk)
Node_creation=createNodeinAll(zk=zk,host_name=host_name)
election_Node=election_node(zk=zk,host_name=host_name)


def instance_migration(dhosts):
    for dhost in dhosts:

            if(zk.exists("/openstack_ha/instances/down_host" + dhost)==False):
                zk.create("/openstack_ha/instances/down_host" + dhost, b"a value", None, True)
                for instance_obj in list_instances(dhost):
                    # Addon-Feature
                    # Can Add another check to only select instances which have HA option enabled
                    # print(instance_obj.id)
                    zk.create("/openstack_ha/instances/down_host/" + dhost+"/"+instance_obj.id, b"a value", None, True)
                    #create instance detatils under the down hosts in zookeepr
                    #migrate.apply_async((instance_obj.id,), queue='mars', countdown=wait_time)
        message_queue(dhost)

def message_queue(dhost=dhost):
    instance_list=zk.get_children("/openstack_ha/instances/down_host/" + dhost)
    if(len(instance_list)!=0):
        #while(len(instance_list)!=0)
        pending_instances_list=zk.get_children("/openstack_ha/instances/pending/"+dhost)
        instance_list = zk.get_children("/openstack_ha/instances/down_host/" + dhost)
        if(pending_instances_list<10):
            add_pending_instance_list=10-len(pending_instances_list)
            for i in range(add_pending_instance_list):
                try:
                    zk.create("/openstack_ha/instances/pending/" + dhost+"/"+instance_list[i])
                    zk.delete("/openstack_ha/instances/down_host/" + dhost + "/" + instance_list[i],recursive=True)
                except:
                    pass
            afteradd_pending_instances_list = zk.get_children("/openstack_ha/instances/pending/" + dhost)
            for j in afteradd_pending_instances_list:
                migrate.apply_async((afteradd_pending_instances_list[j],), queue='mars', countdown=wait_time)
    else:
        if (zk.exists("/openstack_ha/hosts/down/" + dhost) == False):
            zk.create("/openstack_ha/hosts/down/" + dhost, b"a value", None, True)


@celery.task(name='hascheduler.check_hosts')
def check_hosts():
    """Checks Host status of all hosts using nova clients nova.services.list(binary="nova-compute")
    If a host is down disables the host and puts instances on the migration queue
    """
    #Code other than nova_client should be moved to separate try block so that nova api related-
    # exceptions can be handled properly
    print("Before Start")
    #log.info("Before Start")
    leader = leaderCheck(zk=zk)
    create = createNodeinAll(zk=zk,host_name=host_name)
    if (leader == host_name):
        print("Leader.....!"+host_name)
        try:
            # Another try block for handling Api exceptions
            host_dict = list_hosts()
            allhosts = host_dict['all_list']
            dishosts = host_dict['disabled_list']
            dhosts = host_dict['down_list']

            zk_all = zk.get_children("/openstack_ha/hosts/all")
            zk_alive = zk.get_children("/openstack_ha/hosts/alive")
            #handeled Down node from zookeeper
            zk_down = zk.get_children("/openstack_ha/hosts/down")

            # Finds nodes that are down and not handled from zookeeper
            zk_down_Node = []
            if(len(zk_all)!=(len(zk_alive)+len(zk_down))):
                zk_down_Node =  list(set(zk_all) - set(zk_alive))

            if(len(dhosts))==0:
                if(len(zk_down_Node))==0:
                    print("Hosts working Normally....!!!")
                else:
                    print("Zookeeper node Failure..!")
            else:
                print("Cluster in Disaster")
                # Here check the host in dhosts(api) are present in zk_down_Node
                #if present start the instance migrations
                # Checking whether Cluster is Still under HA Policy
                #  high availabity contiditions
                if dhosts <= allhosts - 1:
                    print("Manageble Disaster")
                    for host in dhosts:
                        #checks whether down host from api is un handled(not present in zoo keeper down node)
                        if host in zk_down_Node:
                            # add ping test
                            #skip if maintance
                            if (zk.exists("/openstack_ha/hosts/migration/"+ host)): # it check the permission from the dashborad
                                print(" api down host :"+host+"present in zookeeper down_node:"+zk_down_Node)
                                print("Strart migration....!!!!!")
                                print("migratie instance from the "+host)
                                instance_migration(dhosts)
                            else:
                                #check for time out
                                pass

                        elif host in zk_down:
                            print("Already handled...!!!!!")
                        else:
                            print("api failure....!!!!!")
                else:
                    print("Un-Manageble Disaster")

        except:

            print("Connection loss.....!")
            time.sleep(2)
            zk = KazooClient(hosts='127.0.0.1:2181')
            zk.start()
            Node_creation = createNodeinAll(zk=zk, host_name=host_name)
            election_Node = election_node(zk=zk, host_name=host_name)

