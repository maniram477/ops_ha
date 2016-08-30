#!/usr/bin/python
#scheduler
from StructureZoo import  *
from common_functions import *
from ha_agent import test
import time
#import nova Exceptions

def check_hosts(zk,host_name):
    """Checks Host status of all hosts using nova clients nova.services.list(binary="nova-compute")
    If a host is down disables the host and puts instances on the migration queue
    """
    #Code other than nova_client should be moved to separate try block so that nova api related-
    # exceptions can be handled properly
    print("Before Start")
    #log.info("Before Start")
    try:
        leader = leaderCheck(zk=zk)
        #create = createNodeinAll(zk=zk,host_name=host_name)
        if (leader == host_name):
            print("Leader.....!"+host_name)
            host_dict = list_hosts()
            allhosts = host_dict['all_list']
            dhosts = host_dict['down_list']
            dishosts = host_dict['disabled_list']

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
                            #skip if maintance
                            if host not in dishosts:

                                #adding host down time
                                host_down_time = time.time()
                                host_down_time = str.encode(str(host_down_time))
                                if(zk.exists("/openstack_ha/hosts/time_out/"+host)==False):
                                    zk.create("/openstack_ha/hosts/time_out/"+host, host_down_time)
                                # add ping test
                                ping_status=ping_check(host)
                                if(ping_status==False):
                                    
                                    if (zk.exists("/openstack_ha/hosts/start_migration/"+ host)): # it check the permission from the dashborad
                                        print(" api down host :"+host+"present in zookeeper down_node:"+zk_down_Node)
                                        print("Strart migration....!!!!!")
                                        print("migratie instance from the "+host)
                                        instance_migration(dhosts,test)
                                    else:
                                        #check for time out
                                        curent_time = time.time()
                                        if (zk.exists("/openstack_ha/hosts/time_out/"+host)):
                                            down_host_failuretime = zk.get("/openstack_ha/hosts/time_out/"+host)[0]
                                            down_host_failuretime = down_host_failuretime.decode(encoding='UTF-8')
                                            print("down_host_failuretime",down_host_failuretime)
                                            down_host_failuretime = float(down_host_failuretime)
                                            print(type(down_host_failuretime))
                                            time_interval = curent_time - down_host_failuretime
                                            migrate_time=1800 # 30  mints
                                            if time_interval>migrate:
                                                instance_migration(dhosts,test)
                                        else:
                                            print("%s Node Does'nt have TimeOut Value. Hence will not migrate forever"%host)
                                else:
                                    print("ping test success....!!! Node is alive... Please Check the APIs and other Openstack Services")
                            else:
                                print("Host %s Under Maintenance"%host)
                        elif host in zk_down:
                            print("Already handled...!!!!!")
                        else:
                            print("api failure....!!!!!")
                else:
                    print("Un-Manageble Disaster")

    except kexception.NoNodeError:
        print("No Node Error")
    except tuple(kazoo_exceptions):
        print("Kazoo Exception.....: ")
        time.sleep(2)
        zk = KazooClient(hosts='127.0.0.1:2181')
        zk.start()
        Node_creation = createNodeinAll(zk=zk, host_name=host_name)
        election_Node = election_node(zk=zk, host_name=host_name)
    except:
        pass



#"""Import ensureBasicStructure()--It ensure the DataStructure in Zookeeper and createNode()--It ensure the node is alive. Functions are from new_basicstrut """
#zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
BasicStruct=ensureBasicStructure(zk=zk)
Node_creation=createNodeinAll(zk=zk,host_name=host_name)
election_Node=election_node(zk=zk,host_name=host_name)


while True:
    check_hosts(zk,host_name)
    time.sleep(scheduler_interval)
