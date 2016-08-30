#!/usr/bin/python
#scheduler
from StructureZoo import  *
from common_functions import *
from ha_agent import test,migrate
import time
#import nova Exceptions


def check_hosts(zk,host_name,task):
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
        imalive(zk=zk)
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
                    for node in zk_down_Node:
                        print("Zookeeper node Failure..! ",node)
            else:
                print("Cluster in Disaster")
                if(len(zk_down_Node))!=0:
                    for node in zk_down_Node:
                        print("Zookeeper node Failure..! ",node)
                # Here check the host in dhosts(api) are present in zk_down_Node
                #if present start the instance migrations
                # Checking whether Cluster is Still under HA Policy
                #  high availabity contiditions
                if len(dhosts) <= len(allhosts) - 1:
                    print("Manageble Disaster")
                    for host in dhosts:
                        #checks whether down host from api is un handled(not present in zoo keeper down node)
                        if host in zk_down_Node:
                            print("Both host and zookeeper on ",host,' are down')
                            #skip if maintance
                            if host not in dishosts:
                                print(host," is not disabled")

                                if(zk.exists("/openstack_ha/hosts/time_out/"+host)==None):
                                    print("Inside Time out Node Creation")
                                    #adding host down time
                                    host_down_time = time.time()
                                    host_down_time = str.encode(str(host_down_time))
                                    print(host_down_time)
                                    zk.create("/openstack_ha/hosts/time_out/"+host, host_down_time)
                                # add ping test
                                ping_status=ping_check(host)
                                if(ping_status==False):
                                    print("Ping test also Failed on ",host," proceed with migration")
                                    if (zk.exists("/openstack_ha/hosts/start_migration/"+ host)): # it check the permission from the dashborad
                                        print(" api down host :"+host+"present in zookeeper down_node:"+zk_down_Node)
                                        print("Strart migration....!!!!!")
                                        print("migratie instance from the "+host)
                                        instance_migration(dhosts,task)
                                    else:
                                        #check for time out
                                        print("Checking Timeout for Down Node",host)
                                        curent_time = time.time()
                                        if (zk.exists("/openstack_ha/hosts/time_out/"+host)):
                                            down_host_failuretime = zk.get("/openstack_ha/hosts/time_out/"+host)[0]
                                            down_host_failuretime = down_host_failuretime.decode(encoding='UTF-8')
                                            print("down_host_failuretime",down_host_failuretime)
                                            down_host_failuretime = float(down_host_failuretime)
                                            print(type(down_host_failuretime))
                                            time_interval = curent_time - down_host_failuretime
                                            if time_interval>migrate_time:
                                                instance_migration(dhosts,task)
                                            else:
                                                print("Will Wait for another %d"%(migrate_time-time_interval))
                                        else:
                                            print("%s Node Does'nt have TimeOut Value. Hence will not migrate forever"%host)
                                else:
                                    print("ping test success....!!! Node is alive... Please Check the APIs and other Openstack Services")
                            else:
                                print("Host %s Under Maintenance"%host)
                        elif host in zk_down:
                            print("Host %s Already handled...!!!!!"%host)
                        else:
                            print("api failure....!!!!!",host)
                else:
                    print("Un-Manageble Disaster Too many Nodes are down")

    except Exception as e:
        if issubclass(e.__class__,kexception.NoNodeError):
            print("No node error",e)
        elif any(issubclass(e.__class__, lv) for lv in kazoo_exceptions):
            print("Kazoo Exception.....: ",e)
            time.sleep(2)
            zk = KazooClient(hosts='127.0.0.1:2181')
            zk.start()
            Node_creation = createNodeinAll(zk=zk, host_name=host_name)
            election_Node = election_node(zk=zk, host_name=host_name)
        else:
            print("Unhandled Error ",e)


#"""Import ensureBasicStructure()--It ensure the DataStructure in Zookeeper and createNode()--It ensure the node is alive. Functions are from new_basicstrut """
#zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
BasicStruct=ensureBasicStructure(zk=zk)
Node_creation=createNodeinAll(zk=zk,host_name=host_name)
election_Node=election_node(zk=zk,host_name=host_name)


while True:
    check_hosts(zk,host_name,test)
    time.sleep(scheduler_interval)
