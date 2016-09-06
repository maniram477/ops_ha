#!/usr/bin/python
#scheduler
from StructureZoo import  *
from common_functions import *
from ha_agent import migrate
import time
import logging.config
logging.config.fileConfig("ha_agent.conf")
scheduler_log=logging.getLogger('scheduler')


def check_hosts(zk,host_name,task,scheduler_log):
    """Checks Host status of all hosts using nova clients nova.services.list(binary="nova-compute")
    If a host is down disables the host and puts instances on the migration queue
    """
    #Code other than nova_client should be moved to separate try block so that nova api related-
    # exceptions can be handled properly
    scheduler_log.debug("scheduler before start...!!!")
    #log.info("Before Start")
    try:
        leader = leaderCheck(zk=zk)
        #create = createNodeinAll(zk=zk,host_name=host_name)
        imalive(zk=zk)
        if (leader == host_name):
            scheduler_log.debug("Leader Name.....!%s"%host_name)
            host_dict = list_hosts()
            allhosts = host_dict['all_list']
            api_down_nodes = host_dict['down_list']
            dishosts = host_dict['disabled_list']

            zk_all = zk.get_children("/openstack_ha/hosts/all")
            zk_alive = zk.get_children("/openstack_ha/hosts/alive")
            #handeled Down node from zookeeper
            zk_down = zk.get_children("/openstack_ha/hosts/down")

            # Finds nodes that are down and not handled from zookeeper
            calculated_down_nodes =  list(set(zk_all) - set(zk_alive))

            # Scheduler Only failure
            scheduler_down = list(set(calculated_down_nodes).difference(set(api_down_nodes)))
            for node in scheduler_down:
                scheduler_log.debug("HA Scheduler Failed on Node : %s "%node)
            
            api_down = list(set(api_down_nodes).difference(set(calculated_down_nodes)))
            for node in api_down:
                scheduler_log.debug("API Failed on Node : %s "%node)
                if node not in zk_all:
                    scheduler_log.debug("HA Scheduler not even initialized %s"%node)
            api_scheduler_down = list(set(api_down_nodes).intersection(set(calculated_down_nodes)))

            # Api only failure | Complete Host Failure ( Not yet Handled | Handling | Handled  )
            if(len(api_scheduler_down))==0:
                    scheduler_log.debug("Hosts working Normally....!!!")
            else:
                scheduler_log.warning("More likely Disaster")
                #skip if maintance
                # Here check the host in api_down_nodes(api) are present in calculated_down_nodes
                #if present start the instance migrations
                # Checking whether Cluster is Still under HA Policy
                #  high availabity contiditions
                if len(api_scheduler_down) <= len(allhosts) - 1:
                    scheduler_log.warn("Seems like Manageble Disaster")
                    for host in api_scheduler_down:
                        scheduler_log.warning("Both Api and HA scheduler on ",host,' are down')
                        #checks whether down host from api is un handled(not present in down node calculate from zookeeper )
                        #(host in zk_all and host not in zk_alive) == calculated_down_nodes
                        if host in zk_down:
                            #Node will present in zk_down only when all of it's instances are migrated
                            scheduler_log.debug("Host %s Already handled...!!!!!"%host)
                        else:
                            #Node down on api,zk and ( not handled | handling )
                            if host not in dishosts:
                                #Node Not disabled | disabled reason is not skippable
                                scheduler_log.debug(host," is not disabled or reason is not maintenance")
                                if(zk.exists("/openstack_ha/hosts/time_out/"+host)==None):
                                    scheduler_log.debug("Inside Time out Node Creation")
                                    #adding host down time
                                    host_down_time = time.time()
                                    host_down_time = str.encode(str(host_down_time))
                                    scheduler_log.debug(host_down_time)
                                    zk.create("/openstack_ha/hosts/time_out/"+host, host_down_time)
                                # add ping test
                                ping_status=ping_check(host)
                                if(ping_status):
                                    scheduler_log.debug("Not a Disaster")
                                    scheduler_log.debug("ping test success....!!! Node is alive... Please Check the APIs,HA Scheduler and other Openstack Services")

                                else:
                                    scheduler_log.warning("Ping test also Failed on ",host," proceed with migration")
                                    if (zk.exists("/openstack_ha/hosts/start_migration/"+ host)): # it checks the permission from the dashborad
                                        scheduler_log.warning(" api down host :"+host+"present in zookeeper down_node:"+calculated_down_nodes)
                                        scheduler_log.debug("Strart migration....!!!!!")
                                        scheduler_log.debug("migratie instance from the "+host)
                                        instance_migration(api_down_nodes,task)
                                    else:
                                        #check for time out
                                        scheduler_log.debug("Checking Timeout for Down Node",host)
                                        curent_time = time.time()
                                        if (zk.exists("/openstack_ha/hosts/time_out/"+host)):
                                            down_host_failuretime = zk.get("/openstack_ha/hosts/time_out/"+host)[0]
                                            down_host_failuretime = down_host_failuretime.decode(encoding='UTF-8')
                                            scheduler_log.warning("down_host_failuretime",down_host_failuretime)
                                            down_host_failuretime = float(down_host_failuretime)
                                            time_interval = curent_time - down_host_failuretime
                                            if time_interval>migrate_time:
                                                instance_migration(api_down_nodes,task)
                                            else:
                                                scheduler_log.debug("Will Wait for another %d"%(migrate_time-time_interval))
                                        else:
                                            scheduler_log.debug("%s Node Does'nt have TimeOut Value. Hence will not migrate forever"%host)
                            else:
                                scheduler_log.debug("Host %s Under Maintenance"%host)
                        
                else:
                    scheduler_log.warning("Un-Manageble Disaster Too many Nodes are down")

    except Exception as e:
        if issubclass(e.__class__,kexception.NoNodeError):
            scheduler_log.exception("No node error",e)
        elif any(issubclass(e.__class__, lv) for lv in kazoo_exceptions):
            scheduler_log.exception("Kazoo Exception.....: ",e)
            time.sleep(2)
            zk = KazooClient(hosts='127.0.0.1:2181')
            zk.start()
            Node_creation = createNodeinAll(zk=zk, host_name=host_name)
            election_Node = election_node(zk=zk, host_name=host_name)
        else:
            scheduler_log.warning("Unhandled Error ",e)


#"""Import ensureBasicStructure()--It ensure the DataStructure in Zookeeper and createNode()--It ensure the node is alive. Functions are from new_basicstrut """
#zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
BasicStruct=ensureBasicStructure(zk=zk)
Node_creation=createNodeinAll(zk=zk,host_name=host_name,scheduler_log=scheduler_log)
election_Node=election_node(zk=zk,host_name=host_name,scheduler_log=scheduler_log)


while True:
    check_hosts(zk,host_name,migrate,scheduler_log)
    time.sleep(scheduler_interval)

