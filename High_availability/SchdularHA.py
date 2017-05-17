#!/usr/bin/python
#Scheduler
from StructureZoo import  *
from common_functions import *
from ha_agent import migrate
import time

import logging.config
logging.config.fileConfig("ha_agent.conf")
scheduler_log=logging.getLogger('scheduler')


def check_hosts(zk,host_name,task,scheduler_log):
    """Input - ZookeeperClient , host_name , celert Task
    Output - NaN
    Function - Checks Host status of all hosts using nova clients nova.services.list(binary="nova-compute")
    If a host is down disables the host and puts instances on the migration queue
    """

    scheduler_log.debug("scheduler before start...!!!")
    try:
        #Leader Election
        leader = leaderCheck(zk=zk)

        #Update alive status to zookeeper - seems unnecessary
        imalive(zk=zk) 

        #If current Host is the Leader perform Scheduled Checks  
        if (leader == host_name):
            scheduler_log.debug("Leader Name.....!%s"%host_name)
            
            #Fetch List of Hosts - From API
            host_dict = list_hosts(nova)
            allhosts = host_dict['all_list']
            api_down_nodes = host_dict['down_list']
            dishosts = host_dict['disabled_list']

            zk_all = zk.get_children("/openstack_ha/hosts/all")
            zk_alive = zk.get_children("/openstack_ha/hosts/alive")
            
            #Fetch Down nodes that are already Handeled - From Zookeeper
            zk_down = zk.get_children("/openstack_ha/hosts/down")

            #Fetch nodes that are down and not already handled - From Zookeeper
            calculated_down_nodes =  list(set(zk_all) - set(zk_alive))

            #Find Nodes Where Scheduler Only failed
            scheduler_down = list(set(calculated_down_nodes).difference(set(api_down_nodes)))
            for node in scheduler_down:
                scheduler_log.debug("HA Scheduler Failed on Node : %s "%node)
            
            #Find Nodes Where API Only failed 
            api_down = list(set(api_down_nodes).difference(set(calculated_down_nodes)))
            for node in api_down:
                scheduler_log.debug("API Failed on Node : %s "%node)
                if node not in zk_all:
                    scheduler_log.debug("HA Scheduler not even initialized %s"%node)

            #Find nodes where both API and Zookeeper are failed       
            api_scheduler_down = list(set(api_down_nodes).intersection(set(calculated_down_nodes)))

            # Possible Host states -  Api only failure | Complete Host Failure ( Not yet Handled | Handling | Handled  )
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
                        scheduler_log.warning("Both Api and HA scheduler on" +host+" are down")
                        #checks whether down host from api is un handled(not present in down node calculate from zookeeper )
                        #(host in zk_all and host not in zk_alive) == calculated_down_nodes
                        if host in zk_down:
                            #Node will present in zk_down only when all of it's instances are migrated
                            scheduler_log.debug("Host %s Already handled...!!!!!"%host)
                        else:
                            #Node down on api,zk and ( not handled | handling )
                            if host not in dishosts:
                                #Node Not disabled | disabled reason is not skippable
                                scheduler_log.debug(host+" is not disabled or reason is not maintenance")
                                if(zk.exists("/openstack_ha/hosts/time_out/"+host)==None):
                                    scheduler_log.debug("Inside Time out Node Creation")
                                    
                                    #adding host down time
                                    host_down_time = time.time()
                                    host_down_time = str.encode(str(host_down_time))
                                    scheduler_log.debug(host_down_time)
                                    zk.create("/openstack_ha/hosts/time_out/"+host, host_down_time)
                                    
                                    #adding time_suffix for json_dump file name
                                    temp_time=time.localtime(time.time())                                       
                                    time_suffix=str(temp_time.tm_mday)+"_"+str(temp_time.tm_mon)+"_"+\
                                    str(temp_time.tm_year)+"_"+str(temp_time.tm_hour)+"_"+\
                                    str(temp_time.tm_min)
                                    enc_time_suffix=str.encode(time_suffix)
                                    scheduler_log.debug(time_suffix)
                                    zk.create("/openstack_ha/hosts/time_out/"+host+"/time_suffix",enc_time_suffix)

                                    # call notification_mail(subj,msg) | Adding Down Node details to Notification 
                                    try:
                                        subject = "DGP Office VDI Node Down: %s"%host
                                        message = "Please Check the Network Connectivity and Powersupply as soon as possible"
                                        notification_mail(subject,message,to_email=['naanalteam@naanal.in'])

                                        message = "Please Contact System Administrator"
                                        notification_mail(subject,message)
                                        scheduler_log.debug("mail in Scheduler...!")
                                    except Exception as e:
                                        scheduler_log.debug(e)
                                        scheduler_log.debug("Error....! mail scheduler..!")

                                # add ping test
                                ping_status=ping_check(host)
                                if(ping_status):
                                    scheduler_log.debug("Not a Disaster")
                                    scheduler_log.debug("ping test success....!!! Node is alive... Please Check the APIs,HA Scheduler and other Openstack Services")

                                else:
                                    scheduler_log.warning("Ping test also Failed on "+host+" proceed with migration")
                                    if (zk.exists("/openstack_ha/hosts/start_migration/"+ host)): # it checks the permission from the dashborad
                                        scheduler_log.warning(" api down host :"+host+"present in zookeeper down_node:")
                                        scheduler_log.debug("Strart migration....!!!!!")
                                        scheduler_log.debug("migratie instance from the "+host)
                                        tmp_time_suffix=zk.get("/openstack_ha/hosts/time_out/"+host+"/time_suffix")[0]
                                        zk_time_suffix = tmp_time_suffix.decode()                                        
                                        instance_migration(nova,api_down_nodes,task,zk_time_suffix)
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
                                                tmp_time_suffix=zk.get("/openstack_ha/hosts/time_out/"+host+"/time_suffix")[0]
                                                zk_time_suffix = tmp_time_suffix.decode()
                                                instance_migration(nova,api_down_nodes,task,zk_time_suffix)
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
            scheduler_log.exception("No node error")
        elif any(issubclass(e.__class__, lv) for lv in kazoo_exceptions):
            scheduler_log.exception("Kazoo Exception.....: ")
            time.sleep(2)
            try:
                zk = KazooClient(hosts='127.0.0.1:2181')
                zk.start()            
                Node_creation = createNodeinAll(zk=zk, host_name=host_name)
                election_Node = election_node(zk=zk, host_name=host_name)
            except:
                pass
        else:
            scheduler_log.warning("Unhandled Error ")
            scheduler_log.exception("")


#"""Import ensureBasicStructure()--It ensure the DataStructure in Zookeeper and createNode()--It ensure the node is alive. Functions are from new_basicstrut """
#zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
BasicStruct=ensureBasicStructure(zk=zk)
Node_creation=createNodeinAll(zk=zk,host_name=host_name,scheduler_log=scheduler_log)
election_Node=election_node(zk=zk,host_name=host_name,scheduler_log=scheduler_log)


while True:
    check_hosts(zk,host_name,migrate,scheduler_log)
    time.sleep(scheduler_interval)
    

