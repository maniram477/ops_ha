#!/usr/bin/python

# HA Migration Agent

from celery import Celery
from common_functions import *
import time
import json

celery = Celery('migrate')
celery.config_from_object('config')


@celery.task(name='migrate.migrate')
def migrate(instance_id,time_suffix):
    try:
        error_step=0
        # Seperate Each unit of function and give retry for each one separately
        json_old_instance="migration_old_instance_"+time_suffix+".json"
        json_new_instance="migration_new_instance_"+time_suffix+".json"
        json_error_instance="migration_error_instance_"+time_suffix+".json"
        
        error_step=1
        old_instance_id=instance_id
        tmp_host=""
        
        error_step=2
        cinder,neutron,nova= client_init()
        
        error_step=3  
        instance_object,info,ip_list,bdm,extra = info_collection(nova,instance_id,cinder) 
        old_json,old_json_encoded=json_dump_creation(nova,instance_id,cinder,neutron,None) 
        json_dump_write(filename=json_old_instance,data=old_json)
        
        tmp_host = info['OS-EXT-SRV-ATTR:host'] 
#4
        error_step=4
        # Update BDM Delete on Terminate Status
        # Two Mysql queries 1. Get current status 2. Set DoT to False
        dot_status = Volume_delete_on_terminate(instance_id)
        dot_status = dot_status[0]
        ha_agent.debug("Delete on Terminate Status Updated")
#5        
 
        error_step=5 
        delete_instance(nova,instance_object)
        try:
            delete_instance_status(nova,instance_object)
            ha_agent.debug("Old Instance deleted")
        except Exception as e:
            ha_agent.debug("Unable to delete Instance - Proceeding New Instance Creation")
#6
        error_step=6
        # Check Whether BDM is available
        # Test 1 : Update Volumes set BDM as available and Create New Instance from it.
        
        # Can add one more layer of retry for entire instance recreation process
        try:
            if(bool(bdm)):         
                detached_volume_status(bdm['vda'], cinder=cinder)
            else:
                bdm=None
        except Exception as e:
            ha_agent.debug('BDM Volume Detach Exception')
            detach_volume_db(str(bdm['vda']))
#7         
        error_step=7
        new_instance = recreate_instance(nova,instance_object=instance_object,bdm=bdm,neutron=neutron)
        create_instance_status(nova,new_instance)
        ha_agent.debug("Recreate Finished")
 
        new_info = new_instance._info
        new_instance_id = new_instance.id
        new_host_name= new_info['OS-EXT-SRV-ATTR:host'] 
#8        
        error_step=8
        # Update BDM Delete on Terminate Status to previous state
        dot_status_update(new_instance_id,dot_status)
#9      
        error_step=9
        # Check whether floating_ip and additional Volumes are available
        attach_flt_ip(ip_list,new_instance)
        ha_agent.debug("Floating IP attached Successfully")
#10        
        error_step=10
        attach_volumes(nova,new_info['id'],extra)
        ha_agent.debug("Volume attached Successfully")
#11        
        error_step=11    
        new_instance_json,Encoded_new_instance_json=json_dump_edit(data=old_json,new_instance_id=new_instance_id,\
                                                                   new_host_name=new_host_name)
        json_dump_write(filename=json_new_instance,data=Encoded_new_instance_json)
        #zk = KazooClient(hosts='127.0.0.1:2181')
        #zk.start()
    except Exception as e :
        ha_agent.exception("Overall Task Exception ")
        if any(issubclass(e.__class__, lv) for lv in kazoo_exceptions):
            print("Kazoo Exception.....: ",e)
            time.sleep(2)
            zk = KazooClient(hosts=kazoo_host_ipaddress)
            zk.start()
            ha_agent.debug("Successfull Migration.Adding to Migrated zNode")
            zk.ensure_path("/openstack_ha/instances/migrated/"+tmp_host)
            zk.create("/openstack_ha/instances/migrated/"+tmp_host+"/"+new_instance_id,Encoded_new_instance_json)
            #zk.create("/openstack_ha/instances/migrated/"+tmp_host+"/"+instance_id+"/"+new_instance_id)
        else:
            zk = KazooClient(hosts=kazoo_host_ipaddress)
            zk.start()
            ha_agent.error("Exception hence adding to failure zNode")
            zk.ensure_path("/openstack_ha/instances/failure/"+tmp_host)
            #if loop if error step less than 8 old_instance details grater than add new_instance id,
            #  host to old instance details
            if(error_step<8):
                #Unmodified Old_instance_json
                zk.create("/openstack_ha/instances/failure/"+tmp_host+"/"+instance_id,old_json_encoded)
                json_dump_write(filename=json_error_instance,data=old_json)
                
            else:
                #Edited instance_json
                data,encoded_json=json_dump_edit(data=old_json,new_instance_id=new_instance_id,new_host_name=new_host_name)
                json_dump_write(filename=json_error_instance,data=encoded_json)
                #Create znode for partial Failure 
                zk.create("/openstack_ha/instances/failure/"+tmp_host+"/"+new_instance_id,encoded_json)
            #zk.create("/openstack_ha/instances/failure/"+instance_id)
           
    else:
        zk = KazooClient(hosts=kazoo_host_ipaddress)
        zk.start()
        ha_agent.debug("Successfull Migration.Adding to Migrated zNode")
        zk.ensure_path("/openstack_ha/instances/migrated/"+tmp_host)
        zk.create("/openstack_ha/instances/migrated/"+tmp_host+"/"+new_instance_id,Encoded_new_instance_json)
        #zk.create("/openstack_ha/instances/migrated/"+instance_id)
    finally:
        zk = KazooClient(hosts=kazoo_host_ipaddress)
        zk.start()
        ha_agent.debug("Removing Instance from pending")
        zk.delete("/openstack_ha/instances/pending/"+tmp_host+"/"+instance_id)
        #zk.delete("/openstack_ha/instances/pending/"+instance_id)

