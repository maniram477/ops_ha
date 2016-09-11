#!/usr/bin/python

# HA Migration Agent

from celery import Celery
from common_functions import *
import time
import json

celery = Celery('migrate')
celery.config_from_object('config')


@celery.task(name='migrate.migrate')
def migrate(instance_id):
    try:
        tmp_host=""
        new_tmp_host=""
        volumes={}
        # Seperate Each unit of function and give retry for each one separately
        cinder,neutron,nova= client_init()
        instance_object,info,ip_list,bdm,extra = info_collection(nova,instance_id,cinder)      
        tmp_host = info['OS-EXT-SRV-ATTR:host'] 
        volumes.update(bdm)
        nics = get_fixed_ip(info,neutron)
        if not nics:
            fixed_ip=None
        else:
            fixed_ip=nics[0]
        # Check Whether BDM is available
        ha_agent.debug("Information Collected")
        if not ip_list:            
            folating_ip=None
        else:
            folating_ip=ip_list[0][0]
       
        if bool(extra):            
            volumes.update(extra)
            volume=volumes
        else:
            volume=volumes
        old_instance=json.dumps({"instance_name":instance_object.name,"host_name":tmp_host,"instance_id":instance_object.id,"folating_ip":folating_ip,"fixed_ip":fixed_ip,"volume":volume},ensure_ascii=True)
        old_instance_json=str.encode(old_instance)    
        """if len(extra) > 0:
            for volume in extra:
                detach_volume(extra[volume],cinder=cinder)
                detached_volume_status(extra[volume],cinder=cinder)
        """
        ha_agent.debug("Extra Volume Detached")
        
        # Update BDM Delete on Terminate Status
        # Two Mysql queries 1. Get current status 2. Set DoT to False
        
        delete_instance(nova,instance_object)
        delete_instance_status(nova,instance_object)
        ha_agent.debug("Old Instance deleted")

        # Check Whether BDM is available
        # Test 1 : Update Volumes set BDM as available and Create New Instance from it.
        
        # Can add one more layer of retry for entire instance recreation process
        if(bool(bdm)):           
            detached_volume_status(bdm['vda'], cinder=cinder)
        else:
            bdm=None

        new_instance = recreate_instance(nova,instance_object=instance_object,bdm=bdm,neutron=neutron)
        create_instance_status(nova,new_instance)
        ha_agent.debug("Recreate Finished")
        new_info = new_instance._info
        new_instance_id = new_instance.id
        # Update BDM Delete on Terminate Status to previous state
        
        # Check whether floating_ip and additional Volumes are available
        attach_flt_ip(ip_list,new_instance)
        ha_agent.debug("Floating IP attached Successfully")

        attach_volumes(nova,new_info['id'],extra)
        ha_agent.debug("Volume attached Successfully")
        #zk = KazooClient(hosts='127.0.0.1:2181')
        #zk.start()
        
        #create new_instance json
        instance_object1,info,ip_list,bdm,extra = info_collection(nova,new_instance_id,cinder)
        new_tmp_host = info['OS-EXT-SRV-ATTR:host']
        # Check Whether BDM is available
        ha_agent.debug("Information Collected")
        nics = get_fixed_ip(info,neutron)
        if not nics:
            fixed_ip=None
        else:
            fixed_ip=nics[0]
        # Check Whether BDM is available
        ha_agent.debug("Information Collected")
        if not ip_list:            
            folating_ip=None
        else:
            folating_ip=ip_list[0][0]
            
        if bool(extra):
            bdm.update(extra)
            volume=bdm
        else:
            volume=bdm    
    
        new_instance_details=json.dumps({"instance_name":new_instance.name,"host_name":new_tmp_host,"instance_id":new_instance.id,"folating_ip":folating_ip,"fixed_ip":fixed_ip,"volume":volume},ensure_ascii=True)
        new_instance_json=str.encode(new_instance_details)
        
    
        
    except Exception as e :
        ha_agent.exception("Overall Task Exception ")
        if any(issubclass(e.__class__, lv) for lv in kazoo_exceptions):
            print("Kazoo Exception.....: ",e)
            time.sleep(2)
            zk = KazooClient(hosts='172.30.64.14:2181,172.30.64.13:2181,172.30.64.12:2181')
            zk.start()
            ha_agent.debug("Successfull Migration.Adding to Migrated zNode")
            zk.ensure_path("/openstack_ha/instances/migrated/"+new_tmp_host)
            zk.create("/openstack_ha/instances/migrated/"+new_tmp_host+"/"+new_instance.id,new_instance_json)
            #zk.create("/openstack_ha/instances/migrated/"+tmp_host+"/"+instance_id+"/"+new_instance_id)
        else:
            zk = KazooClient(hosts='172.30.64.14:2181,172.30.64.13:2181,172.30.64.12:2181')
            zk.start()
            ha_agent.error("Exception hence adding to failure zNode")
            zk.ensure_path("/openstack_ha/instances/failure/"+tmp_host)
            zk.create("/openstack_ha/instances/failure/"+tmp_host+"/"+instance_id,old_instance_json)
            #zk.create("/openstack_ha/instances/failure/"+instance_id)
    else:
        zk = KazooClient(hosts='172.30.64.14:2181,172.30.64.13:2181,172.30.64.12:2181')
        zk.start()
        ha_agent.debug("Successfull Migration.Adding to Migrated zNode")
        zk.ensure_path("/openstack_ha/instances/migrated/"+new_tmp_host)
        zk.create("/openstack_ha/instances/migrated/"+new_tmp_host+"/"+new_instance.id,new_instance_json)
        #zk.create("/openstack_ha/instances/migrated/"+instance_id)
    finally:
        zk = KazooClient(hosts='172.30.64.14:2181,172.30.64.13:2181,172.30.64.12:2181')
        zk.start()
        ha_agent.debug("Removing Instance from pending")
        zk.delete("/openstack_ha/instances/pending/"+tmp_host+"/"+instance_id)
        #zk.delete("/openstack_ha/instances/pending/"+instance_id)

            
