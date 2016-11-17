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
        # Seperate Each unit of function and give retry for each one separately
        old_instance_id=instance_id
        tmp_host=""
        cinder,neutron,nova= client_init()        
        instance_object,info,ip_list,bdm,extra = info_collection(nova,instance_id,cinder) 
        old_instance_json=json_dump_creation(nova,instance_id,cinder,neutron,None,'migration_old_instance.json')       
        tmp_host = info['OS-EXT-SRV-ATTR:host']      
       
        
        """
        remove_fixed_ip(nova,instance_object.id,fixed_ip['v4-fixed-ip'])
        for fip in ip_list:
            remove_floating_ip(nova,instance_object.id,fip[0])
        if len(extra) > 0:
        for volume in extra:
            try:
                detach_volume(extra[volume],cinder=cinder)
                detached_volume_status(extra[volume],cinder=cinder)
                ha_agent.debug("Extra Volume Detached %s"%extra[volume])
            except Exception as e:
                ha_agent.debug('Exception removing extra volume %s'%extra[volume])
        """
        
        # Update BDM Delete on Terminate Status
        # Two Mysql queries 1. Get current status 2. Set DoT to False
        dot_status = Volume_delete_on_terminate(instance_id)
        dot_status = dot_status[0]
        ha_agent.debug("Delete on Terminate Status Updated")
        

        delete_instance(nova,instance_object)
        try:
            delete_instance_status(nova,instance_object)
            ha_agent.debug("Old Instance deleted")
        except Exception as e:
            ha_agent.debug("Unable to delete Instance - Proceeding New Instance Creation")

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


        new_instance = recreate_instance(nova,instance_object=instance_object,bdm=bdm,neutron=neutron)
        create_instance_status(nova,new_instance)
        ha_agent.debug("Recreate Finished")
        new_info = new_instance._info
        new_instance_id = new_instance.id
        # Update BDM Delete on Terminate Status to previous state
        dot_status_update(new_instance_id,dot_status)
        # Check whether floating_ip and additional Volumes are available
        attach_flt_ip(ip_list,new_instance)
        ha_agent.debug("Floating IP attached Successfully")
        attach_volumes(nova,new_info['id'],extra)
        ha_agent.debug("Volume attached Successfully")
        #zk = KazooClient(hosts='127.0.0.1:2181')
        #zk.start()        
        new_instance_json=json_dump_creation(nova,new_instance_id,cinder,neutron,old_instance_id,'migration_new_instance.json')
        
    except Exception as e :
        ha_agent.exception("Overall Task Exception ")
        if any(issubclass(e.__class__, lv) for lv in kazoo_exceptions):
            print("Kazoo Exception.....: ",e)
            time.sleep(2)
            zk = KazooClient(hosts=kazoo_host_ipaddress)
            zk.start()
            ha_agent.debug("Successfull Migration.Adding to Migrated zNode")
            zk.ensure_path("/openstack_ha/instances/migrated/"+tmp_host)
            zk.create("/openstack_ha/instances/migrated/"+tmp_host+"/"+new_instance.id,new_instance_json)
            #zk.create("/openstack_ha/instances/migrated/"+tmp_host+"/"+instance_id+"/"+new_instance_id)
        else:
            zk = KazooClient(hosts=kazoo_host_ipaddress)
            zk.start()
            ha_agent.error("Exception hence adding to failure zNode")
            zk.ensure_path("/openstack_ha/instances/failure/"+tmp_host)
            zk.create("/openstack_ha/instances/failure/"+tmp_host+"/"+instance_id,old_instance_json)
            #zk.create("/openstack_ha/instances/failure/"+instance_id)
            new_instance_json=json_dump_creation(nova,instance_id,cinder,neutron,\
                                                 None,'migration_Error_instance.json')
    else:
        zk = KazooClient(hosts=kazoo_host_ipaddress)
        zk.start()
        ha_agent.debug("Successfull Migration.Adding to Migrated zNode")
        zk.ensure_path("/openstack_ha/instances/migrated/"+tmp_host)
        zk.create("/openstack_ha/instances/migrated/"+tmp_host+"/"+new_instance.id,new_instance_json)
        #zk.create("/openstack_ha/instances/migrated/"+instance_id)
    finally:
        zk = KazooClient(hosts=kazoo_host_ipaddress)
        zk.start()
        ha_agent.debug("Removing Instance from pending")
        #zk.delete("/openstack_ha/instances/pending/"+tmp_host+"/"+instance_id)
        #zk.delete("/openstack_ha/instances/pending/"+instance_id)

