#!/usr/bin/python

# HA Migration Agent

from celery import Celery
from common_functions import *
import time


celery = Celery('migrate')
celery.config_from_object('config')




@celery.task(name='migrate.migrate')
def migrate(instance_id):
    try:
        # Seperate Each unit of function and give retry for each one separately
        cinder,neutron = client_init()
        instance_object,info,ip_list,bdm,extra = info_collection(instance_id,cinder)
        tmp_host = info['OS-EXT-SRV-ATTR:host']
        # Check Whether BDM is available
        print("Information Collected")

        """if len(extra) > 0:
            for volume in extra:
                detach_volume(extra[volume],cinder=cinder)
                detached_volume_status(extra[volume],cinder=cinder)
"""
        print("Extra Volume Detached")
        
        # Update BDM Delete on Terminate Status
        # Two Mysql queries 1. Get current status 2. Set DoT to False
        
        delete_instance(instance_object)
        delete_instance_status(instance_object)
        print("Old Instance deleted")

        # Check Whether BDM is available
        # Test 1 : Update Volumes set BDM as available and Create New Instance from it.
        
        # Can add one more layer of retry for entire instance recreation process
        detached_volume_status(bdm['vda'], cinder=cinder)

        new_instance = recreate_instance(instance_object=instance_object,bdm=bdm,neutron=neutron)
        create_instance_status(new_instance)
        print("Recreate Finished")
        new_info = new_instance._info
        new_instance_id = new_instance.id
        # Update BDM Delete on Terminate Status to previous state
        
        # Check whether floating_ip and additional Volumes are available
        attach_flt_ip(ip_list,new_instance)
        print("Floating IP attached Successfully")

        attach_volumes(new_info['id'],extra)
        print("Volume attached Successfully")
        #zk = KazooClient(hosts='127.0.0.1:2181')
        #zk.start()
        
    except Exception as e :
        print("Overall Task Exception: ",e)
        if any(issubclass(e.__class__, lv) for lv in kazoo_exceptions):
            print("Kazoo Exception.....: ",e)
            time.sleep(2)
            zk = KazooClient(hosts='127.0.0.1:2181')
            zk.start()
            print("Successfull Migration.Adding to Migrated zNode")
            #zk.create("/openstack_ha/instances/migrated/"+tmp_host+"/"+instance_id+"/"+new_instance_id)
        else:
            print("Exception hence adding to failure zNode")
            #zk.create("/openstack_ha/instances/failure/"+instance_id)
    else:
        print("Successfull Migration.Adding to Migrated zNode")
        #zk.create("/openstack_ha/instances/migrated/"+instance_id)
    finally:
        print("Removing Instance from pending")
        #zk.delete("/openstack_ha/instances/pending/"+instance_id)

            