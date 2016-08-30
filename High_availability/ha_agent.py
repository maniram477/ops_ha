#!/usr/bin/python

# HA Migration Agent

from celery import Celery
from common_functions import *
import time


celery = Celery('migrate')
celery.config_from_object('config')

@celery.task(name='migrate.test')
def test(instance_id):
    print(instance_id)


@celery.task(name='migrate.migrate')
def migrate(instance_id):
    try:
        # Seperate Each unit of function and give retry for each one separately
        cinder,neutron = client_init()
        instance_object,info,ip_list,bdm,extra = info_collection(instance_id)
        
        # Check Whether BDM is available

        if len(extra) > 0:
            for volume in extra:
                detach_volume(extra[volume],cinder=cinder)
                detached_volume_status(extra[volume])
        
        
        # Update BDM Delete on Terminate Status
        # Two Mysql queries 1. Get current status 2. Set DoT to False
        
        delete_instance(instance_object)
        delete_instance_status(instance_object)
        
        # Check Whether BDM is available
        # Test 1 : Update Volumes set BDM as available and Create New Instance from it.
        
        # Can add one more layer of retry for entire instance recreation process
        new_instance = recreate_instance(instance_object=instance_object,bdm=bdm,neutron=neutron)
        new_info = new_instance._info
        
        # Update BDM Delete on Terminate Status to previous state
        
        # Check whether floating_ip and additional Volumes are available
        attach_flt_ip(ip_list,new_instance)
        attach_volumes(new_info['id'],extra)
        
    except Exception as e :
        print("Overall Task Exception: ",e)