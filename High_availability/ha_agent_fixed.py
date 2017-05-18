#!/usr/bin/python

# HA Migration Agent

from celery import Celery
from common_functions import *
import time
import json

celery = Celery('migrate')
celery.config_from_object('config')

def create_dump_dirs(time_suffix,remigration=None):
    """Input - time suffix , remigration
    Output - NaN
    Function - Creates base directories for json_dump files
    """
    
    if not dump_directory.endswith("/"):
        dump_directory = dump_directory+"/"

    directory = dump_directory + time_suffix

    if not os.path.exists(directory):
        os.makedirs(directory)

    if remigration:
        subdir = directory+"/remigration"
        if not os.path.exists(subdir):
            os.makedirs(subdir)
    else:
        subdir = directory+"/migration"
        if not os.path.exists(subdir):
            os.makedirs(subdir)

def generate_file_name(time_suffix,remigration=None):
    """Input - time suffix , remigration
    Output - Names for json files (old_ins_name,new_ins_name,error_ins_name)
    Function - Creates file names for different json files
    """

    if remigration:
        old_ins_name=time_suffix+"/remigration/remigration_old_instance.json"
        new_ins_name=time_suffix+"/remigration/remigration_new_instance.json"
        error_ins_name=time_suffix+"/remigration/remigration_error_instance.json"
    else:
        old_ins_name=time_suffix+"/migration/migration_old_instance.json"
        new_ins_name=time_suffix+"/migration/migration_new_instance.json"
        error_ins_name=time_suffix+"/migration/migration_error_instance.json"
    return old_ins_name,new_ins_name,error_ins_name

@celery.task(name='migrate.migrate')
def migrate(instance_id,time_suffix,remigration=None,host_name=None,ha_agent=ha_agent):
    try:
        error_step=1
        # Initializations
        new_host_name=""
        tmp_host=""
        old_instance_id=instance_id
        

        error_step=2
        # Json Dump Directories and  Json File Names creation
        create_dump_dirs(time_suffix,remigration)
        json_old_instance,json_new_instance,json_error_instance = generate_file_name(time_suffix,remigration=remigration)
        

        error_step=3
        # Client initializations
        cinder,neutron,nova= client_init()
        
        error_step=4
        # Info Collection from Old Instance 
        instance_object,info,ip_list,bdm,extra = info_collection(nova,instance_id,cinder)
        tmp_host = info['OS-EXT-SRV-ATTR:host']
        
        error_step=5
        #Json dump Write - Instance details are dumped to json_old_instance file
        old_json,old_json_encoded=json_dump_creation(nova,instance_id,cinder,neutron,None)
        json_dump_write(filename=json_old_instance,data=old_json)
        
        error_step=6
        # Update BDM Delete on Terminate Status
        # Two Mysql queries 1. Get current status 2. Set DoT to False
        dot_status = Volume_delete_on_terminate(instance_id)
        dot_status = dot_status[0]
        ha_agent.debug("Delete on Terminate Status Updated")   
 
        error_step=7 
        # Old Instance Deletion and polling till it gets deleted
        # After this operation instance is either deleted or left orphaned
        delete_instance(nova,instance_object)
        try:
            delete_instance_status(nova,instance_object)
            ha_agent.debug("Old Instance < %s > deleted"%old_instance_id)
        except Exception as e:
            # Instance Left orphaned - Hene should remove other resources(BDM,Secondary Volumes,Floating IP, Fixed IP)
            # BDM Deletion is hadled in the next step
            ha_agent.debug("< %s >: Unable to delete Instance"%old_instance_id)
            
            ha_agent.debug("< %s >: Removing Secondary Volumes"%old_instance_id)
            for mount,vol_id in extra:
                detach_volume(vol_id,cinder=cinder,inst_id=old_instance_id)
                detached_volume_status(vol_id, cinder=cinder,inst_id=old_instance_id)

            ha_agent.debug("< %s >: Removing Floating IPs"%old_instance_id)
            ha_agent.debug("< %s >: Removing Fixed IPs"%old_instance_id)
            ha_agent.debug("< %s >: Proceding New Instance Creation"%old_instance_id)
            
        error_step=8
        # Check Whether BDM is available if not reset state to available on DB
        try:
            if(bool(bdm)):         
                detached_volume_status(bdm['vda'], cinder=cinder,inst_id=old_instance_id)
            else:
                bdm=None
        except Exception as e:
            ha_agent.debug('BDM Volume Detach Exception')
            detach_volume_db(str(bdm['vda']))
       
        error_step=9
        # New Instance Creation and polling till it gets created
        new_instance = recreate_instance(nova,instance_object=instance_object,bdm=bdm,target_host=host_name,neutron=neutron)
        create_instance_status(nova,new_instance)
        ha_agent.debug("Recreate Finished")
    
        error_step=10
        # Info collection from New Instance 
        new_instance_id = new_instance.id
        new_instance_object,new_info,new_ip_list,new_bdm,new_extra = info_collection(nova,new_instance_id,cinder) 
        new_host_name= new_info['OS-EXT-SRV-ATTR:host']
        
        error_step=11
        # Update BDM Delete on Terminate Status to previous state
        dot_status_update(new_instance_id,dot_status)
    
        error_step=12
        # Check whether floating_ip and additional Volumes are available
        attach_flt_ip(ip_list,new_instance)
        ha_agent.debug("Floating IP attached Successfully")
      
        error_step=13
        attach_volumes(nova,new_info['id'],extra)
        ha_agent.debug("Volume attached Successfully")
     
        error_step=14
        
        new_instance_json,Encoded_new_instance_json=json_dump_edit(data=old_json,new_instance_id=new_instance_id,\
                                                                   new_host_name=new_host_name)
        json_dump_write(filename=json_new_instance,data=new_instance_json)
        #zk = KazooClient(hosts='127.0.0.1:2181')
        #zk.start()
    except Exception as e :
        ha_agent.exception("Overall Task Exception ")
        if remigration and host_name:            
            if any(issubclass(e.__class__, lv) for lv in kazoo_exceptions):

                print("Kazoo Exception.....: ",e)
                time.sleep(2)
                zk = KazooClient(hosts=kazoo_host_ipaddress)
                zk.start()
                ha_agent.debug("Successfull ReMigration.Adding to reMigrated zNode")
                zk.ensure_path("/openstack_ha/instances/remigrated/"+tmp_host)
                zk.create("/openstack_ha/instances/remigrated/"+tmp_host+"/"+new_instance_id,Encoded_new_instance_json)
                #zk.create("/openstack_ha/instances/migrated/"+tmp_host+"/"+instance_id+"/"+new_instance_id)
            else:
                zk = KazooClient(hosts=kazoo_host_ipaddress)
                zk.start()
                ha_agent.error("Exception hence adding to Remigration failure zNode")
                zk.ensure_path("/openstack_ha/instances/remigrated/failure/"+tmp_host)
                #if loop if error step less than 8 old_instance details grater than add new_instance id,
                #  host to old instance details
                if(error_step<8):
                    #Unmodified Old_instance_json
                    
                    zk.create("/openstack_ha/instances/remigrated/failure/"+tmp_host+"/"+instance_id,old_json_encoded)
                    json_dump_write(filename=json_error_instance,data=old_json)

                else:
                    #Edited instance_json                    
                    data,encoded_json=json_dump_edit(data=old_json,new_instance_id=new_instance_id,new_host_name=new_host_name)
                    json_dump_write(filename=json_error_instance,data=data)
                    #Create znode for partial Failure 
                    zk.create("/openstack_ha/instances/remigrated/failure/"+tmp_host+"/"+new_instance_id,encoded_json)
                    #zk.create("/openstack_ha/instances/failure/"+instance_id)
        else:
            print("MIGRATION PROCESS")
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
                    print("inside less than 8")
                    zk.create("/openstack_ha/instances/failure/"+tmp_host+"/"+instance_id,old_json_encoded)
                    json_dump_write(filename=json_error_instance,data=old_json)

                else:
                    #Edited instance_json
                    print("inside grate than 8")
                    data,encoded_json=json_dump_edit(data=old_json,new_instance_id=new_instance_id,new_host_name=new_host_name)
                    json_dump_write(filename=json_error_instance,data=data)
                    #Create znode for partial Failure 
                    zk.create("/openstack_ha/instances/failure/"+tmp_host+"/"+new_instance_id,encoded_json)
                #zk.create("/openstack_ha/instances/failure/"+instance_id)
    else:
        zk = KazooClient(hosts=kazoo_host_ipaddress)
        zk.start()
        if remigration and host_name:
            ha_agent.debug("Successfull ReMigration.Adding to ReMigrated zNode")
            zk.ensure_path("/openstack_ha/instances/remigrated/"+tmp_host)
            zk.create("/openstack_ha/instances/remigrated/"+tmp_host+"/"+new_instance_id,Encoded_new_instance_json)
        else:
            ha_agent.debug("Successfull Migration.Adding to Migrated zNode")
            zk.ensure_path("/openstack_ha/instances/migrated/"+tmp_host)
            zk.create("/openstack_ha/instances/migrated/"+tmp_host+"/"+new_instance_id,Encoded_new_instance_json)
            #zk.create("/openstack_ha/instances/migrated/"+instance_id)
    finally:
        zk = KazooClient(hosts=kazoo_host_ipaddress)
        zk.start()
        if remigration and host_name:
            ha_agent.debug("Removing Instance from migrated after successfully remigrated instances")
            #zk.delete("/openstack_ha/instances/migrated/"+tmp_host+"/"+instance_id)            
        else:
            ha_agent.debug("Removing Instance from pending")
            #zk.delete("/openstack_ha/instances/pending/"+tmp_host+"/"+instance_id)
            #zk.delete("/openstack_ha/instances/pending/"+instance_id)
           
        

