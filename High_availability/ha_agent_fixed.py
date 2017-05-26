#!/usr/bin/python

# HA Migration Agent

from celery import Celery
from common_functions import *
import time
import json

celery = Celery('migrate')
celery.config_from_object('config')

def create_dump_dirs(time_suffix,remigration=None,dump_directory=None):
    """Input - time suffix , remigration
    Output - NaN
    Function - Creates base directories for json_dump files
    """
    
    if not dump_directory.endswith("/"):
        dump_directory = dump_directory+"/"

    directory = dump_directory + time_suffix
    
    if not os.path.exists(dump_directory):
        os.makedirs(dump_directory)

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


def error_log(error_step,instance_id,old_instance_id=None,e=None,ha_agent=None,instance_name=None):
    if(error_step==1):
        ha_agent.error("< %s > [ %s ] Unknown Exception : "%(instance_id,instance_name),e)
    elif(error_step==2):
        ha_agent.error("< %s > [ %s ] Dump Directory Creation failed with Exception :"%(instance_id,instance_name),e)
    elif(error_step==3):
        ha_agent.error("< %s > [ %s ] Client Initialization failed with Exception :"%(instance_id,instance_name),e)
    elif(error_step==4):
        ha_agent.error("< %s > [ %s ] Info collection failed with Exception :"%(instance_id,instance_name),e)
    elif(error_step==5):
        ha_agent.error("< %s > [ %s ] Json Dump write of old Instance failed with Exception :"%(instance_id,instance_name),e)
    elif(error_step==6):
        ha_agent.error("< %s > [ %s ] DoT status update failed with Exception :"%(instance_id,instance_name),e)
    elif(error_step==7):
        ha_agent.error("< %s > [ %s ] Instance Deletion failed with Exception :"%(instance_id,instance_name),e)
    elif(error_step==8):
        ha_agent.error("< %s > [ %s ]  BDM reset failed with Exception :"%(instance_id,instance_name),e)
    elif(error_step==9):
        ha_agent.error("< %s > [ %s ]  New Instance Creation failed with Exception :"%(old_instance_id,instance_name),e)
    elif(error_step==10):
        ha_agent.error("< %s > [ %s ]  Info collection on New Instance: < %s > failed with Exception :"%(old_instance_id,instance_name,instance_id),e)
    elif(error_step==11):
        ha_agent.error("< %s > [ %s ]  DoT status update on New Instance: < %s > failed with Exception :"%(old_instance_id,instance_name,instance_id),e)
    elif(error_step==12):
        ha_agent.error("< %s > [ %s ] Floationg IP attach on New Instance: < %s >  failed with Exception :"%(old_instance_id,instance_name,instance_id),e)
    elif(error_step==13):
        ha_agent.error("< %s > [ %s ]  Volume attach on New Instance: < %s > failed with Exception :"%(old_instance_id,instance_name,instance_id),e)
    elif(error_step==13):
        ha_agent.error("< %s > [ %s ] Json Dump creation of New Instance: < %s > failed with Exception :"%(old_instance_id,instance_name,instance_id),e)
    else:
        ha_agent.error("Unknown Exception",e)

@celery.task(name='migrate.migrate')
def migrate(instance_id,time_suffix,remigration=None,host_name=None,ha_agent=ha_agent):
    try:
        error_step=1
        # Initializations
        new_host_name=""
        tmp_host=""
        old_instance_id=instance_id
        instance_name = ""

        error_step=2
        # Json Dump Directories and  Json File Names creation
        create_dump_dirs(time_suffix,remigration,dump_directory=dump_directory)
        json_old_instance,json_new_instance,json_error_instance = generate_file_name(time_suffix,remigration=remigration)
        #ha_agent.debug("File names generated successfully for Instance < %s > [%s]"%(instance_id,instance_name))


        error_step=3
        # Client initializations
        cinder,neutron,nova= client_init()
        #ha_agent.debug("Clients Successfully Initialized for Instance < %s > [%s]"%(instance_id,instance_name))
                

        error_step=4
        # Info Collection from Old Instance 
        instance_object,info,ip_list,bdm,extra,instance_name = info_collection(nova,instance_id,cinder)
        ha_agent.debug("Successfully collected Info about Instance < %s > [%s]"%(old_instance_id,instance_name))

        nics = get_fixed_ip(info,neutron,instance_id=instance_id,instance_name=instance_name) # Fixed IPs
        ha_agent.debug("Successfully collected Fixed IP(s) of Instance < %s > [%s]"%(old_instance_id,instance_name))

        tmp_host = info['OS-EXT-SRV-ATTR:host']
        

        error_step=5
        #Json dump Write - Instance details are dumped to json_old_instance file
        old_json,old_json_encoded=json_dump_creation(nova,instance_id,cinder,neutron,None,step_count=error_step)
        json_dump_write(filename=json_old_instance,data=old_json,dump_directory=dump_directory,instance_id=old_instance_id,instance_name=instance_name)
        ha_agent.debug("Json Dump created successfully for Old Instance < %s > [%s]"%(old_instance_id,instance_name))

        
        error_step=6
        # Update BDM Delete on Terminate Status
        # Two Mysql queries 1. Get current status 2. Set DoT to False
        dot_status = Volume_delete_on_terminate(instance_id,instance_name=instance_name)
        dot_status = dot_status[0]
        ha_agent.debug("Delete on Terminate Status Updated for Instance < %s > [%s]"%(old_instance_id,instance_name))   
        
        old_json,old_json_encoded = update_step_count(data=old_json,step_count=error_step,instance_id=instance_id,instance_name=instance_name)


        error_step=7 
        # Old Instance Deletion and polling till it gets deleted
        # After this operation instance is either deleted or left orphaned
        delete_instance(nova,instance_object,instance_id=instance_id,instance_name=instance_name)
        try:
            delete_instance_status(nova,instance_object)
            ha_agent.debug("Old Instance < %s > [%s] deleted"%(old_instance_id,instance_name))
        except Exception as e:
            # Instance Left orphaned - Hene should remove other resources(BDM,Secondary Volumes,Floating IP, Fixed IP)
            # BDM Deletion is hadled in the next step
            ha_agent.debug("Unable to delete Instance < %s > [%s] "%(old_instance_id,instance_name))
            
            for mount,vol_id in extra:
                detach_volume(vol_id,cinder=cinder,inst_id=old_instance_id,instance_name=instance_name)
                detached_volume_status(vol_id, cinder=cinder,inst_id=old_instance_id,instance_name=instance_name)

            for flt_ip , fix_ip in ip_list:
                remove_floating_ip(nova,old_instance_id,flt_ip,instance_name=instance_name)

            for nic in nics:
                if nic.has_key('v4-fixed-ip'):
                    remove_fixed_ip(nova,old_instance_id,nic.get('v4-fixed-ip'),instance_name=instance_name)

            ha_agent.debug("< %s > [%s] Proceding New Instance Creation"%(old_instance_id,instance_name))
        

        old_json,old_json_encoded = update_step_count(data=old_json,step_count=error_step,instance_id=instance_id,instance_name=instance_name)


        error_step=8
        # Check Whether BDM is available if not reset state to available on DB
        try:
            if(bool(bdm)):         
                detached_volume_status(bdm['vda'], cinder=cinder,inst_id=old_instance_id,instance_name=instance_name)
            else:
                bdm=None
        except Exception as e:
            detach_volume_db(str(bdm['vda']),instance_id=old_instance_id,instance_name=instance_name)

        old_json,old_json_encoded = update_step_count(data=old_json,step_count=error_step,instance_id=instance_id,instance_name=instance_name)

        ha_agent.debug("Updated BDM status on Volume of Instance < %s > [%s]"%(old_instance_id,instance_name))

        
        # ------------------------------------------------ New Instance --------------------------------------------------- #
        error_step=9
        # New Instance Creation and polling till it gets created
        new_instance = recreate_instance(nova,instance_object=instance_object,bdm=bdm,target_host=host_name,neutron=neutron)
        create_instance_status(nova,new_instance,instance_name=instance_name)
        new_instance_id = new_instance.id
    
        old_json,old_json_encoded = update_step_count(data=old_json,step_count=error_step,instance_id=instance_id,instance_name=instance_name)
        

        error_step=10
        # Info collection from New Instance 
        new_instance_object,new_info,new_ip_list,new_bdm,new_extra,instance_name = info_collection(nova,new_instance_id,cinder) 
        new_host_name= new_info['OS-EXT-SRV-ATTR:host']

        old_json,old_json_encoded = update_step_count(data=old_json,step_count=error_step,instance_id=instance_id,instance_name=instance_name)
 
        ha_agent.debug("Successfully collected Info about New Instance < %s > [%s]"%(new_instance_id,instance_name))

        
        error_step=11
        # Update BDM Delete on Terminate Status to previous state
        dot_status_update(new_instance_id,dot_status)
    
        old_json,old_json_encoded = update_step_count(data=old_json,step_count=error_step,instance_id=instance_id,instance_name=instance_name)

        ha_agent.debug("DoT status updated for Instance < %s > [%s]"%(new_instance_id,instance_name))



        error_step=12
        # Attach Floating IP address
        attach_flt_ip(ip_list,new_instance)

        old_json,old_json_encoded = update_step_count(data=old_json,step_count=error_step,instance_id=instance_id,instance_name=instance_name)
        ha_agent.debug("Floating IP(s) attached Successfully to Instance < %s > [%s]"%(new_instance_id,instance_name))


        error_step=13
        # Attach Secondary Volumes
        attach_volumes(nova,new_info['id'],extra)

        old_json,old_json_encoded = update_step_count(data=old_json,step_count=error_step,instance_id=instance_id,instance_name=instance_name)
        ha_agent.debug("Volume attached Successfully to Instance < %s > [%s]"%(new_instance_id,instance_name))

        error_step=14
        # Create Json dump of New instance 
        new_instance_json,Encoded_new_instance_json=json_dump_edit(data=old_json,new_instance_id=new_instance_id,\
                                                                   new_host_name=new_host_name,step_count=error_step,instance_name=instance_name)
        json_dump_write(filename=json_new_instance,data=new_instance_json,dump_directory=dump_directory,instance_id=new_instance_id,instance_name=instance_name)
        ha_agent.debug("Json Dump created successfully for New Instance < %s > [%s]"%(new_instance_id,instance_name))

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
            else:
                zk = KazooClient(hosts=kazoo_host_ipaddress)
                zk.start()
                ha_agent.error("Exception hence adding to Remigration failure zNode")
                zk.ensure_path("/openstack_ha/instances/remigrated/failure/"+tmp_host)
                #if loop if error step less than 9 old_instance details grater than add new_instance id,
                #  host to old instance details
                if(error_step<10):
                    #Unmodified Old_instance_json
                    error_log(error_step,old_instance_id,e=e,ha_agent=ha_agent,instance_name=instance_name)
                
                    if(error_step>6):
                        zk.create("/openstack_ha/instances/remigrated/failure/"+tmp_host+"/"+old_instance_id,old_json_encoded)
                        json_dump_write(filename=json_error_instance,data=old_json,dump_directory=dump_directory,instance_id=old_instance_id,instance_name=instance_name)

                else:
                    error_log(error_step,new_instance_id,e=e,ha_agent=ha_agent,old_instance_id=old_instance_id,instance_name=instance_name)

                    #Edited instance_json
                    if(error_step not in [11,14]):
                        data,encoded_json=json_dump_edit(data=old_json,new_instance_id=new_instance_id,new_host_name=new_host_name,step_count=error_step,instance_name=instance_name)
                        json_dump_write(filename=json_error_instance,data=data,dump_directory=dump_directory,instance_id=new_instance_id,instance_name=instance_name)
                        #Create znode for partial Failure 
                        zk.create("/openstack_ha/instances/remigrated/failure/"+tmp_host+"/"+new_instance_id,encoded_json)
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
            else:
                zk = KazooClient(hosts=kazoo_host_ipaddress)
                zk.start()
                ha_agent.error("Exception hence adding to failure zNode")
                zk.ensure_path("/openstack_ha/instances/failure/"+tmp_host)
                #if loop if error step less than 9 old_instance details grater than add new_instance id,
                #  host to old instance details
                if(error_step<10):
                    #Unmodified Old_instance_json
                    error_log(error_step,old_instance_id,e=e,ha_agent=ha_agent,instance_name=instance_name)

                    if(error_step>6):
                        json_dump_write(filename=json_error_instance,data=old_json,dump_directory=dump_directory,instance_id=old_instance_id,instance_name=instance_name)
                        zk.create("/openstack_ha/instances/failure/"+tmp_host+"/"+old_instance_id,old_json_encoded)

                else:
                    error_log(error_step,new_instance_id,e=e,ha_agent=ha_agent,old_instance_id=old_instance_id,instance_name=instance_name)

                    #Edited instance_json
                    if(error_step not in [11,14]):
                        data,encoded_json=json_dump_edit(data=old_json,new_instance_id=new_instance_id,new_host_name=new_host_name,step_count=error_step,instance_name=instance_name)
                        json_dump_write(filename=json_error_instance,data=data,dump_directory=dump_directory,instance_id=new_instance_id,instance_name=instance_name)
                        #Create znode for partial Failure 
                        zk.create("/openstack_ha/instances/failure/"+tmp_host+"/"+new_instance_id,encoded_json)
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
    finally:
        zk = KazooClient(hosts=kazoo_host_ipaddress)
        zk.start()
        if remigration and host_name:
            if(error_step>11):
                ha_agent.debug("Removing Instance from migrated after successfully remigrated instances")
                zk.delete("/openstack_ha/instances/pending/"+tmp_host+"/"+instance_id)
        else:
            if(error_step>11):
                ha_agent.debug("Removing Instance from pending /openstack_ha/instances/pending/"+tmp_host+"/"+instance_id)
                zk.delete("/openstack_ha/instances/pending/"+tmp_host+"/"+instance_id)
        

