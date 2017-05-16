#!/usr/bin/python
from novaclient import client as nova_client
from neutronclient.v2_0 import client as neutron_client
from cinderclient import client as cinder_client
import socket
import fcntl
import struct
from retrying import retry
import inspect
import kazoo.exceptions as kexception
import kazoo
from kazoo.client import KazooClient
import os
from nova import exception as nova_exceptions
from novaclient import exceptions as novaclient_exceptions
from cinderclient import exceptions as c_exception
import cinderclient.openstack.common.apiclient.exceptions as c_api_exception
import logging.config
import MySQLdb
import json
import string
import smtplib

#-------------Logging Vars-----------------#
logging.config.fileConfig("ha_agent.conf")
ha_agent=logging.getLogger('ha_agent')
scheduler_log=logging.getLogger('scheduler')
#------------------------------------------#

#------HA Scheduler and Worker Vars--------#

#Number of seconds to wait before adding instance to migration queue
migrate_time=120# In Seconds. 

num_instances_batch = 10

##Openstack Credentials
controller_ip="30.20.0.2" #Management IP address(VIP)
user ="admin"
passwd = "admin"
tenant = "admin"
##

## Mysql User Details
mysql_user ="ha"
mysql_pass ="ha_pass"
##

##Kazoo Client 
kazoo_host_ipaddress='30.20.0.3:2181,30.20.0.4:2181,30.20.0.5:2181'
##

#-------------------------------------------#

#---------------Json Dump Vars--------------#
dump_directory="/var/log/ops_ha/json_dump/"
#-------------------------------------------#



#----------------Notification Vars----------#
email = "naanal"
pwd = "*************"
to_email = ['naanal123@naanal.in','naanaltec@gmail.com']
#-------------------------------------------#


host_name=socket.gethostname()


#---Used Variables inside retry Functions---#
scheduler_interval = 5 #In Seconds
api_retry_count = 3 
api_retry_interval = 2000 #In MilliSeconds

poll_status_count = 10
poll_status_interval = 5000 #In MilliSeconds
#-------------------------------------------#


maintenance_state = ['maintenance','skip','pause_migration']

#-------------------Exceptions--------------#
kazoo_exceptions = [obj for name, obj in inspect.getmembers(kexception) if inspect.isclass(obj) and issubclass(obj, Exception)]
cinder_exceptions = [obj for name, obj in inspect.getmembers(c_exception) if inspect.isclass(obj) and issubclass(obj, Exception)]
cinder_api_exceptions = [obj for name, obj in inspect.getmembers(c_api_exception) if inspect.isclass(obj) and issubclass(obj, Exception)]
all_cinder_exceptions = cinder_exceptions + cinder_api_exceptions
#-------------------------------------------#

#---------------Client----------------------#
zk = KazooClient(hosts='127.0.0.1:2181')
nova = nova_client.Client(2,user,passwd,tenant,"http://%s:5000/v2.0"%controller_ip,connection_pool=True)
#-------------------------------------------#


#Retry Functions
def api_failure(exc):
    return True
    
def poll_status(exc):
    if exc.message == "poll":
        return True
    else:
        return False
    
def poll_vm_status(exc):
    if exc.message == "error":
        return False
    else:
        return True
    

#Configuration Functions
def get_ip_address(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    a = fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24]
    return socket.inet_ntoa(a)

#Health Check Functions
def ping_check(hostname):
    scheduler_log.debug("Pinging.... " + hostname)
    response = os.system("/bin/ping -c 1 " + hostname)
    scheduler_log.debug("Response from ping check %d"%(response))
    #and then check the response...
    if response == 0:
        scheduler_log.debug(hostname + 'is up!')
        return True
    else:
        scheduler_log.debug(hostname + 'is down!')
        return False


#common Funcitons

def dbwrap(func):
    """Wrap a function in an idomatic SQL transaction.  The wrapped function
    should take a cursor as its first argument; other arguments will be
    preserved.
    """
    def new_func(*args, **kwargs):
        conn = MySQLdb.connect(controller_ip,mysql_user,mysql_pass)
        cursor = conn.cursor()
        try:
            retval = func(cursor, *args, **kwargs)
        except Exception as e:
            #log.error()
            retval = None
            ha_agent.exception('MYSQL EXCEPTION')
        finally:
            cursor.close()
            conn.commit()
            conn.close()
        return retval
    return new_func


# Host Functions
@retry(retry_on_exception=api_failure,stop_max_attempt_number=api_retry_count,wait_fixed=api_retry_interval)
def list_hosts(nova):
    """Input - NovaClient object
    Output - dictionary with list of all hosts,list of down hosts, list of disabled hosts 
    """
    try:
        return {'all_list': [host.host for host in nova.services.list(binary="nova-compute")],\
                'down_list': [host.host for host in nova.services.list(binary="nova-compute") if host.state.lower() == 'down'],\
                'disabled_list': [host.host for host in nova.services.list(binary="nova-compute") if host.status.lower() == 'disabled' if host.disabled_reason in maintenance_state]\
               }
    except Exception as ee:
        ha_agent.warning("Inside the list host Function..!")
        ha_agent.exception('')
        raise Exception('step0')

def down_hosts(nova):
    """Input - NovaClient object
    Output - list of down hosts
    """
    return [host.host for host in nova.services.list(binary="nova-compute") if host.state.lower() == 'down']


def active_hosts(nova):
    """Input - NovaClient object
    Output - list of active hosts
    """
    return [host.host for host in nova.services.list(binary="nova-compute") if host.state.lower() == 'up']


def host_disable(nova):
    """Not Used"""
    pass




@retry(retry_on_exception=api_failure,stop_max_attempt_number=api_retry_count,wait_fixed=api_retry_interval)
def client_init():
    """Input - None
    Output - Cinder , Neutron , Nova Client Objects
    """
    try:
        neutron = neutron_client.Client(username=user,
                                            password=passwd,
                                            tenant_name=tenant,
                                            auth_url="http://%s:5000/v2.0"%controller_ip)
        cinder = cinder_client.Client(1,user,passwd,tenant,"http://%s:5000/v2.0"%controller_ip)
        nova = nova_client.Client(2,user,passwd,tenant,"http://%s:5000/v2.0"%controller_ip,connection_pool=True)
        return cinder,neutron,nova
    except Exception as ee:
        ha_agent.warning("During neutron,cinder initialization")
        ha_agent.exception('')
        raise Exception('step1')
    




# Instacne Functions
@retry(retry_on_exception=api_failure,stop_max_attempt_number=api_retry_count,wait_fixed=api_retry_interval)
def info_collection(nova,instance_id,cinder):
    """Input - NovaClient object , Instance ID , CinderClient object
    Output - Instance Object , Instance Info , IP List , Block Device Mapping(Boot Volume),
    Extra Volumes 
    """
    try:
        ha_agent.debug("Inside the info_collection...!")
        instance = nova.servers.get(instance_id)
        info = instance._info
        ip_list = floating_ip_check(info)
        bdm,extra = cinder_volume_check(info,cinder=cinder)
        return instance,info,ip_list,bdm,extra
    except Exception as e:
        ha_agent.warn("Collecting the Infromation about instances")
        ha_agent.exception('')
        raise Exception('step2')

        
@retry(retry_on_exception=api_failure,stop_max_attempt_number=api_retry_count,wait_fixed=api_retry_interval)
def delete_instance(nova,instance_object):
    """Input - Instance Object
    Output - True | False
    Function - Deletes Instance
    """
    try:
        nova.servers.delete(instance_object.id)
    except Exception as e:
        ha_agent.warn("During the deletion of instance...!")
        ha_agent.exception('')

@retry(retry_on_exception=api_failure,stop_max_attempt_number=poll_status_count,wait_fixed=poll_status_interval)
def delete_instance_status(nova,instance_object):
    """Input - NovaClient Object , Instance Object
    Output - NaN
    Function - Polls Instance status till it get's deleted
    """
    try:
        allow_retry_task = ['deleting',None]
        tmp_ins = nova.servers.get(instance_object.id)
        if tmp_ins._info['OS-EXT-STS:vm_state'] == 'error':
            tmp_ins.force_delete()
            
        elif tmp_ins._info['OS-EXT-STS:task_state'] in allow_retry_task:
            raise Exception("poll")
    except Exception as e:
        ha_agent.warn("Exception in deleting instance...!")
        ha_agent.exception('')
        if isinstance(e,novaclient_exceptions.NotFound):
            ha_agent.debug("Instance Not Found hence deleted")
        else:
            raise Exception(e)


def list_instances(nova,host_name=None):
    """Input - Hostname (optional)
    Output - Instance List
    Function - List Instances (on specific Host if Host given as Input | on All Host ) 
    """
    ins_list = nova.servers.list(search_opts={'host':host_name})
    return ins_list

@dbwrap
def get_instance_uuid(cursor,name):
    """Input - DB Cursor , Name of Instance
    Output - Instance UUID
    Function - fetches uuid of an instance from DB using Instance name
    """
    cursor.execute("select uuid from nova.instances  where display_name='%s' and deleted=0 order by created_at desc;"%name)
    return cursor.fetchone()    
    
def get_instance(nova,name):
    """Input - Name of the Instance 
    Output - Instance Object
    Function - Finds the newly created Instance with display_name,deleted,meta as identifiers
    """
    uuid = get_instance_uuid(con,name)
    try:
        instance = nova.servers.get(uuid)
    except Exception as e:
        ha_agent.warn("Exception occured duting get uuid of instance..!")
        ha_agent.exception('')
        #log.error(e)
        return None
    return instance
 

@retry(retry_on_exception=api_failure,stop_max_attempt_number=api_retry_count,wait_fixed=api_retry_interval)               
def create_instance(nova,name=None,image=None,bdm=None,\
                         flavor=None,nics=None,availability_zone=None,\
                         disk_config=None,meta=None,security_groups=None):
    """Input - NovaClient object , Name , Image | bdm , flavor , Nics , Optional( availability_zone,
    disk_config,meta,security_groups )
    Output - Instance Object 
    Function - Creates an Instance and returns instance Object 
    """
    try:
        
        instance_object = nova.servers.create(name=name,image=image,block_device_mapping=bdm,\
                             flavor=flavor,nics=nics,availability_zone=availability_zone,\
                             disk_config=disk_config,security_groups=security_groups)
        return instance_object
    except Exception as e:
        ha_agent.warning("Exception during instance creation...!")
        ha_agent.exception('')

@retry(retry_on_exception=poll_vm_status,stop_max_attempt_number=poll_status_count,wait_fixed=poll_status_interval)               
def create_instance_status(nova,instance_object):
    """Input - NovaClient Object , Instance Object
    Output - NaN
    Function - Polls Instance status till it get's Created
    """
    try:        
        allow_retry = ['spawning','building','starting','powering_on','scheduling','block_device_mapping','networking']
        tmp_ins = nova.servers.get(instance_object.id)
        status = ( tmp_ins._info['OS-EXT-STS:vm_state'], tmp_ins._info['OS-EXT-STS:task_state'] )
        if status[0] == 'active':
            ha_agent.debug("After creation:  Instance in active state")
        elif status[0] == 'error':
            raise Exception("error")
        elif status[1] in allow_retry:
            raise Exception("poll")
    except Exception as e:
        ha_agent.warn("Exception: checking the instance status after creation...!")
        ha_agent.exception('')
        if e.message == 'error':
            ha_agent.error("Instance - %s went to ERROR state",(instance_object.id))
        else:
            raise Exception(e)
    


@dbwrap
def Volume_delete_on_terminate(cursor,ins_id):
    """Input - DBcursor , Instance ID
    Output - Status of DoT(Delete on Terminate)
    Function - fetches Delete on Terminate Status of boot volume from DB using Instance id and updates DoT to False
    """
    cursor.execute("select delete_on_termination from nova.block_device_mapping where instance_uuid='%s';"%ins_id)
    dot_status = cursor.fetchone()
    cursor.execute("update nova.block_device_mapping set delete_on_termination=False where instance_uuid='%s';"%ins_id)
    return dot_status

@dbwrap
def dot_status_update(cursor,ins_id,status):
    """Input - DBcursor , Instance ID , DoT status
    Output - NaN
    Function - Updates DoT status 
    """
    cursor.execute("update nova.block_device_mapping set delete_on_termination=%s where instance_uuid='%s';"%(status,ins_id))
    

# Volume Related Functions     
@dbwrap
def detach_volume_db(cursor,vol_id):
    """Input - DBcursor , Volume ID 
    Output - NaN
    Function - Updates volume status = available and attach_status = detached on volumes table and
    Updates volume attach_status = detached on volume_attachment table
    """
    cursor.execute(" update cinder.volumes set status='available',attach_status='detached' where id='%s';"%vol_id)
    cursor.execute(" update cinder.volume_attachment set attach_status='detached' where volume_id='%s';"%vol_id)

        
@retry(retry_on_exception=api_failure,stop_max_attempt_number=api_retry_count,wait_fixed=api_retry_interval)
def detach_volume(volume,cinder=None):
    """Input - Volumes - Dictionary Containing volume details 
    Eg: {'/dev/vdb':'16e5593c-15c7-48a6-b46f-2bda2951e3b0','/dev/vdc':'16e5593c-15c7-48a6-b46f-2bda2951esd0'}
    Output - True | False
    Function - Detach Volumes From Instance
    """
    try:
        cinder.volumes.detach(volume)
    except Exception as e:
        ha_agent.warn("Soft Exception: During detach_volume")
        ha_agent.exception('')

@retry(retry_on_exception=poll_status,stop_max_attempt_number=10,wait_fixed=1000)      
def detached_volume_status(volume,cinder=None):
    """Input - Volume Object
    Output - NaN
    Function - Polls Volumes status till it get's deleted
    """
    try:
        allow_retry = ['detaching','in-use'] 
        tmp_vol = cinder.volumes.get(volume)
        if tmp_vol.status in allow_retry:
            tmp_vol.detach()
            raise Exception("poll")
    except Exception as e:
        ha_agent.warn("Exception Checking detached_volume_status")
        ha_agent.exception('')        
        if e.message == 'poll':
            raise Exception("poll")         
        elif any(issubclass(e.__class__, lv) for lv in all_cinder_exceptions):
            ha_agent.info("MAYDAY - Looks Like cinderclient or API is not accessible")
            ha_agent.info("PARACHUTE - Update MYSQL in-use to available ")
            detach_volume_db(str(volume))
        else:
            raise Exception('Exception Checking detached_volume_status"')


def cinder_volume_check(info,cinder=None):
    """ Input - info - instance._info - Information from instance Object, CinderClient
    Output - BDM and Extra Volumes
    """
    
    bdm = {}
    volumes ={}
    try:
        volumes = {cinder.volumes.get(x['id']).attachments[0]['device']:x['id'] for x in info.get('os-extended-volumes:volumes_attached') }
        
        if volumes.has_key('/dev/vda'):
            bdm = {'vda': volumes['/dev/vda']}
            del(volumes['/dev/vda'])
            
    except Exception as e:
        #log.warning(e)
        ha_agent.warning("Exception:cinder_volume_check")
        ha_agent.exception('')
        bdm = None
    else:
        pass
    return bdm,volumes

        
@retry(retry_on_exception=api_failure,stop_max_attempt_number=api_retry_count,wait_fixed=api_retry_interval)                  
def attach_volumes(nova,instance,volumes):
    """Input - Volumes - Dictionary Containing volume details 
    Eg: {'vdb':'16e5593c-15c7-48a6-b46f-2bda2951e3b0','vdc':'16e5593c-15c7-48a6-b46f-2bda2951esd0'}
        - Instance ID
    Output - NaN
    Function - Attach Volumes to Instances
    """
    try:
        for dev in volumes:
            nova.volumes.create_server_volume(instance,volumes[dev],dev)
    except Exception as e:
        ha_agent.warning("Exception During Attach_volumes")
        ha_agent.exception('')

    

# IP Functions
def floating_ip_check(info):
    """Input - Instance Info
    Output - List of tuple (Floating_IP,Fixed_IP)
    Function - Parses Floating IP addresses from Instance Info and converts it to required format
    """
    for net in info.get('addresses',''):
        tmp_list = []
        tmp_ip = []
        fix_ip = ''
            
        for nic in info.get('addresses')[net]:
            if nic['OS-EXT-IPS:type'] == 'fixed':
                    fix_ip = nic['addr']
            elif nic['OS-EXT-IPS:type'] == 'floating':
                    tmp_ip.append(nic['addr'])
            
        tmp_list.extend([(flt_ip,fix_ip) for flt_ip in tmp_ip])
                
        return tmp_list 
         
@retry(retry_on_exception=api_failure,stop_max_attempt_number=api_retry_count,wait_fixed=api_retry_interval)    
def get_fixed_ip(info,neutron):
    """Input - Instance Info , NeutronClient Object
    Output - List of IP addresses
    Function - Parses IP addresses from Instance Info and converts it to required format
    """
    try:
        nics = []
        for net in info.get('addresses',''):
            tmp_dict = {} 
            for nic in info.get('addresses')[net]:
                net_id = [ x['id'] for x in neutron.list_networks(name=net)['networks'] if len(x) ]
                tmp_dict['net-id'] = net_id[0]
                for port in info.get('addresses')[net][0]:
                    tmp_dict['v%s-%s-ip'%(nic['version'],nic['OS-EXT-IPS:type'])]= nic['addr']
                    nics.append(tmp_dict)
                    return nics
    except Exception as e:
        ha_agent.warning("Exception: get_fixed_ip")
        ha_agent.exception('')
        
    
@retry(retry_on_exception=api_failure,stop_max_attempt_number=api_retry_count,wait_fixed=api_retry_interval)                  
def attach_flt_ip(ip_list,instance_object):
    """Input - List of Tuples containing Floating IPs and Fixed IPs 
             - Instance Object
    Output - True | False
    Function - Loop through the list and attach floating ip to instance
    """
    
    try:
        for flt_ip,fix_ip in ip_list:
            instance_object.add_floating_ip(flt_ip,fix_ip)
    except Exception as e:
        ha_agent.warning("Exception: attach_flt_ip")
        ha_agent.exception('')

def remove_fixed_ip(nova,inst_id,fixed_ip):
    """Input - NovaClient , Instance Id, Fixed IP
    Output - NaN
    Function - Removes Fixed Ip 
    """
    try:
        nova.servers.remove_fixed_ip(inst_id,fixed_ip)
    except Exception as e:
        ha_agent.warning("Exception: remove_fixed_ip")
        ha_agent.exception('')

def remove_floating_ip(nova,inst_id,floating_ip):
    """Input - NovaClient , Instance Id, Floating IP
    Output - NaN
    Function - Removes Floating Ip 
    """
    try:
        nova.servers.remove_floating_ip(inst_id,floating_ip)
    except Exception as e:
        ha_agent.warning("Exception: remove_floating_ip")
        ha_agent.exception('')


#Migration
def recreate_instance(nova,instance_object,target_host=None,bdm=None,neutron=None):
    '''Input - NovaClient , Instance Object , Target Host Name , BDM , NeutronClient
    Output - Instance Object
    Function - Takes (Instance Object ,Target Host)  as input,Deletes the Instance,Creates similiar Instance on another Healthy Host 
    '''
    
    #volume_delete_on_terminate Flip if not
    
    info = instance_object._info
    host=info['OS-EXT-SRV-ATTR:host']
    name= info['name']
    inst_id = info['id']
    flavor= info['flavor']['id']
    availability_zone=info['OS-EXT-AZ:availability_zone']
    if target_host:
        availability_zone+=":%s"%target_host
    disk_config='AUTO'
    moved = info['metadata'].get('moved')
    origin = info['metadata'].get('origin')
    if origin == target_host:
        moved = '0'
    meta = {'moved':moved,'origin': host }
    image = dict(info['image']).get('id','')
    security_groups = [x['name'] for x in info.get('security_groups','')]
    if len(security_groups) == 0:
        security_groups = ['default']
    
    nics = get_fixed_ip(info,neutron)
    
    #time.sleep(15)
    instance_object = create_instance(nova,name=name,image=image,bdm=bdm,\
                    flavor=flavor,nics=nics,availability_zone=availability_zone,\
                    disk_config=disk_config,meta=meta,security_groups=security_groups) 
    #create_instance_status(instance_object)
    return instance_object
 
#HA-Agent Migration Functions
def instance_migration(nova,dhosts,task,time_suffix):
    """Input - NovaClient , list of down Hosts , Task (passed to message_queue), Time string to track down time
    Output - NaN
    Function - 1.On the first iteration creates parent Nodes for the down host:
                        a. /openstack_ha/instances/pending/<<HOST>>
                        b. /openstack_ha/instances/migrated/<<HOST>>
                        c. /openstack_ha/instances/failure/<<HOST>>
                        d. /openstack_ha/instances/down_instances/<<HOST>>
            2. Adds all instances on the down node to /openstack_ha/instances/down_instances/<<HOST>>
    """
    for dhost in dhosts:

        if(zk.exists("/openstack_ha/instances/pending/" + dhost)==None):
            zk.create("/openstack_ha/instances/pending/" + dhost)
        if(zk.exists("/openstack_ha/instances/migrated/" + dhost)==None):
            zk.create("/openstack_ha/instances/migrated/" + dhost)
        if(zk.exists("/openstack_ha/instances/failure/" + dhost)==None):
            zk.create("/openstack_ha/instances/failure/" + dhost)
        
        if(zk.exists("/openstack_ha/instances/down_instances/" + dhost)==None):
            zk.create("/openstack_ha/instances/down_instances/" + dhost)
            for instance_obj in list_instances(nova,dhost):
                # Addon-Feature
                # Can Add another check to only select instances which have HA option enabled
                zk.create("/openstack_ha/instances/down_instances/" + dhost+"/"+instance_obj.id)
                #create instance detatils under the down hosts in zookeeper
        #After adding  Down Host and its instances to Zookeeper message_queue function which process 
        #Instances in Batch is called
        message_queue(dhost,task,time_suffix)

def message_queue(dhost=None,task=None,time_suffix=None):
    """Input - Down Host,task , Time string to track down time
    Output - NaN
    Function - 1. Fetches List of instances to be migrated from zookeeper
                i.e - /openstack_ha/instances/down_instances/<<HOST>>

    2.  Fetches List of instances in migrating state
            i.e /openstack_ha/instances/pending/<<HOST>>

    3.  Adds Instance to zookeeper Node /openstack_ha/instances/pending/<<HOST>> 
        based on num_instances_batch, pending instance counts and removes from 
        /openstack_ha/instances/down_instances/<<HOST>>

    4.  Adds Instance to Migration Agent queue
    """
    instance_list=zk.get_children("/openstack_ha/instances/down_instances/" + dhost)
    pending_instances_list=zk.get_children("/openstack_ha/instances/pending/"+dhost)
    scheduler_log.debug("Instances yet to be added to Migration Queue: %d"%(len(instance_list)))
    scheduler_log.debug("Instances already on Migration Queue: %d"%(len(pending_instances_list)))
    if(len(instance_list)!=0):
       scheduler_log.debug("Instances yet to be handled: %d Instances on Queue:  %d"%(len(instance_list),len(pending_instances_list)))

        if(len(pending_instances_list)<num_instances_batch):
            add_pending_instance_list=num_instances_batch-len(pending_instances_list)
            scheduler_log.debug("Adding %d more instances to Queue"%add_pending_instance_list)
            for i in range(add_pending_instance_list):
                try:
                    zk.create("/openstack_ha/instances/pending/" + dhost+"/"+instance_list[i])
                    zk.delete("/openstack_ha/instances/down_instances/" + dhost + "/" + instance_list[i],recursive=True)
                    instance_string = str(instance_list[i])
                    task.apply_async((instance_string,time_suffix,), queue='mars', countdown=5)
                except Exception as e:
                    scheduler_log.warning("Exception: message_queue Function..!")
        else:
            scheduler_log.debug("Waiting.. .. ..Migration Queue is Already Full...")

    else:
        if (zk.exists("/openstack_ha/hosts/down/" + dhost) == None):
            zk.create("/openstack_ha/hosts/down/" + dhost)


#Json Dump
def json_dump_creation(nova=None,instance_id=None,cinder=None,\
                       neutron=None,old_instance_id=None):
    try:
            instance_object,info,ip_list,bdm,extra = info_collection(nova,instance_id,cinder) 
            tmp_host=""           
            instance_name=""             
            flavor=""
            image=""
            security_groups=""
            volumes={}            
            instance_name=instance_object.name
            instance_id=instance_object.id
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
                
            flavor= info['flavor']['id']
            image = dict(info['image']).get('id','')
            security_groups = [x['name'] for x in info.get('security_groups','')]            
            data={"instance_name":instance_name,"host_name":tmp_host,"instance_id":instance_id,\
                  "old_instance_id":old_instance_id,"flavor":flavor,"image":image,"security_groups":security_groups,\
                  "folating_ip":folating_ip,"fixed_ip":fixed_ip,"volume":volume}            
            old_instance=json.dumps(data,ensure_ascii=True)
            old_instance_json=str.encode(old_instance)
            #write the instance details to json file           
            return data,old_instance_json
    except Exception as e:
            print(e)
            ha_agent.debug('Json Dump Exception')
            ha_agent.exception('')
            
def json_dump_write(filename=None,data=None):
    file_path = dump_directory + filename
    with open(file_path, 'a+') as outfile:
                outfile.write('\n')
                json.dump(data, outfile, indent=4, sort_keys=True, separators=(',', ':'))
                outfile.write(',')

def json_dump_edit(data=None,new_instance_id=None,new_host_name=None):
    old_instance_id=data["instance_id"]  
    old_host_name=data["host_name"]
    data["instance_id"] =new_instance_id
    data["host_name"]=new_host_name
    data["old_instance_id"] =old_instance_id
    data["old_host_name"]=old_host_name
    old_instance=json.dumps(data,ensure_ascii=True)
    old_instance_json=str.encode(old_instance)
    return data,old_instance_json


#Notification
def notification_mail(subj="",msg="",to_email=to_email,email=email,pwd=pwd):
    server = smtplib.SMTP('smtp.webfaction.com',25)
    server.starttls()
    server.login(email,pwd)

    subj = subj
    msg = msg
    email = email + "@naanal.in"
    print("mail in commonfunctions.........!!!")

    for i in to_email:
        BODY = string.join((
            "From: %s" % email,
            "To: %s" % to_email,
            "Subject: %s" % subj ,
            "",
            msg
            ), "\r\n")
        server.sendmail(email,to_email,BODY)
    server.quit()
