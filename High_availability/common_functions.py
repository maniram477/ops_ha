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
from ha_agent import test,migrate


controller_ip="30.20.0.2"
user ="admin"
passwd = "admin"
tenant = "admin"
wait_time = 5 #In Seconds - Based on SLA

host_name=socket.gethostname()


# Use Variables inside retry Functions
scheduler_interval = 5 #In Seconds
api_retry_count = 3 
api_retry_interval = 1000 #In MilliSeconds

poll_status_count = 100
poll_status_interval = 2000 #In MilliSeconds

migrate_time=60# In Seconds

maintenance_state = ['maintenance','skip','pause_migration']
kazoo_exceptions = [obj for name, obj in inspect.getmembers(kexception) if inspect.isclass(obj) and issubclass(obj, Exception)]

zk = KazooClient(hosts='127.0.0.1:2181')

nova = nova_client.Client(2,user,passwd,tenant,"http://%s:5000/v2.0"%controller_ip,connection_pool=True)
#conn = MySQLdb.connect(controller_ip,mysql_user,mysql_pass)

# Retry Functions
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

# Health Check Functions
def ping_check(hostname):
    print("Pinging ",hostname)
    response = os.system("ping -c 1 " + hostname)

    #and then check the response...
    if response == 0:
        print hostname, 'is up!'
        return True
    else:
        print hostname, 'is down!'
        return False


#common Funcitons

def dbwrap(func):
    """Wrap a function in an idomatic SQL transaction.  The wrapped function
    should take a cursor as its first argument; other arguments will be
    preserved.
    """
    def new_func(conn, *args, **kwargs):
        cursor = conn.cursor()
        try:
            retval = func(cursor, *args, **kwargs)
        except Exception as e:
            #log.error()
            retval = None
            print e
        finally:
            cursor.close()
        return retval
    return new_func


# Host Functions
@retry(retry_on_exception=api_failure,stop_max_attempt_number=3,wait_fixed=1000)
def list_hosts():
    try:
        return {'all_list': [host.host for host in nova.services.list(binary="nova-compute")],\
                'down_list': [host.host for host in nova.services.list(binary="nova-compute") if host.state.lower() == 'down'],\
                'disabled_list': [host.host for host in nova.services.list(binary="nova-compute") if host.status.lower() == 'disabled' if host.disabled_reason in maintenance_state]\
               }
    except Exception as ee:
        print(ee)
        raise Exception('step0')

def down_hosts():
    return [host.host for host in nova.services.list(binary="nova-compute") if host.state.lower() == 'down']


def active_hosts():
    return [host.host for host in nova.services.list(binary="nova-compute") if host.state.lower() == 'up']


def host_disable():
    pass




@retry(retry_on_exception=api_failure,stop_max_attempt_number=3,wait_fixed=1000)
def client_init():
    try:
        neutron = neutron_client.Client(username=user,
                                            password=passwd,
                                            tenant_name=tenant,
                                            auth_url="http://%s:5000/v2.0"%controller_ip)
        cinder = cinder_client.Client(1,user,passwd,tenant,"http://%s:5000/v2.0"%controller_ip)
        return cinder,neutron
    except Exception as ee:
        print(ee)
        raise Exception('step1')
    




# Instacne Functions
@retry(retry_on_exception=api_failure,stop_max_attempt_number=3,wait_fixed=1000)
def info_collection(instance_id):
    try:
        instance = nova.servers.get(instance_id)
        info = instance._info
        ip_list = floating_ip_check(info)
        bdm,extra = cinder_volume_check(info,cinder=cinder)
        return instance,info,ip_list,bdm,extra
    except Exception as ee:
        print(ee)
        raise Exception('step2')

        
@retry(retry_on_exception=api_failure,stop_max_attempt_number=3,wait_fixed=1000)
def delete_instance(instance_object):
    """Input - Instance Object
    Op - Deletes Instance
    Output - True | False
    """
    try:
        nova.servers.delete(instance_object.id)
    except Exception as e:
        print('Exception in step5',e)

@retry(retry_on_exception=api_failure,stop_max_attempt_number=3,wait_fixed=1000)
def delete_instance_status(instance_object):
    try:
        allow_retry_task = ['deleting',None]
        tmp_ins = nova.servers.get(instance_object.id)
        if tmp_ins._info['OS-EXT-STS:vm_state'] == 'error':
            tmp_ins.force_delete()
        elif tmp_ins._info['OS-EXT-STS:task_state'] in allow_retry_task:
            raise Exception("poll")
    except Exception as e:
        print('Exception in step6: ',e)
        if isinstance(e,exceptions.NotFound):
            print("Instance Not Found hence deleted")
        else:
            raise Exception(e) 

   

def list_instances(host_name=None):
    """Input - Hostname (optional)
    Op - List Instances (on specific Host if Host given as Input | on All Host ) 
    Output - Instance List
    """
    ins_list = nova.servers.list(search_opts={'host':host_name})
    return ins_list

def delete_instance(instance_object):
    """Input - Instance Object
    Op - Deletes Instance
    Output - True | False
    """
    try:
        nova.servers.delete(instance_object.id)
    except Exception as e:
        print e
        return False
    else:
        print instance_object.id,"Deleted"
        return True
    
@dbwrap
def get_instance_uuid(cursor,name):
    cursor.execute("select uuid from nova.instances  where display_name='%s' and deleted=0 order by created_at desc;"%name)
    return cursor.fetchone()    
    
def get_instance(name):
    """Input - Name of the Instance 
    Op - Finds the newly created Instance with display_name,deleted,meta as identifiers
    Output - Instance Object
    """
    uuid = get_instance_uuid(con,name)
    try:
        instance = nova.servers.get(uuid)
    except Exception as e:
        print e
        #log.error(e)
        return None
    return instance
 

@retry(retry_on_exception=api_failure,stop_max_attempt_number=3,wait_fixed=1000)               
def create_instance(name=None,image=None,bdm=None,\
                         flavor=None,nics=None,availability_zone=None,\
                         disk_config=None,meta=None,security_groups=None):
    try:
        
        instance_object = nova.servers.create(name=name,image=image,block_device_mapping=bdm,\
                             flavor=flavor,nics=nics,availability_zone=availability_zone,\
                             disk_config=disk_config,meta=meta,security_groups=security_groups)
        return instance_object
    except Exception as e:
        print('Exception in step8',e)

@retry(retry_on_exception=poll_vm_status,stop_max_attempt_number=3,wait_fixed=1000)               
def create_instance_status(instance_object):
    try:        
        allow_retry = ['spawning','building','starting','powering_on','scheduling','block_device_mapping','networking']
        tmp_ins = nova.servers.get(instance_object.id)
        status = ( tmp_ins._info['OS-EXT-STS:vm_state'], tmp_ins._info['OS-EXT-STS:task_state'] )
        if status[0] == 'active':
            print("Success")
        elif status[1] in allow_retry:
            raise Exception("poll")
        elif status[0] == 'error':
            raise Exception("error")
    except Exception as e:
        print('Exception in step9',e)
        if e.message == 'error':
            print("Instance - %s went to ERROR state'%(instance_object.id)")
        raise Exception(e)
    

    
    
    
# Volume Related Functions     

        
@retry(retry_on_exception=api_failure,stop_max_attempt_number=3,wait_fixed=1000)
def detach_volume(volume,cinder=None):
    """Input - Volumes - Dictionary Containing volume details 
    Eg: {'/dev/vdb':'16e5593c-15c7-48a6-b46f-2bda2951e3b0','/dev/vdc':'16e5593c-15c7-48a6-b46f-2bda2951esd0'}
    Op - Detach Volumes From Instance
    Output - True | False
    """
    try:
        cinder.volumes.detach(volume[dev])
    except Exception as e:
        print('Soft Exception in step3',e)

@retry(retry_on_exception=poll_status,stop_max_attempt_number=3,wait_fixed=1000)      
def detached_volume_status(volume):
    try:
        allow_retry = ['detaching','in-use'] 
        tmp_vol = cinder.volumes.get(volume)
        if tmp_vol.status in allow_retry:
            raise Exception("poll")
    except Exception as e:
        print(e)
        if e.message == 'poll':
            raise Exception("poll")
        else:
            raise Exception('step4')


def cinder_volume_check(info,cinder=None):
    """info - instance._info - Information from instance Object
    This Function should uncheck Volume_delete_on_terminate and return volume details
    Should Change Volume Status from in_use to available"""
    
    bdm = {}
    volumes ={}
    try:
        volumes = {cinder.volumes.get(x['id']).attachments[0]['device']:x['id'] for x in info.get('os-extended-volumes:volumes_attached') }
        
        
        if volumes.has_key('/dev/vda'):
            bdm = {'vda': volumes['/dev/vda']}
            del(volumes['/dev/vda'])
            
    except Exception as e:
        #log.warning(e)
        print e
        bdm = None
    else:
        #if block_device_mapping.has_key('vda'):
        #    image=''
        pass
    return bdm,volumes

        
@retry(retry_on_exception=api_failure,stop_max_attempt_number=3,wait_fixed=1000)                  
def attach_volumes(instance,volumes):
    """Input - Volumes - Dictionary Containing volume details 
    Eg: {'vdb':'16e5593c-15c7-48a6-b46f-2bda2951e3b0','vdc':'16e5593c-15c7-48a6-b46f-2bda2951esd0'}
        - Instance ID
    Op - Attach Volumes to Instances
    """
    try:
        for dev in volumes:
            nova.volumes.create_server_volume(instance,volumes[dev],dev)
    except Exception as e:
        print('Exception in step11',e)

    

# IP Functions
def floating_ip_check(info):
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
         
@retry(retry_on_exception=api_failure,stop_max_attempt_number=3,wait_fixed=1000)    
def get_fixed_ip(info,neutron):
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
        print('Exception in step7',e)    
        
    
@retry(retry_on_exception=api_failure,stop_max_attempt_number=3,wait_fixed=1000)                  
def attach_flt_ip(ip_list,instance_object):
    """Input - List of Tuples containing Floating IPs and Fixed IPs 
             - Instance Object
    Op - Loop through the list and attach floating ip to instance
    Output - True | False
    """
    
    try:
        for flt_ip,fix_ip in ip_list:
            instance_object.add_floating_ip(flt_ip,fix_ip)
    except Exception as e:
        print('Exception in step10',e)
    
def recreate_instance(instance_object,target_host=None,bdm=None,neutron=None):
    '''Takes (Instance Object ,Target Host)  as input,Deletes the Instance,Creates similiar Instance on another Healthy Host 
       returns (True | False) '''
    
    #volume_delete_on_terminate Flip if not
    
    info = instance_object._info
    host=info['OS-EXT-SRV-ATTR:host']
    name= info['name']
    print name
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
    security_groups = [x['name'] for x in info.get('security_groups')]
    
    nics = get_fixed_ip(info,neutron)
    
    #time.sleep(15)
    instance_object = create_instance(name=name,image=image,bdm=bdm,\
                    flavor=flavor,nics=nics,availability_zone=availability_zone,\
                    disk_config=disk_config,meta=meta,security_groups=security_groups) 
    create_instance_status(instance_object)
    return instance_object
 
#HA-Agent Migration Functions
def instance_migration(dhosts):
    for dhost in dhosts:

        if(zk.exists("/openstack_ha/instances/pending/" + dhost)==None):
            zk.create("/openstack_ha/instances/pending/" + dhost)
        if(zk.exists("/openstack_ha/instances/migrated/" + dhost)==None):
            zk.create("/openstack_ha/instances/migrated/" + dhost)
        if(zk.exists("/openstack_ha/instances/failure/" + dhost)==None):
            zk.create("/openstack_ha/instances/failure/" + dhost)
        
        if(zk.exists("/openstack_ha/instances/down_instances/" + dhost)==None):
            zk.create("/openstack_ha/instances/down_instances/" + dhost)
            for instance_obj in list_instances(dhost):
                # Addon-Feature
                # Can Add another check to only select instances which have HA option enabled
                print(instance_obj.id)
                zk.create("/openstack_ha/instances/down_instances/" + dhost+"/"+instance_obj.id)
                #create instance detatils under the down hosts in zookeepr
                #migrate.apply_async((instance_obj.id,), queue='mars', countdown=wait_time)
        message_queue(dhost)


def message_queue(dhost=None):
    instance_list=zk.get_children("/openstack_ha/instances/down_instances/" + dhost)
    pending_instances_list=zk.get_children("/openstack_ha/instances/pending/"+dhost)

    if(len(instance_list)!=0):
        #while(len(instance_list)!=0)
        #instance_list = zk.get_children("/openstack_ha/instances/down_instances/" + dhost)
        print("Instances yet to be handled: ",instance_list," Instances on Queue: ", pending_instances_list )
        if(len(pending_instances_list)<10):
            add_pending_instance_list=10-len(pending_instances_list)
            for i in range(add_pending_instance_list):
                print("Adding %d more instances to Queue"%add_pending_instance_list)
                try:
                    zk.create("/openstack_ha/instances/pending/" + dhost+"/"+instance_list[i])
                    zk.delete("/openstack_ha/instances/down_instances/" + dhost + "/" + instance_list[i],recursive=True)
                    test.apply_async((instances_list[i],), queue='mars', countdown=5)
                except Exception as e:
                    print(e)

            #afteradd_pending_instances_list = zk.get_children("/openstack_ha/instances/pending/" + dhost)
            #for j in afteradd_pending_instances_list:
            #    task.apply_async((afteradd_pending_instances_list[j],), queue='mars', countdown=wait_time)
    else:
        if (zk.exists("/openstack_ha/hosts/down/" + dhost) == None):
            zk.create("/openstack_ha/hosts/down/" + dhost, b"a value", None, True)
