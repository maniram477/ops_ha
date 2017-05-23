#!/usr/bin/python
from novaclient import client as nova_client
import time
import sys
from ha_agent import migrate as task
from common_functions import *
host_name=None

#


def list_instances_2(nova,host_name=None):
    """Input - Hostname (optional)
    Op - List Instances (on specific Host if Host given as Input | on All Host )
    Output - Instance List
    """
    if host_name:
        ins_list = nova.servers.list(search_opts={'host':host_name})
    else:
        print("No host Name Given")
        ins_list = []
    return ins_list

def migrate_host_2(host_name=None):
    temp_time = time.localtime(time.time())
    time_suffix = str(temp_time.tm_mday) + "_" + str(temp_time.tm_mon) + "_" + \
                  str(temp_time.tm_year) + "_" + str(temp_time.tm_hour) + "_" + \
                  str(temp_time.tm_min)

    nova = nova_client.Client(2, user, passwd, tenant, "http://%s:5000/v2.0" % controller_ip, connection_pool=True)
    instance_list=list_instances_2(nova=nova,host_name=host_name)

    for instance in instance_list:
        task.apply_async((instance.id,time_suffix,), queue='mars', countdown=5)


if __name__ == __main__:
    if len(sys.argv) >= 2:
        host_name = sys.argv[1]
        migrate_host_2(host_name)
    else:
        if host_name:
            migrate_host_2(host_name)
        else:
            print("Hostname Argument missing")
