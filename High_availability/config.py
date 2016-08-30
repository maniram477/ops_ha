#!/usr/bin/python

#Config File
from common_functions import get_ip_address
ip_address = get_ip_address('br-mgmt')

BROKER_URL = 'amqp://nova:FWlPjEo0@%s:5673//'%ip_address

#CELERY_RESULT_BACKEND = "amqp"
CELERY_RESULT_BACKEND = None
#CELERY_RESULT_BACKEND = "db+sqlite:///results.db"
#CELERY_RESULT_DBURI = 'mysql://user:password@localhost/database'
CELERYD_CONCURRENCY = 1
#CELERY_TASK_RESULT_EXPIRES=2
