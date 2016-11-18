#!/usr/bin/python

#Config File
from common_functions import get_ip_address
ip_address = get_ip_address('br-mgmt')
RABBIT_USER = 'nova'
RABBIT_PASSWORD = 'WUadxi939gMnEFkRhVnbF5Fm'
BROKER_URL = 'amqp://%s:%s@%s:5673//'%(RABBIT_USER,RABBIT_PASSWORD,ip_address)

#CELERY_RESULT_BACKEND = "amqp"
CELERY_RESULT_BACKEND = None
#CELERY_RESULT_BACKEND = "db+sqlite:///results.db"
#CELERY_RESULT_DBURI = 'mysql://user:password@localhost/database'
CELERYD_CONCURRENCY = 20
#CELERY_TASK_RESULT_EXPIRES=2
