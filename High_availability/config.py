#!/usr/bin/python

#Config File for Celery
from common_functions import BROKER_URL , CELERY_RESULT_BACKEND , CELERYD_CONCURRENCY

#BROKER_URL = 'amqp://%s:%s@%s:5673//'%(RABBIT_USER,RABBIT_PASSWORD,ip_address)

#CELERY_RESULT_BACKEND = "amqp"
#CELERY_RESULT_BACKEND = None
#CELERY_RESULT_BACKEND = "db+sqlite:///results.db"
#CELERY_RESULT_DBURI = 'mysql://user:password@localhost/database'
#CELERYD_CONCURRENCY = 20
#CELERY_TASK_RESULT_EXPIRES=2
