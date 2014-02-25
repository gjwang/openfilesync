'''
Created on 2013-8-6

@author: gjwang
'''


import logging.handlers
from pprint import pformat
import time
from datetime import timedelta
from apscheduler.scheduler import Scheduler 

from celery import Celery
from celery.task.sets import TaskSet
from celery.schedules import crontab
from celery.events.state import Worker

from celeryfilesync.visitdir import visitdir
from celeryfilesync.tasks import add, sendMsg, download, download_list

from config import monitorpath, wwwroot, httphostname, exclude_exts, exclude_exts, broker

WHOLE_SYNC_INTERVAL = 60*30

timeout = 10

def my_monitor(app):
    _logging = logging.getLogger()

    state = app.events.State()
    app.control.rate_limit('celeryfilesync.tasks.download_list', '1/m')

    inspecter = app.control.inspect(timeout=timeout)

    workers_lastonlinetime = {}
    worker_queue = {}

    workers_offlinetime = {}
    workers_status = {}#'workername':{'is_online':True, 'last_online_time': 138, 'last_whole_sync_time': 138, 
                       #'offline_time':'', 'whole_sync_task_ids'['', '']  }


    def get_worker_queue():
        active_queues = inspecter.active_queues() or {}

        for worker in active_queues:
            queue0 = active_queues[worker][0]
            worker_queue[worker] = queue0['name']

	    worker_st = workers_status.get(worker)
            if worker_st is None:
                worker_st = {}
                worker_st['queue'] = queue0['name']
                worker_st['is_online'] = True
                worker_st['last_online_time'] = time.time()
		worker_st['offline_time'] = None
                worker_st['last_whole_sync_time'] = 0
                worker_st['whole_sync_task_ids'] = []
                workers_status[worker] = worker_st
	    else:
                worker_st['queue']= queue0['name']
                worker_st['is_online'] = True

    def allhost_do_whole_sync():
        get_worker_queue()

        dirslist, fileslist = visitdir(monitorpath, wwwroot, exclude_exts)
        queues = worker_queue.values() 

        #for queue in queues:
	for worker, status in workers_status.items():
            if status['is_online']:
                if time.time() - status['last_whole_sync_time'] < WHOLE_SYNC_INTERVAL:
		    _logging.error('woker:%s do whole sync too frequent(less than %s seconds), ignore this time',
                                    worker, WHOLE_SYNC_INTERVAL)
                    continue

                #TODO: revoke the unstarted whole sync tasks
                queue = status['queue']
                _logging.info("%s online now. Do a whole sync", worker)
                res = download_list.apply_async(args=(dirslist, fileslist, httphostname), queue=queue, expires=1800, retry=False)
                try:
                    _logging.info("taskid: %s", res.id)
                    #status['whole_sync_task_ids'].append(res.id)
                except Exception as e:
                    _logging.error('get taskid exception: %s', e)

        
    #notifiy hosts a whole sync at startup
    _logging.info("notifiy all hosts a whole sync at startup")
    allhost_do_whole_sync()

    sched = Scheduler()          
    #sched.daemonic = False  
    sched.add_cron_job(allhost_do_whole_sync , day_of_week='*', hour=2, minute=0,second=0)
    sched.start()  


    #notify a host to do a whole sync
    def do_whole_sync(hostname):
        get_worker_queue()
        queue = worker_queue.get(hostname, None)
        if queue:
            dirslist, fileslist = visitdir(monitorpath, wwwroot, exclude_exts)

            _logging.info("%s:%s worker online now. Do a whole sync", hostname, queue)
            try:
                res = download_list.apply_async(args=(dirslist, fileslist, httphostname), queue=queue, expires=1800, retry=False)
                _logging.info("taskid: %s", res.id)
            except Exception as e:
                _logging.error("Exception: %s", e)


    def announce_failed_tasks(event):
        state.event(event)
        task_id = event['uuid']
        _logging.error("TASK FAILED: event: %s", pformat(event, indent=4))

        #print('TASK FAILED: %s[%s] %s' % (
        #    event['name'], task_id, state[task_id].info(), ))

    def worker_online(event):
        hostname = event['hostname']
        state.event(event) #this method would delete hostname field
        #print('event: %s' % (pformat(event, indent=4), ))
        #print event['type']    
        
        timestamp = event['timestamp']

        workers_offlinetime.pop(hostname, None)

        if hostname in workers_lastonlinetime:
            #filter the same online event
            if (int(timestamp) - int(workers_lastonlinetime[hostname])) < WHOLE_SYNC_INTERVAL:
                #print('the same event: %s' % (pformat(event, indent=4), ))
                return

            _logging.info("worker %s last online time: %s", hostname, workers_lastonlinetime[hostname])

        workers_lastonlinetime[hostname] = timestamp
        #worker_st = workers_status.get(hostname)
        #if worker_st is not None:
        #    worker_st['is_online'] = True
        #    worker_st['last_whole_sync_time'] = time.time()


        do_whole_sync(hostname)
 
    def on_worker_offline(event):
        hostname = event['hostname']
        state.event(event)
        #print('event: %s' % (pformat(event, indent=4), ))
        #print event['type'] 
        timestamp = event['timestamp']
        
        _logging.info("worker %s offline time %s", hostname, timestamp)
        
        worker_st = workers_status.get(hostname)
        if worker_st is not None:
            worker_st['is_online'] = False
            worker_st['offline_time'] = time.time()

    def announce_dead_workers(event):
        hostname = event['hostname']
        #print('alive event: %s' % (pformat(event, indent=4), )) 
        state.event(event)  

        workers = dict(state.workers)
        for worker in workers:
            hostname = worker
                
            if not workers[hostname].alive:                
                if workers_offlinetime.get(hostname):
                    return
                else:
                    workers_offlinetime[hostname] = event['timestamp']
                    _logging.info('Worker %s missed heartbeats', hostname)
            

         
    def on_events(event):
        hostname = event['hostname']
        #print('event: %s' % (pformat(event, indent=4), ))
        state.event(event)
        #print('event2: %s' % (pformat(event, indent=4), ))
        timestamp = event['timestamp']
        _logging.info("%s, eventtype: %s, time: %s", hostname, event['type'], timestamp)

    def capture():
        with app.connection() as connection:
            recv = app.events.Receiver(connection, handlers={
                    'task-failed': announce_failed_tasks,
                    'worker-heartbeat': announce_dead_workers,
                    'worker-online': worker_online,
                    'worker-offline': on_worker_offline,
                    '*': on_events,
            })
            recv.capture(limit=None, timeout=None, wakeup=True)

    capture()


if __name__ == '__main__':
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    log_FileHandler = logging.handlers.TimedRotatingFileHandler(filename = "log/monitor_workersonline.log",
                                                                when = 'midnight',
                                                                interval = 1,
                                                                backupCount = 7)
    
    log_FileHandler.setFormatter(formatter)
    log_FileHandler.setLevel(logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(log_FileHandler)


    celery = Celery(broker=broker)
    #celery = Celery(broker='amqp://guest:guest@127.0.0.1:5672//')
    print "begin to monitor workers online..."
    logger.info("begin to monitor workers online...")
    #my_monitor(celery)

    retries = 1
    while True:
        try:
            #celery = Celery(broker=broker)
	    my_monitor(celery)
        except Exception as exc:
	    retries += 1
            sectime = min(2 ** retries, 1800)
            logger.error('lost connection, retries in %s secs: %s', sectime, exc)
            time.sleep(sectime)
    
