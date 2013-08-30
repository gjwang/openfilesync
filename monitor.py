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

from config import monitorpath, wwwroot, httphostname, exclude_exts, broker

timeout = 2000 / 1000.0

def my_monitor(app):
    _logging = logging.getLogger()

    state = app.events.State()
    inspecter = app.control.inspect(timeout=timeout)

    workers_lastonlinetime = {}
    worker_queue = {}

    workers_offlinetime = {}

    def get_worker_queue():
        active_queues = inspecter.active_queues() or {}

        for worker in active_queues:
            queue0 = active_queues[worker][0]
            worker_queue[worker] = queue0['name']
            
        #print worker_queue

    def allhost_do_whole_sync():
        get_worker_queue()

        dirslist, fileslist = visitdir(monitorpath, wwwroot, exclude_exts)
        queues = worker_queue.values() 

        for queue in queues:
            print "%s online now. Do a whole sync" % (queue)
            _logging.debug("download_list.apply_async(args=(%s, %s), queue=%s)", dirslist, fileslist, queue)
            res = download_list.apply_async(args=(dirslist, fileslist, httphostname), queue=queue)
            try:
                print "taskid: %s" % (res.id)
                _logging.info("taskid: %s", res.id)
            except Exception as e:
                print e

        
    #notifiy hosts a whole sync at startup
    print "notifiy all hosts a whole sync at startup"
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

            print "%s:%s worker online now. Do a whole sync" % (hostname, queue)
            _logging.info("%s:%s worker online now. Do a whole sync", hostname, queue)
            _logging.debug("download_list.apply_async(args=(%s, %s), queue=%s)", dirslist, fileslist, queue)
            try:
                print "httphostname: ", httphostname
                res = download_list.apply_async(args=(dirslist, fileslist, httphostname), queue=queue)
                print "taskid: %s" % (res.id)
                _logging.info("taskid: %s", res.id)
            except Exception as e:
                print e
                _logging.error("Except: %s", e)


    def announce_failed_tasks(event):
        state.event(event)
        task_id = event['uuid']
        print('event: %s' % (pformat(event, indent=4), ))
        _logging.error("TASK FAILED: event: %s", pformat(event, indent=4))

        #print('TASK FAILED: %s[%s] %s' % (
        #    event['name'], task_id, state[task_id].info(), ))

    def worker_online(event):
        hostname = event['hostname']
        state.event(event) #this methon would delete hostname field
        #print('event: %s' % (pformat(event, indent=4), ))
        #print event['type']    
        
        timestamp = event['timestamp']

        #if workers_offlinetime.get(hostname):
        #   del workers_offlinetime[hostname]

        workers_offlinetime.pop(hostname, None)

        if hostname in workers_lastonlinetime:
            #filter the same online event
            if (int(timestamp) - int(workers_lastonlinetime[hostname])) < 5:
                #print('the same event: %s' % (pformat(event, indent=4), ))
                return

            _logging.info("worker %s last online time: %s", hostname, workers_lastonlinetime[hostname])

        workers_lastonlinetime[hostname] = timestamp
        do_whole_sync(hostname)
 
    def on_worker_offline(event):
        hostname = event['hostname']
        state.event(event)
        #print('event: %s' % (pformat(event, indent=4), ))
        #print event['type'] 
        timestamp = event['timestamp']
        #print "%s last offline time %d" % (hostname, timestamp)
        
        _logging.info("worker %s offline time %s", hostname, timestamp)
        

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
                    print('Worker %s missed heartbeats' % (hostname))
                    workers_offlinetime[hostname] = event['timestamp']
                    _logging.info('Worker %s missed heartbeats', hostname)
            

         
    def on_events(event):
        hostname = event['hostname']
        #print('event: %s' % (pformat(event, indent=4), ))
        state.event(event)
        #print('event2: %s' % (pformat(event, indent=4), ))
        timestamp = event['timestamp']
        #print "%s, eventtype: %s, time: %s" % (hostname, event['type'], timestamp)
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
            sectime = min(2 ** retries, 4096)
            logger.error('lost connection, retries in %s secs: %s', sectime, exc)
            time.sleep(sectime)
    
