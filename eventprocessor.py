# -*- coding: utf-8 -*-
'''
Created on 2013-8-6

@author: gjwang
'''

import logging.handlers
import copy
import time
import celery
from celery import current_app
from celery.task.sets import TaskSet
from celery import group
from celeryfilesync.tasks import download, download_list, rmfile, mkemptydir, rmemptydir, fsrename

import pyinotify
from apscheduler.scheduler import Scheduler

import os
from os.path import walk, isfile, join, dirname, basename, normpath, splitext, getsize
import threading
from threading import Condition, Lock

from celeryfilesync.visitdir import visitdir
from config import monitorpath, wwwroot, httphostname, broker, backend, exclude_exts

INSPECT_TIMEOUT = 10
CHECK_ACTIVE_QUEUE_TIME = 60  #seconds
MAX_OFFLINE_TIME =  3600*1    #seconds
WHOLE_SYNC_TASK_EXPIRES_TIME = MAX_OFFLINE_TIME/2
DOWNLOAD_TASK_EXPIRES_TIME = 3600*24 

class EventHandler(pyinotify.ProcessEvent):
    def __init__(self, app, monitorpath = '/data/', hostname = 'http://127.0.0.1'):
        self._logging = logging.getLogger(self.__class__.__name__)

        self.app = app
	app.control.rate_limit('celeryfilesync.tasks.download_list', '1/h')

        self.monitorpath = monitorpath
        if self.monitorpath.endswith('/') or self.monitorpath.endswith('\\'):
            self.monpath_length = len(self.monitorpath)
        else:
            self.monpath_length = len(self.monitorpath) + 1

        self.hostname = hostname
        self.inspecter = app.control.inspect(timeout=INSPECT_TIMEOUT)
        #self.workers_online = {}
        self.workers_status = {}#'workername':{'last_online_time': 138, 'last_whole_sync_time': 138,
                                #              'whole_sync_task_ids'['', '']  }

        #self._condition = Condition(Lock()) 

        self.activeQueueThread = self.CheckActivQueueThread(self.checkaliveworker)
        self.activeQueueThread.start()

        self.sched = Scheduler()
        self.sched.add_cron_job(self.all_workers_do_whole_sync , day_of_week='*', hour='*/3', minute=0, second=0)
        self.sched.start()

    def all_workers_do_whole_sync(self):
        dirslist, fileslist = visitdir(monitorpath, wwwroot, exclude_exts)
	now_time = int(time.time())
        for worker, status in self.workers_status.items():
	    if now_time - status['last_whole_sync_time'] < WHOLE_SYNC_TASK_EXPIRES_TIME:
	        self._logging.info("worker:%s do whole_sync too frequent(%s seconds), skip this time",
				    worker, now_time - status['last_whole_sync_time'])	
		continue

            try:
                res = download_list.apply_async(args=(dirslist, fileslist, httphostname),
                                         queue=status['queue'], expires=WHOLE_SYNC_TASK_EXPIRES_TIME, retry=False)
		status['last_whole_sync_time'] = now_time
                self._logging.info("cron job: worker=%s, whole_sync taskid: %s", worker, res.id)
            except Exception as e:
                self._logging.error('all_workers_do_whole_sync exception: %s', e)	

    class CheckActivQueueThread(threading.Thread):
        def __init__(self, func):
            threading.Thread.__init__(self)
            self._logging = logging.getLogger(self.__class__.__name__)
            self.stop = False
            self.func = func
        def run(self):
            while self.stop == False:
                try:
                    self.func()
                except Exception as e:
                    self._logging.error('check active workers exception: %s', e)  
                time.sleep(CHECK_ACTIVE_QUEUE_TIME)

        def stop(self):
            self.stop = True


    def checkaliveworker(self):
        #self._tmp_workers = {}

        self.active_queues = self.inspecter.active_queues() or {}
	time_now = int(time.time())

	dirslist = None
        fileslist = None
        for worker in self.active_queues:
            queue0 = self.active_queues[worker][0]
            #self._tmp_workers[worker] = queue0['name']

            status = self.workers_status.get(worker)
            if status is None:
		status = {}
                status['queue'] = queue0['name']
                status['last_whole_sync_time'] = 0
                self.workers_status[worker] = status
                try:
                    if dirslist is None:
		        dirslist, fileslist = visitdir(monitorpath, wwwroot, exclude_exts)
		    res = download_list.apply_async(args=(dirslist, fileslist, httphostname),
					 queue=status['queue'], expires=WHOLE_SYNC_TASK_EXPIRES_TIME, retry=False)
                    status['last_whole_sync_time'] = time_now
                    self._logging.info("worker: %s, new online, whole_sync taskid: %s", worker, res.id)
                    #status['whole_sync_task_ids'].append(res.id)
                except Exception as e:
                    self._logging.error('get whole_sync taskid exception: %s', e)

            status['last_online_time'] = time_now

        for worker, status in self.workers_status.items():
	    offline_time = int(time.time()) - status['last_online_time']
            if status['last_online_time'] < time_now:
                self._logging.info("worker: %s offline, status: %s", worker, status)

            if offline_time > MAX_OFFLINE_TIME:
		del self.workers_status[worker]
                self._logging.info("worker: %s offline time(%s) > max_offline_time(%s)", 
		                    worker, offline_time, MAX_OFFLINE_TIME)

	self._logging.info('workers status: %s\n', self.workers_status)


    def process_IN_CREATE(self, event):
        #print "event:", str(event)
        #self._logging.info("event: %s", str(event))
        #print "type event:", type(event)
        #print "event: %s" % pformat(event)

        if event.dir == True:
            pass
            #relativepath = event.pathname[self.monpath_length:]
            #self._logging.info("Creating folder: \'%s\', relativepath %s", event.pathname, relativepath)
            #mkemptydir(relativepath = relativepath)
        else:
            self._logging.info("Creating file: \'%s\'. Ignore this event", event.pathname)

        
    def process_IN_DELETE(self, event):
        #self._logging.info("Removing: %s", event.pathname)
        url = self.mergeurl(event.pathname)

        if event.dir == True:
            self._logging.info("remove folder: %s", event.pathname)
            self.notifyworker(rmemptydir, (url, None))
        else:
            if splitext(event.pathname)[1].lower() in exclude_exts:
                logger.info("exclude file: %s, skip", event.pathname)
                return 

            self._logging.info("remove file: %s", event.pathname)
            self.notifyworker(rmfile, (url, None))


    def process_IN_CLOSE_WRITE(self, event):
        #url = 'http://192.168.5.60/cctv/tencent.mp4'
        #url = 'http://223.82.137.218/live/test.ts'
        #url = 'http://192.168.5.60/cctv/cctv_000000001.ts'

        if splitext(event.pathname)[1].lower() in exclude_exts:
            self._logging.info("Closewrite: \'%s\', ignored it", event.pathname)
            return

        url = self.mergeurl(event.pathname)
        self._logging.info("Closewrite: %s, push: url: %s", event.pathname, url)
	try:
	    filesz = getsize(event.pathname)
            self.notifyworker(download, (url, filesz, None) )
	except Exception as exc:
	    self._logging.error("%s", exc)
            
 
    def process_IN_ISDIR(self, event):
        #print "isdir:", event.pathname
        pass

    def process_IN_MOVED_FROM(self, event):
        #print "movefrom:", event.pathname
        #print "event:", str(event)

        self._logging.info("movefrom: %s", event.pathname)
        #self._logging.info("event: %s", str(event))

    def process_IN_MOVED_TO(self, event):
        #print "moveto:", event.pathname
        #print "event:", str(event)
        self._logging.info("moveto: %s", event.pathname)
        #self._logging.info("event: %s", str(event))
        #url = self.mergeurl(event.pathname)

        #bug: after rename dir, the event.pathname remain the stll
        #if hasattr(event, 'src_pathname'):
        #    print "move from src path:", event.src_pathname, event.pathname
        #    self.notifyworker(fsrename, (event.src_pathname[self.monpath_length:], event.pathname[self.monpath_length:]))
        #else:
        #    self.notifyworker(rmfile, (url, None))            

    def mergeurl(self, pathname):
        url = os.path.join(self.hostname, pathname[self.monpath_length:].replace('\\', '/'))
        return url

    def notifyworker(self, func, arg = ()):
        #TODO: use broadcast to notify all hosts
        #results = []
        #tasks = []
        #self._condition.acquire()
        #try:            
        #    queues = self.workers_online.values()
        #finally:
        #    self._condition.release()

        for worker, status in self.workers_status.items():
            try:
		q = status['queue']
                self._logging.info("push: %s.apply_async(args=%s, queue=%s)", func, arg, q)
                res = func.apply_async(args=arg, queue=q, expires=DOWNLOAD_TASK_EXPIRES_TIME, retry=True,
                                                                     retry_policy={
                                                                                    'max_retries': 3,
                                                                                    'interval_start': 5,
                                                                                    'interval_step': 1,
                                                                                    'interval_max': 100,}
                                       ) 
                self._logging.info("taskid: %s", res.id)
            except Exception as e:
                self._logging.error("Exception occur while doing: %s(arg=%s, queue=%s), except: %s  ",
                                                                  func, arg, q, e)
        #for q in queues:
        #    try:
        #        self._logging.info("push: %s.apply_async(args=%s, queue=%s)", func, arg, q)
        #        res = func.apply_async(args=arg, queue=q, expires=DOWNLOAD_TASK_EXPIRES_TIME, retry=True, retry_policy={
        #                                                                            'max_retries': 3,
        #                                                                            'interval_start': 5,
        #                                                                            'interval_step': 1,
        #                                                                            'interval_max': 100,}
        #                               ) 
        #        self._logging.info("taskid: %s", res.id)
        #    except Exception as e:
        #        self._logging.error("Exception occur while doing: %s(arg=%s, queue=%s), except: %s  ",
        #                                                          func, arg, q, e)

        #for q in queues:
            #print "queue: %s" % (q)
        #    tasks.append(func.subtask(args=arg, options={'queue':q}))
        #job = TaskSet(tasks)

        #try:
        #    res = job.apply_async()
            #results.append(res)
        #    print "taskid: %s" % (res.id)
        #except Exception, e:
        #    print e            


if __name__ == '__main__':
    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    log_FileHandler = logging.handlers.TimedRotatingFileHandler(filename = "log/eventprocessor.log",
                                                                when = 'midnight',
                                                                interval = 1,
                                                                backupCount = 7)
    
    log_FileHandler.setFormatter(formatter)
    log_FileHandler.setLevel(logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(log_FileHandler)

    app = celery.Celery('celeryfilesync',
                broker=broker,
                backend=backend,
                include=['celeryfilesync.tasks'])


    wm = pyinotify.WatchManager()  # Watch Manager
    # watched events
    mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_ISDIR | pyinotify.IN_MOVED_FROM | pyinotify.IN_MOVED_TO

    handler = EventHandler(app, wwwroot, httphostname)

    #log.setLevel(10)
    print "begin to monitor dir:", monitorpath
    logger.info("begin to monitor dir: %s", monitorpath)
    #notifier = pyinotify.ThreadedNotifier(wm, handler)
    #notifier.start()
    #wdd = wm.add_watch(monitorpath, mask, rec=True, auto_add=True)

    #wm.rm_watch(wdd.values())
    #notifier.stop()

    notifier = pyinotify.Notifier(wm, handler)
    wdd = wm.add_watch(monitorpath, mask, rec=True, auto_add=True)
    notifier.loop()
