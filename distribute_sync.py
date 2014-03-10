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
#from config import monitorpath, wwwroot, httphostname, broker, backend, exclude_exts
from config_distribute import monitorpath, wwwroot, httphostname, broker, backend, exclude_exts, hash_num, hash_config 

import hashlib
from urlparse import urlsplit

INSPECT_TIMEOUT = 10
CHECK_ACTIVE_QUEUE_TIME = 30 #seconds
MAX_OFFLINE_TIME =  60*30    #seconds
WHOLE_SYNC_TASK_EXPIRES_TIME = MAX_OFFLINE_TIME
DOWNLOAD_TASK_EXPIRES_TIME = 3600*24 

class EventHandler(pyinotify.ProcessEvent):
    def __init__(self, app, monitorpath = '/data/', hostname = 'http://127.0.0.1'):
        self._logging = logging.getLogger(self.__class__.__name__)
        self.app = app
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
        #dirslist, fileslist = visitdir(monitorpath, wwwroot, exclude_exts)
	now_time = int(time.time())

        fileslist = None
        workers_args = {}

        for worker, status in self.workers_status.items():
	    if now_time - status['last_whole_sync_time'] < WHOLE_SYNC_TASK_EXPIRES_TIME:
	        self._logging.info("worker:%s do whole_sync too frequent(%s seconds), skip this time",
				    worker, now_time - status['last_whole_sync_time'])	
		continue

            try:
            	if fileslist is None:
                    dirslist, fileslist = visitdir(monitorpath, wwwroot, exclude_exts)

                    for f_sz in fileslist:
                    	fl = f_sz[0]
                        if not fl.startswith('/'):
                            fl = join('/', fl)

                        hs_code = int(hashlib.md5(fl).hexdigest()[0:4], 16)%hash_num
                        workers = hash_config.get(hs_code) or []

                        self._logging.info('hash_code=%d, workers:%s, filepath=%s', hs_code, workers, fl)

                        for w in workers:
                            if workers_args.get(w) is None:
                                workers_args[w] = []
                            workers_args[w].append(f_sz)

                args = workers_args.get(worker)
                if args:
                    res = download_list.apply_async(args=([], workers_args[worker], httphostname),
                                          queue=status['queue'], expires=WHOLE_SYNC_TASK_EXPIRES_TIME, retry=False)
                    status['last_whole_sync_time'] = time_now
                    self._logging.info("cron job: worker=%s, whole_sync taskid: %s", worker, res.id)
                else:
                    self._logging.info("cron job: worker=%s, whole_sync is not in config_hash", worker)
            except Exception as e:
                self._logging.exception('all_workers_do_whole_sync exception: %s', e)	

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
        self.active_queues = self.inspecter.active_queues() or {}
	time_now = int(time.time())

	dirslist = None
        fileslist = None
        workers_args = {}
        for worker in self.active_queues:
            queue0 = self.active_queues[worker][0]
            status = self.workers_status.get(worker)
            if status is None:
		status = {}
                status['queue'] = queue0['name']
                status['last_whole_sync_time'] = 0
                self.workers_status[worker] = status

                self._logging.info("worker: %s, new online", worker)
                try:
                    if dirslist is None:
		        dirslist, fileslist = visitdir(monitorpath, wwwroot, exclude_exts)

                        for f_sz in fileslist:
			    try:
                                f = f_sz[0] 
                                if not f.startswith('/'):
                                    f = join('/', f)

                                hs_code = int(hashlib.md5(f).hexdigest()[0:4], 16)%hash_num
                                workers = hash_config.get(hs_code) or []

                                self._logging.info('hash_code=%d, workers:%s, filepath=%s', hs_code, workers, f)

                                for w in workers:
                                    if workers_args.get(w) is None:
                                        workers_args[w] = []
                                    workers_args[w].append(f_sz)
			    except Exception as e:
				self._logging.exception('hash exception: %s', e)

                    args = workers_args.get(worker)
		    if args:
		        res = download_list.apply_async(args=([], workers_args[worker], httphostname),
		    			     queue=status['queue'], expires=WHOLE_SYNC_TASK_EXPIRES_TIME, retry=False)
                        status['last_whole_sync_time'] = time_now
                        self._logging.info("worker: %s, new online, whole_sync taskid: %s", worker, res.id)
		    else:
                        self._logging.error("whole_sync: worker=%s is not in hash_config", worker)

                except Exception as e:
                    self._logging.exception('do whole_sync exception: %s', e)

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
        if event.dir == True:
            relativepath = event.pathname[self.monpath_length:]
            self._logging.info("Creating folder: \'%s\', relativepath %s", event.pathname, relativepath)
            mkemptydir(relativepath = relativepath)
        else:
            self._logging.info("Creating file: \'%s\'. Ignore this event", event.pathname)

        
    def process_IN_DELETE(self, event):
        url = self.mergeurl(event.pathname)

        if event.dir == True:
            pass
        else:
            if splitext(event.pathname)[1].lower() in exclude_exts:
                logger.info("exclude file: %s, skip", event.pathname)
                return

            self._logging.info("remove file: %s", event.pathname)
            self.distrib_worker(url, rmfile, (url, None))

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
            #self.notifyworker(download, (url, filesz, None) )
            self.distrib_worker(url, download, (url, filesz, None) )
	except Exception as exc:
	    self._logging.error("%s", exc)
            
 
    def process_IN_ISDIR(self, event):
        pass

    def process_IN_MOVED_FROM(self, event):
        self._logging.info("movefrom: %s", event.pathname)

    def process_IN_MOVED_TO(self, event):
        self._logging.info("moveto: %s", event.pathname)

    def mergeurl(self, pathname):
        url = os.path.join(self.hostname, pathname[self.monpath_length:].replace('\\', '/'))
        return url

    def hash_code(self, url):
        path = urlsplit(url).path
        return int(hashlib.md5(path).hexdigest()[0:4], 16)%hash_num

    def distrib_worker(self, url, func, args = ()):
        hs_code = self.hash_code(url)
        self._logging.info('hash_code=%d, url=%s', hs_code, url)

        workers = hash_config.get(hs_code) or []
        for worker in workers:
            status = self.workers_status.get(worker)
            if status:
                self.async_notifywoker(q=status['queue'], func=func, args=args)
            else:
                self._logging.info("worker:%s not in workersonline list", worker)

    def async_notifywoker(self, q, func, args = ()):
        try:
            self._logging.info("push: %s.apply_async(args=%s, queue=%s)", func, arg, q)
            res = func.apply_async(args=args, queue=q, expires=DOWNLOAD_TASK_EXPIRES_TIME, retry=True,
                                                                     retry_policy={
                                                                                    'max_retries': 3,
                                                                                    'interval_start': 5,
                                                                                    'interval_step': 1,
                                                                                    'interval_max': 100,}
                                  )
            self._logging.info("taskid: %s", res.id)
        except Exception as e:
            self._logging.errno("async_notifywoker(queue=%s, func=%s), exception: %s  ", q, func, e)

    def notifyworker(self, func, arg = ()):
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
                self._logging.errno("Exception occur while doing: %s(arg=%s, queue=%s), except: %s  ",
                                                                  func, arg, q, e)
def check_runtime_env():
    if hash_num != len(hash_config):
        print 'hash_num=%s not equals len(hash_config)=%s. exited!!!!'%(hash_num, len(hash_config))
        exit(0)

if __name__ == '__main__':
    check_runtime_env()

    formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
    log_FileHandler = logging.handlers.TimedRotatingFileHandler(filename = "log/distribute_sync_client.log",
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
