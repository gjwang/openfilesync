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
from celeryfilesync.tasks import add, sendMsg, download, rmfile, mkemptydir, rmemptydir, fsrename

import pyinotify

import os
from os.path import walk, isfile, join, dirname, basename, normpath, splitext, getsize
import threading
from threading import Condition, Lock
from monitor import my_monitor

from config import monitorpath, wwwroot, httphostname, broker, backend, exclude_exts


timeout = 2000 / 1000.0
CHECK_ACTIVE_QUEUE_TIME = 10 #seconds


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
        self.inspecter = app.control.inspect(timeout=timeout)
        self.workers_online = {}
        self._condition = Condition(Lock()) 

        #self.checkaliveworker()
        self.activeQueueThread = self.CheckActivQueueThread(self.checkaliveworker)
        self.activeQueueThread.start()


    class CheckActivQueueThread(threading.Thread):
        def __init__(self, func):
            threading.Thread.__init__(self)
            self.stop = False
            self.func = func
        def run(self):
            while self.stop == False:
                #print "checkaliveworker"
                try:
                    self.func()
                except Exception as e:
                    print e  
                time.sleep(CHECK_ACTIVE_QUEUE_TIME)

        def stop(self):
            self.stop = True


    def checkaliveworker(self):
        self._tmp_workers = {}

        self.active_queues = self.inspecter.active_queues() or {}
        for worker in self.active_queues:
            #print worker
            queue0 = self.active_queues[worker][0]
            #print queue0['name']
            self._tmp_workers[worker] = queue0['name']

            #self.workers_online[worker] = queue0['name']

        tmp_set = set(self._tmp_workers.iteritems())
        worker_set = set(self.workers_online.iteritems())
        diffset = tmp_set ^ worker_set

        if len(diffset) != 0:
            self._condition.acquire()
            try:
                self.workers_online = copy.deepcopy(self._tmp_workers)
            finally:
                self._condition.release()

            diffset = tmp_set - worker_set
            if len(diffset):
                #print "workers %s new online" % (diffset)
                self._logging.info("workers %s new online", diffset)                

            diffset = worker_set - tmp_set
            if len(diffset):
                #print "workers %s offline" % (diffset)
                self._logging.info("workers %s offline", diffset)


            #print "workersonline: %s" % (self.workers_online)
            self._logging.info("workersonline: %s",self.workers_online)


        #print "workersonline: %s" % (self.workers_online)

    def process_IN_CREATE(self, event):
        #self.checkaliveworker()
        #print "event:", str(event)
        #self._logging.info("event: %s", str(event))
        #print "type event:", type(event)
        #print "event: %s" % pformat(event)

        if event.dir == True:
            relativepath = event.pathname[self.monpath_length:]
            self._logging.info("Creating folder: \'%s\', relativepath %s", event.pathname, relativepath)
            mkemptydir(relativepath = relativepath)
        else:
            #print "Creating file: %s. Ignore it" % (event.pathname)
            self._logging.info("Creating file: \'%s\'. Ignore this event", event.pathname)

        
    def process_IN_DELETE(self, event):
        #print "event:", str(event)
        #print "Removing:", event.pathname
        #self._logging.info("Removing: %s", event.pathname)

        url = self.mergeurl(event.pathname)
        #print url

        if event.dir == True:
            #print "remove folder:", event.pathname            
            self._logging.info("remove folder: %s", event.pathname)
            self.notifyworker(rmemptydir, (url, None))
        else:
            #print "remove file: %s" % (event.pathname)
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
        self.notifyworker(download, (url, getsize(event.pathname), None) )
            
 
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
        tasks = []
        self._condition.acquire()
        try:            
            queues = self.workers_online.values()
        finally:
            self._condition.release()
        
        for q in queues:
            try:
                self._logging.info("push: %s.apply_async(args=%s, queue=%s)", func, arg, q)
                res = func.apply_async(args=arg, queue=q, retry=True, retry_policy={
                                                                                    'max_retries': 3,
                                                                                    'interval_start': 5,
                                                                                    'interval_step': 1,
                                                                                    'interval_max': 100,}
                                       ) 
                #print "taskid: %s" % (res.id)
                self._logging.info("taskid: %s", res.id)
            except Exception as e:
                print e            
                self._logging.errno("Exception occur while doing: %s(arg=%s, queue=%s), except: %s  ",
                                                                  func, arg, q, e)
                

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


#monitorpath = '/var/www/test'
#wwwroot = '/var/www'
#httphostname = 'http://192.168.5.60'


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
