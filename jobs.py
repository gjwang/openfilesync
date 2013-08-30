'''
Created on 2013-7-23

@author: gjwang
'''
import time
import celery
from celery import current_app
from celery.task.sets import TaskSet
from celery import group
from celeryfilesync.tasks import add, sendMsg, download, download_list
from kombu.common import Broadcast

import pyinotify
from eventprocessor import EventHandler

from pprint import pformat

from celery.utils.compat import OrderedDict

from celeryfilesync.visitdir import visitdir
from monitor import my_monitor
import threading

#workers = OrderedDict()
print "create celery"


#app = celery.Celery(broker='redis://')

print "create OK"
#app = current_app
timeout = 2000 / 1000.0
#i = app.control.inspect(timeout=timeout)
#print "app inspect"
#ping = app.control.ping()
#print "ping", ping
workers = {}
 
#ret = app.control.broadcast('ping',reply=True)
#print "ret", ret

#print "broadcast"

try_interval = 1
def monitor():     
    try:
        try_interval *= 2
        #print('Inspecting workers...')
        #stats = i.stats()
        #print('Stats: %s' % pformat(stats))
        #registered = i.registered()
        #print('Registered: %s' % pformat(registered))
        #scheduled = i.scheduled()
        #print('Scheduled: %s' % pformat(scheduled))
        #active = i.active()
        #print('Active: %s' % pformat(active))
        #reserved = i.reserved()
        #print('Reserved: %s' % pformat(reserved))
        #revoked = i.revoked()
        #print('Revoked: %s' % pformat(revoked))
        #ping = i.ping()
        #print('Ping: %s' % pformat(ping))
        active_queues = i.active_queues()
        #print('Active queues: %s' % pformat(active_queues))
        
        for worker in active_queues:
            #print worker
            queue0 = active_queues[worker][0]
            #print queue0['name']
            workers[worker] = queue0['name']
            
        print workers
        #print
        
        #for worker in workers:
        #    print worker
            #print workers[worker]
        
        # Inspect.conf was introduced in Celery 3.1
        #conf = hasattr(i, 'conf') and i.conf()
        #print('Conf: %s' % pformat(conf))
    except (KeyboardInterrupt, SystemExit):
        import thread
        thread.interrupt_main()
    except Exception as e:
        print("Failed to inspect workers: '%s', trying "
                              "again in %s seconds" % (e, try_interval))
        print(e)
        
        time.sleep(try_interval)        

def main_task():    
    workers = {}
    if True:
        #tStart = time.time()
        active_queues = inspecter.active_queues() or {}
        #tEnd = time.time()
        #print tEnd - tStart

        #print('Active queues: %s' % pformat(active_queues))                
        for worker in active_queues:
            #print worker
            queue0 = active_queues[worker][0]
            #print queue0['name']
            workers[worker] = queue0['name']
            
        print workers

    results = []
    url = 'http://192.168.5.60/cctv/tencent.mp4'
    url2 = 'http://www.blog.pythonlibrary.org/wp-content/uploads/2012/06/wxDbViewer.zip'        

    tasks=[]

    for item in workers.iteritems():
        worker = item[0]
        queue = item[1]
        
        tasks.append(add.subtask(args=(3, 3), options={'queue':queue}))
        tasks.append(download.subtask(args=(url, None), options={'queue':queue}))
        tasks.append(download.subtask(args=(url2, None), options={'queue':queue}))
    
        #for i in range(10):
            #tasks.append(add.subtask(args=(i, i),options={'queue':queue}))
            #tasks.append(download.subtask(args=(url, None), options={'queue':queue}))
            #tasks.append(download.subtask(args=(url2, None), options={'queue':queue}))
        job = TaskSet(tasks)

        results.append(job.apply_async())

    print    
    print "download result:"
    
    for res in results:
        try:
            print res.id
        except Exception, e:
            print e
        
    #job = TaskSet(tasks=[add.subtask(args=(i, i),options={'queue':'queue1'}) for i in range(10)])
    #result1 = job.apply_async()
    
    #job = TaskSet(tasks=[add.subtask(args=(i, i),options={'queue':'queue2'}) for i in range(10)])
    #result2 = job.apply_async()
    #print result1.get()
    #print result2.get()


class pyinotifierThread(threading.Thread):
    def __init__(self, notifier):
        threading.Thread.__init__(self)
        self.notifier = notifier
        self.stop = False
    def run(self):
        while self.stop == False:
            self.notifier.loop()

    #def stop(self):
    #    self.stop = True
    #    self.notifier.stop()


class monitorThread(threading.Thread):
    def __init__(self, celery_app):
        threading.Thread.__init__(self)
        self.app = celery_app
        self.stop = False
    def run(self):
        while self.stop == False:
            print "my_monitor"
            my_monitor(app)

    def stop(self):
        self.stop = True
        self.notifier.stop()


monitorpath = '/var/www/test'
wwwroot = '/var/www'
hostname = 'http://192.168.5.60'

exclude_exts=['.pyc', '.bak', '.ddd', '.png', '.gif', '.jpg']


if __name__ == '__main__':
    app = celery.Celery('celeryfilesync',
                        broker='redis://',
                        backend='redis://',
                        include=['celeryfilesync.tasks'])

    #mt = monitorThread(app)
    #mt.start()

    inspecter = app.control.inspect(timeout=timeout)

    wm = pyinotify.WatchManager()  # Watch Manager
    # watched events
    mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_ISDIR | pyinotify.IN_MOVED_FROM | pyinotify.IN_MOVED_TO

    handler = EventHandler(app, wwwroot, hostname)

    
    print "Do a whole sync at startup..."
    dirslist1, fileslist1 = visitdir(monitorpath, wwwroot, exclude_exts)
    #print dirslist1, fileslist1
    #handler.notifyworker(download_list, (dirslist1, fileslist1, hostname) )

    print "download_list"
    #CELERY_QUEUES = (Broadcast('broadcast_tasks'), )
    #CELERY_ROUTES = {'download_list': {'queue': 'broadcast_tasks'}}

    download_list.apply_async(args=(dirslist1, fileslist1, hostname), queue='broadcast_tasks')
    tasks = []
    results = []
    #Broadcast('broadcast_tasks')
    #tasks.append(download_list.subtask(args=(dirslist1, fileslist1), options={'queue':'broadcast_tasks'}))
    #job = TaskSet(tasks)            
    #results.append(job.apply_async())



    #log.setLevel(10)
    #print "begin to monitor dir:", monitorpath
    #notifier = pyinotify.ThreadedNotifier(wm, handler)
    #wdd = wm.add_watch(monitorpath, mask, rec=True, auto_add=True)
    #notifier.start()

    #wm.rm_watch(wdd.values())
    #notifier.stop()
    
    notifier = pyinotify.Notifier(wm, handler)
    wdd = wm.add_watch(monitorpath, mask, rec=True, auto_add=True)
    #notifier.loop()

    pyinotifer = pyinotifierThread(notifier)
    pyinotifer.start()

    #print "monitor events real-time..."
    #my_monitor(app)

