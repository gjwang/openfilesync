# -*- coding: utf-8 -*- 
#

import time
import logging
#import traceback
import Queue, threading
#from threading import Condition, Lock
from threading import Thread
from config_worker import HANDLER_THREAD_COUNT, TASK_QUEUE_MAX_SIZE

MAX_RETRIES_FAILED_TASK = 4

class _Worker(Thread):
    '''
    worker thread which get task from queue to execute
    '''      

    def __init__(self, threadname, workQueue, parent):
        threading.Thread.__init__(self, name=threadname)
        self.__logger = logging.getLogger(threadname)
        self.__parent = parent
        self.__workQueue = workQueue

        self.stop = False
        
    def run(self):
        while not self.stop:
            try:
                callback = self.__workQueue.get()
                task = callback[0]
                param = callback[1]
                if task is None:
                    continue

                try:
                    if param is not None:
                        task(param)
                    else:
                        task()
                except Exception as processEx:
                    self.__parent.add_failed_task(callback)

                    self.__logger.error("%s execute callback: %r failed due to %s", self.name, callback, str(processEx))
                finally:
                    self.__parent.complete_task()
            except IOError:
                pass
            except Exception as getEx:
                self.__logger.error("%s get task from queue failed: %s", self.name, getEx)
        

class _procfailed_Worker(Thread):
    '''
    worker thread which to process failed tasks
    '''      

    def __init__(self, threadname, parent):
        threading.Thread.__init__(self, name=threadname)
        self.__logger = logging.getLogger(threadname)
        self.__parent = parent
        self.stop = False
        
    def run(self):
        while not self.stop:
            try:
                self.__parent.process_failed_task()
            except Exception as getEx:
                self.__logger.error("%s process_failed_task: %s", self.name, getEx)
            
            time.sleep(1)


class _ThreadPool(object):
    
    @classmethod
    def instance(cls):
        if not hasattr(cls, "_instance"):
            cls._instance = cls()
            cls.__logger = logging.getLogger("thread_pool")
        return cls._instance
        
    def initialize(self, workerCount=HANDLER_THREAD_COUNT):
        self.__workQueue = Queue.Queue(maxsize=TASK_QUEUE_MAX_SIZE)
        self._unfinished_tasks = 0
        self._condition = Condition(Lock())        
        self.__workerCount = workerCount
        self.__workers = []
        for i in range(self.__workerCount):
            worker = _Worker("_Worker-" + str(i + 1), self.__workQueue, self)
            worker.start()
            self.__workers.append(worker)

        self.__failed_dict = {}
        self.__failed_condition = Condition(Lock())
        self.max_retries = MAX_RETRIES_FAILED_TASK
        self.failed_worker = _procfailed_Worker("procfailed_Worker", self)
        self.failed_worker.start()

    def stop(self):
        ''' Wait for each of them to terminate'''
        while self.__workers:
            worker = self.__workers.pop()
            worker.stop = True
            self.__workQueue.put(None)

        self.failed_worker.stop = True
            
    def add_task(self, callback):
        if callback is not None:
            self._condition.acquire()
            self._unfinished_tasks += 1
            self._condition.release()
            self.__workQueue.put((callback, None))
        
    def add_task_with_param(self, callback, param):
        if callback is not None:
            self._condition.acquire()
            self._unfinished_tasks += 1
            self._condition.release()
            self.__workQueue.put((callback, param))

    def add_failed_task(self, task):
        self.__failed_condition.acquire()
        try:
            self.__failed_dict[task] = 1
        finally:
            self.__failed_condition.release()


    def process_failed_task(self):
        self.__failed_condition.acquire()
        try:
            task_items = self.__failed_dict.items()
        finally:
            #because callback(param)  may take some time, so we release the lock
            self.__failed_condition.release()

        for task, retries in task_items:         
            retries += 1
            task_success = False

            if retries in xrange(2, self.max_retries*2 + 1, 2):
                try:
                    callback = task[0]
                    param = task[1]
                    callback(param)                            
                except Exception, exc:
                    self.__logger.error("retried %s failed: %s, retries=%s", task, exc, retries/2)
                else:
                    task_success = True

            self.__failed_condition.acquire()
            try:
                if task_success or retries >= self.max_retries*2:
                    del self.__failed_dict[task]
                    self.__failed_condition.notify_all()
                else:
                    self.__failed_dict[task] = retries                

            finally:
                self.__failed_condition.release()
        

    def get_failed_list(self):
        return self.__failed_dict
                
    def complete_task(self):
        self._condition.acquire()
        try:
            unfinished = self._unfinished_tasks - 1
            if unfinished <= 0:
                if unfinished < 0:
                    self.__logger.error('task_done() called too many times')
                self._condition.notify_all()
            self._unfinished_tasks = unfinished
            self.__logger.info('Thread pool has %d unfinished tasks', self._unfinished_tasks)
        finally:
            self._condition.release()
        
    def clear_task(self):
        self._condition.acquire()        
        self._unfinished_tasks = 0
        while not self.__workQueue.empty():
            try:
                task = self.__workQueue.get_nowait()
                self.__logger.error('Thread pool has unfinished task:%s', str(task))
            except Exception as ex:
                self.__logger.error("%s clear_task: %r failed due to %s", self.name, str(ex))
        self._condition.release()

        self.__failed_condition.acquire()
        try:
            self.__failed_dict.clear()
        except Exception as ex:
            self.__logger.error("%s clear failed_dict tasks: %r failed due to %s", self.name, str(ex))
        finally:
            self.__failed_condition.release()
                
    def wait_for_complete(self, timeout=None):
        start = time.time()
        self._condition.acquire()
        try:
            while self._unfinished_tasks:
                #releases the underlying lock, then blocks until it is awakened by a notify() or notifyAll()
                #or timeout
                self._condition.wait(600)
                if timeout is not None:
                    current = time.time()
                    if (current - start) > timeout:
                        self.__logger.error('Thread pool timeout')
                        break
        finally:
            self._condition.release()

        self.__failed_condition.acquire()
        try:
            while len(self.__failed_dict):
                self.__failed_condition.wait(10)
                if timeout is not None:
                    current = time.time()
                    if (current - start) > timeout:
                        self.__logger.error('process failed tasks timeout')
                        break
        finally:
            self.__failed_condition.release()

        
ThreadPool = _ThreadPool.instance()
