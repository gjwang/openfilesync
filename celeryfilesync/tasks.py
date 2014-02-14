'''
Created on 2013-7-31

@author: gjwang
'''

import logging
from logging import getLogger
import time
import urllib
import urllib2

from celery import task
from celery.task.schedules import crontab
from celery.decorators import periodic_task
from celery.task import current
from celery import Celery
#from celery.schedules import crontab
#from celery.task import periodic_task

import os
from urlparse import urlsplit
from os.path import join, dirname, basename, normpath, splitext, getsize
from os import errno

from visitdir import visitdir

from config import dstwwwroot, dstmonitorpath, dstdir, MAX_RETRY_TIMES, worker_broker, worker_backend, special_exts

from utils.threadpool import ThreadPool

#formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
#log_FileHandler = logging.handlers.TimedRotatingFileHandler(filename = "log/worker_filesync.log",
#                                                            when = 'midnight',
#                                                            interval = 1,
#                                                            backupCount = 7)
    
#log_FileHandler.setFormatter(formatter)
#log_FileHandler.setLevel(logging.INFO)
logger = logging.getLogger(__name__)
#logger.setLevel(logging.INFO)
#logger.addHandler(log_FileHandler)


celery = Celery('celeryfilesync',
                broker=worker_broker,
                backend=worker_backend,
                include=['celeryfilesync.tasks'])

@task
def hello():
    return 'hello world'

def mul(x, y):
    return x * y

@task()
def add(x, y):
    return x + y

@task
def sendMsg(msg):
    return msg

@task(default_retry_delay=10, max_retries=MAX_RETRY_TIMES)
def download(url, filesize = None, localFileName = None):
    try:
        global dstdir
        if localFileName != None:
            localName = localFileName
        else:
            localName = join(dstdir, urlsplit(url).path[1:])

        req = None

        if filesize is None:
            req = urllib2.Request(url)
            r = urllib2.urlopen(req)
            filesize = r.headers["Content-Length"]

        filesize = int(filesize)

        if os.path.exists(localName) and getsize(localName) == filesize \
           and splitext(localName)[1].lower() not in special_exts:
            logger.info("File \'%s\' existed and filesize(%s) equals", url, filesize)
        else:
            dstdirname = dirname(localName)
            if not os.path.exists(dstdirname):
                os.makedirs(dstdirname)

            if req is None:
                req = urllib2.Request(url)
                r = urllib2.urlopen(req)
            
            block_sz = 8192*2
            f= open(localName, 'wb')
            #with open(localName, 'wb') as f:
            if 1:
                while True:
                    buffer = r.read(block_sz)
                    if not buffer:
                        break
                    f.write(buffer)
            f.close()

            sz = getsize(localName)
            if sz != filesize:
                logger.error("download %s unfinished: filesz:%s != localfilesz:%s" % (url, filesize, sz))
                raise Exception("download %s unfinished: filesz:%s != localfilesz:%s" % (url, filesize, sz))
            logger.info("down: %s to %s OK", url, localName)
        return True 
    except Exception, exc:
        logger.info("down: %s to %s failed: %s", url, localName, exc)
        if isinstance(exc, urllib2.HTTPError) and exc.code == 404:
            return True
        current.retry(exc=exc, countdown=min(2 ** current.request.retries, 360))

#@task
def rmdir(url, localfilename = None):
    if localfilename:
        localname = localfilename
    else:
        localname = join(dstdir, urlsplit(url).path[1:])

    if os.path.isdir(localname):
        #rm sub dirs/files first
        for root, dirs, files in os.walk(localname, topdown=False):
            for name in files:
                os.remove(join(root, name))

            for name in dirs:
                os.rmdir(join(root, name))
                
        logger.info("rmdir: %s", localname)
        os.rmdir(localname)
        return 'OK'
    elif os.path.exists(localname):
        return '%s is not a dir' % (localname)
    else:
        return '%s dir not exists' % (localname)

@task
def mkemptydir(relativepath, topdir = dstdir):
    try:
        dstdirname = join(topdir, relativepath)
        logger.info("makedir %s", dstdirname)
        if not os.path.exists(dstdirname):
            os.makedirs(dstdirname)
        return 'OK'
    except Exception, exc:
        logger.error("makedir %s failed: %s ", dstdirname, exc)
        return "makedir %s failed: %s ", dstdirname, exc
    
@task
def rmemptydir(url, localfilename = None):
    if localfilename:
        localname = localfilename
    else:
        localname = join(dstdir, urlsplit(url).path[1:])

    if os.path.isdir(localname):
        #rm empty subdir first
        for root, dirs, files in os.walk(localname, topdown=False):
            for name in dirs:
                try:
                    os.rmdir(join(root, name))
                except OSError as ex:
                    if ex.errno == errno.ENOTEMPTY:
                        logger.error('dir \'%s\' not empty', join(root, name) )
                    else:
                        logger.error("rmdir exception: %s", ex)

        try:
            os.rmdir(localname)
            logger.info("rmemptydir: %s OK", localname)
            return 'OK'
        except OSError as ex:
            if ex.errno == errno.ENOTEMPTY:
                logger.error('dir \'%s\' not empty', localname)
            else:
                logger.error("rmdir exception: %s", ex)

            return ex

    elif os.path.exists(localname):
        logger.error('rmemptydir: %s not a dir', localname)
        return '%s not a dir' % (localname)
    else:
        logger.info('rmemptydir: %s dir not exists', localname)
        return '%s dir not exists' % (localname)


@task
def rmfile(url, localfilename = None):
    if localfilename:
        localname = localfilename
    else:
        localname = join(dstdir, urlsplit(url).path[1:])

    if os.path.exists(localname):
        try:
            os.remove(localname)
            logger.info("rmfile: %s ok", localname)
            return 'OK'            
        except OSError as ex:
            logger.error("rmfile: %s failed: %s", localname, ex)
            return ex
    else:
        logger.info('rmfile: %s file not exists', localname)
        return '%s file not exists' % (localname)

#filesync rename
@task
def fsrename(src, dst):
    try:
        os.rename(join(dstdir, src), join(dstdir, dst))
    except OSError as ex:
        if ex.errno == errno.ENOENT:
            #No such file or directory
            logger.error('no %s', src)
            #download()
        else:
            logger.error("name %s to %s exception: %s", join(dstdir, src), join(dstdir, dst), ex)

        raise ex
        
    return "OK"


@task(default_retry_delay=10, max_retries=0)
#@periodic_task(run_every=crontab(hour='*', minute='*/1', day_of_week='*'))
def download_list(srcdirs = [], srcfiles = [], hostname = 'http://127.0.0.1'):    
    dstdirs, dstfiles = visitdir(dstmonitorpath, dstwwwroot)

    downloadfiles = set(srcfiles) - set(dstfiles)
    rmdirs = set(dstdirs) - set(srcdirs)
    rmfiles = set(dstfiles) - set(srcfiles)

    logger.warn("rmfiles count=%s", len(rmfiles))
    for f, _ in rmfiles:
        file = join(dstdir, f)
        logger.info('Going to rm file: %s', file)
        rmfile(None, file)

    logger.warn("rmdirs count=%s", len(rmdirs))
    for d in rmdirs:
        dir = join(dstdir, d)
        logger.info('Going to rm dir: %s', dir)
        rmemptydir(None, dir)

    logger.warn("downfiles set count=%s", len(downloadfiles))

    failed_downloads = []
    
    #fake urls, for test failed downloads
    #downloadfiles.add(( 'http://www.example.com/songs/mp3.mp3', 0) )
    #downloadfiles.add(( 'http://www.example.com/songs/mp3_2.mp3', 0) )

    #for (f, filesize) in downloadfiles:
        #url = join(hostname, f)
        #try:
        #    download(url, filesize, join(dstdir, f))
        #except Exception, exc:
        #    logger.error("sync %s failed: %s", url, exc)
        #    failed_downloads.append((f, filesize))
            #current.retry(exc=exc)

    logger.warn("download tasks count = %s", len(downloadfiles))

    ThreadPool.initialize()
    for (f, filesize) in downloadfiles:
        url = join(hostname, f)
        ThreadPool.add_task_with_param(download, url)
    ThreadPool.wait_for_complete(timeout=3600*10)

    logger.warn("complete downloads")
    
    ThreadPool.clear_task()
    ThreadPool.stop()

    return 'OK'


@periodic_task(run_every=crontab(hour='*', minute='*/1'))
def every_monday_morning():
    print("This is run every Monday morning at 7:30")
    
if __name__ == "__main__":
    download("http://www.blog.pythonlibrary.org/wp-content/uploads/2012/06/wxDbViewer.zip")    

