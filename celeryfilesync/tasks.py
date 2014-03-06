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

from config_worker import dstwwwroot, dstmonitorpath, dstdir, MAX_RETRY_TIMES, worker_broker, worker_backend
from config_worker import special_exts, exclude_exts

from utils.threadpool import ThreadPool

data_dir = './log'
try:
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
except Exception, exc:
    print str(exc)
formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
log_FileHandler = logging.handlers.TimedRotatingFileHandler(filename = "log/worker.log",
                                                            when = 'midnight',
                                                            interval = 1,
                                                            backupCount = 7)
    
log_FileHandler.setFormatter(formatter)
log_FileHandler.setLevel(logging.INFO)
#logger = logging.getLogger(__name__)
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(log_FileHandler)


celery = Celery('celeryfilesync',
                broker=worker_broker,
                backend=worker_backend,
                include=['celeryfilesync.tasks'])

@task(default_retry_delay=10, max_retries=MAX_RETRY_TIMES)
def download(url, filesize = None, localFileName = None):
    try:
        global dstdir

        if localFileName != None:
            localName = localFileName
        else:
            localName = join(dstdir, urlsplit(url).path[1:])

        if splitext(localName)[1].lower() in exclude_exts:
            logger.info("exclude file: %s, skip", localName) 
	    return True 

        req = urllib2.Request(url)
        r = urllib2.urlopen(req)
        file_len = int(r.headers["Content-Length"])

        if filesize is not None and filesize != file_len:
            logger.error("filesize(%s) != file_len(%s): %s", filesize, file_len, url)

        filesize = file_len

        if os.path.exists(localName) and getsize(localName) == filesize and \
	   splitext(localName)[1].lower() not in special_exts:
            logger.info("File \'%s\' existed and filesize(%s) equals, skip", url, filesize)
            r.close()
	    return True

        dstdirname = dirname(localName)
        if not os.path.exists(dstdirname):
            os.makedirs(dstdirname)
            
        block_sz = 8192*2
        with open(localName, 'wb') as f:
            while True:
                buffer = r.read(block_sz)
                if not buffer:
                    break
                f.write(buffer)

        r.close()
        sz = getsize(localName)
        if sz != filesize:
            logger.error("download %s unfinished: filesz:%s != localfilesz:%s" % (url, filesize, sz))
            raise Exception("download %s unfinished: filesz:%s != localfilesz:%s" % (url, filesize, sz))
        logger.info("down: %s to %s, filesize=%s, OK", url, localName, sz)

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

    if splitext(localname)[1].lower() in exclude_exts:
        logger.info("exclude file: %s, skip", localname)
        return True

    if os.path.exists(localname):
        try:
            os.remove(localname)
            logger.info("rmfile: %s ok", localname)

            #rm empty dir, avoid empty 'holes'
            try:
                os.removedirs(dirname(localname))
            except OSError as ex:
                if ex.errno == errno.ENOTEMPTY:
                    pass
                else:
                    logger.error('rm dir failed: %s', dirname(localname))
                    return 'rm dir failed: %s' % dirname(localname)

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
    #rmdirs = set(dstdirs) - set(srcdirs)
    rmfiles = set(dstfiles) - set(srcfiles)

    logger.warn("rmfiles count=%s", len(rmfiles))
    for f, sz in rmfiles:
        file = join(dstdir, f)
        localfilesz = None
	if os.path.exists(file): 
	    localfilesz = getsize(file)

        is_skip = False
	for dlf, dlsz in downloadfiles:
	    if f == dlf:
                logger.info('file:%s is in download list, suggest_size(%s) =? scan_size(%s), real_size:%s, skip',
				 f, dlsz, sz, localfilesz)
		is_skip = True
                break 

        if not is_skip:
            logger.info('Going to rm file: %s, scan_filesize(%s) =? local_filesize(%s)', file, sz, localfilesz)
            rmfile(None, file)

    #logger.warn("rmdirs count=%s", len(rmdirs))
    #for d in rmdirs:
    #    dir = join(dstdir, d)
    #    logger.info('Going to rm dir: %s', dir)
    #    rmemptydir(None, dir)

    failed_downloads = []
    
    #for (f, filesize) in downloadfiles:
        #url = join(hostname, f)
        #try:
        #    download(url, filesize, join(dstdir, f))
        #except Exception, exc:
        #    logger.error("sync %s failed: %s", url, exc)
        #    failed_downloads.append((f, filesize))
            #current.retry(exc=exc)

    download_count = len(downloadfiles)
    logger.warn("download tasks count = %s", download_count)

    if download_count > 0:
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
    logger.info("This is run every Monday morning at 7:30")
    
if __name__ == "__main__":
    download("http://www.blog.pythonlibrary.org/wp-content/uploads/2012/06/wxDbViewer.zip")    

