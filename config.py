#source/client configue
wwwroot = '/data/www'
monitorpath = wwwroot
httphostname = 'http://192.168.5.60'

exclude_exts=['.swx', '.swp', '.txt~', '.tmp', '.svn']

broker = "redis://192.168.5.60:6379//"
backend = "redis://192.168.5.60:6379//"



#dest/worker configure
worker_broker = "redis://192.168.5.60:6379//"
worker_backend = "redis://192.168.5.60:6379//"

dstdir = '/home/wgj/backup'
dstwwwroot = dstdir
dstmonitorpath = dstwwwroot

#file in special_exts download it anyway
special_exts=['.m3u8']

#thread num while doing whole sync
HANDLER_THREAD_COUNT = 8
TASK_QUEUE_MAX_SIZE = 30240

#use 2**retry_times backoff methon, don't retry too much times unless you kwow what you are doing
MAX_RETRY_TIMES = 7
