#!/bin/sh -e
# chkconfig: 2345 08 92
# description: Automatically startup worker service.

workername="worker6.example.com"
workqueue=$workername
broker="redis://localhost:6379//"
logfile="log/worker.log"

startup()
{
	#cd /home/wgj/celery_proj/openfilesync || exit 0        
	celery --app=celeryfilesync.tasks worker -n ${workername} -Q ${workqueue} --concurrency=2  --broker=${broker} --loglevel=warn --logfile=${logfile} &	
}

shutdown()
{
	ps -ef|grep "celery --app=celeryfilesync.tasks worker -n $workername -Q $workqueue"|grep -v grep|awk '{print $2}'|xargs kill
}

case "$1" in
        start)
                startup
                ;;
        stop)
                shutdown
                ;;
        restart)
                shutdown
                sleep 1
                startup
                ;;

        *)
                echo "usage: start | stop | restart"
                exit 1
                ;;
esac
exit
