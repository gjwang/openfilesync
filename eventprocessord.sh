#!/bin/sh

# chkconfig: 2345 08 92
# description: Automatically startup tv service.

startup()
{
	#cd /home/wgj/celery_proj/openfilesync || exit 0
	python eventprocessor.py &	
}
shutdown()
{
	ps -ef|grep "python eventprocessor.py"|grep -v grep|awk '{print $2}'|xargs kill
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
                startup
                ;;

esac
exit