#!/bin/sh

# chkconfig: 2345 08 92
# description: Automatically startup tv service.

startup()
{
	python distribute_sync.py &	
}
shutdown()
{
	ps -ef|grep "python distribute_sync.py"|grep -v grep|awk '{print $2}'|xargs kill
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

        *)
                echo "usage: start | stop | restart"
                exit 1
                ;;

esac
exit

