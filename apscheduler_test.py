from apscheduler.scheduler import Scheduler  
import time  
  
# Start the scheduler  
sched = Scheduler()  
  
  
def job_function():  
    print "Hello World"  
  
print 'start to sleep'  
print 'wake'  
sched.daemonic = False  
sched.add_cron_job(job_function,day_of_week='mon-fri', hour='*', minute='0-59',second='*/5')  
sched.start()  
print "end"
