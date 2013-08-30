'''
Created on 2013-8-6

@author: Administrator
'''
from celery import Celery
#from celeryfilesync import DumpCam
from pprint import pformat
from celery.events.snapshot import Polaroid

class DumpCam(Polaroid):

    def on_shutter(self, state):
        if not state.event_count:
            #print' No new events since last snapshot.'
            return
        print('Workers: %s' % (pformat(state.workers, indent=4), ))
        for worker in state.workers:
            print worker
        #print state.workers
        print('Tasks: %s' % (pformat(state.tasks, indent=4), ))
        print('Total: %s events, %s tasks' % (
            state.event_count, state.task_count))


def main(app, freq=1.0):
    state = app.events.State()
    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={'*': state.event})
        with DumpCam(state, freq=freq):
            recv.capture(limit=None, timeout=None)

if __name__ == '__main__':
    celery = Celery(broker='redis://')
    main(celery, 5)
