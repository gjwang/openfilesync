'''
Created on 2013-8-6

@author: Administrator
'''
from pprint import pformat

from celery.events.snapshot import Polaroid

class DumpCam(Polaroid):

    def on_shutter(self, state):
        if not state.event_count:
            # No new events since last snapshot.
            return
        print('Workers: %s' % (pformat(state.workers, indent=4), ))
        print('Tasks: %s' % (pformat(state.tasks, indent=4), ))
        print('Total: %s events, %s tasks' % (
            state.event_count, state.task_count))