'''
Created on 2013-8-6

@author: gjwang
'''
import pyinotify

class EventHandler(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        print "Creating:", event.pathname
        print "event:", str(event)

    def process_IN_DELETE(self, event):
        print "Removing:", event.pathname

    def process_IN_CLOSE_WRITE(self, event):
        print "Closewrite:", event.pathname

    def process_IN_ISDIR(self, event):
        print "isdir:", event.pathname

if __name__ == '__main__':
    wm = pyinotify.WatchManager()  # Watch Manager
    mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_ISDIR 

    handler = EventHandler()
    notifier = pyinotify.Notifier(wm, handler)
    wdd = wm.add_watch('/data/src', mask, rec=True, auto_add=True)

    notifier.loop()
