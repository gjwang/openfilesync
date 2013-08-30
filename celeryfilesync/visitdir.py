# -*- coding: utf-8 -*-
'''
Created on 2013-8-8

@author: gjwang
'''

import shutil
from distutils import dir_util
from pprint import pformat
import filecmp, sys, os
from os.path import walk, isfile, join, dirname, basename, normpath, splitext, getsize

def visitdir(topdir, wwwroot = None, exclude_exts = []):
    if wwwroot == None:
        wwwroot = topdir

    if wwwroot.endswith('/') or wwwroot.endswith('\\'):
        length = len(wwwroot)
    else:
        length = len(wwwroot) + 1
        
    list_dirs = os.walk(topdir)
    
    fileslist = []
    dirslist = []
    for root, dirs, files in list_dirs:
        #print "root: ", root
        if 'CVS' in dirs:
            dirs.remove('CVS')  # don't visit CVS directories
                     
        for d in dirs:
            #print d 
            #print os.path.join(root, d)
            dirslist += [os.path.join(root, d)[length:].replace('\\', '/')]
                  
        for f in files:
            if splitext(f)[1].lower() not in exclude_exts:
                #print f, os.path.join(root, f)[length:].replace('\\', '/')
                filename = os.path.join(root, f)
                #fileslist.append(filename[length:].replace('\\', '/'))
                fileslist.append( (filename[length:].replace('\\', '/'), getsize(filename)) )
        
    #print fileslist
    return dirslist, fileslist    

def download(url, topdir, localFileName):
    dstdir = dirname(os.path.join(topdir, localFileName))

    #print "down: ", os.path.join(dir1, f), dstdir

    if not os.path.exists(dstdir):
        os.makedirs(dstdir)

    print "down:", url + ' > ' + dstdir

    shutil.copy2(url, dstdir) 
    

monitorpath = '/var/www/test'
wwwroot = '/var/www'
dir1 = monitorpath
#dir1 = '/data/src'

dir2 = '/data/dst/test'
dirwwwroot = '/data/dst'

exclude_exts=['.pyc', '.bak', '.ddd', '.png', '.gif', '.jpg']

if __name__ == "__main__":
    #main() 
    dirslist1, fileslist1 = visitdir(monitorpath, wwwroot, exclude_exts)


    #print 'dir1', dirslist1, fileslist1

    dirslist2, fileslist2 = visitdir(dir2, dirwwwroot)

    #print 'dir2', dirslist2, fileslist2
    #print dirslist1, dirslist2
    
    #print
    downloaddirs = set(dirslist1) - set(dirslist2)

    #print downloaddirs

    downloadfiles = set(fileslist1) - set(fileslist2)
    rmdirs = set(dirslist2) - set(dirslist1)
    rmfiles = set(fileslist2) - set(fileslist1)
    print "downfiles set:", downloadfiles   
    print "rmfiles set:  ", rmfiles
    print "rmdirs set:   ", rmdirs
    
    for f in downloadfiles:
        download(os.path.join(wwwroot, f), dirwwwroot, localFileName = f)
    
    print

    for f in rmfiles:
        rmfile = os.path.join(dirwwwroot, f)
        if os.path.exists(rmfile):
            os.remove(rmfile)
            print "rm: ",rmfile 

    print
    for d in rmdirs:
        rmdir = os.path.join(dirwwwroot, d)
        print "rmdir: ",rmdir
        if os.path.isdir(rmdir):
            for root, dirs, files in os.walk(rmdir, topdown=False):
                #for name in files:
                    #os.remove(os.path.join(root, name))
                #    pass
                for name in dirs:
                    #print "rmsub:", os.path.join(root, name)
                    os.rmdir(os.path.join(root, name))
                
            #print "rm: ", rmdir
            os.rmdir(rmdir)
        #else:
            #print "not exist: ", rmdir, 
        
        #print

