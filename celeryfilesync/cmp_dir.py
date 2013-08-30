'''
Created on 2013-8-7

@author: Administrator
'''
# -*- coding: utf-8 -*- 
import os 
import sys
import shutil

from os.path import walk, join, normpath 

def Test1(rootDir): 
    list_dirs = os.walk(rootDir) 
    for root, dirs, files in list_dirs: 
        for d in dirs: 
            print os.path.join(root, d)      
        for f in files: 
            print os.path.join(root, f) 

def Test2(rootDir): 
    for lists in os.listdir(rootDir): 
        path = os.path.join(rootDir, lists) 
        print path 
        if os.path.isdir(path): 
            Test2(path)   



PathA = "E:\\FullData\\"
PathB = "E:\\IncomplData\\"

PathC = "E:\\DiffData\\"

def visit(arg, dirname, names):
    print dirname
   
    dir=dirname.replace(PathA,"")
   
    dirnameB = os.path.join(PathB,dir)
    dirnameC = os.path.join(PathC,dir)

    print "%s, %s, %s"%(dir, dirnameB, dirnameC)
   
    if os.path.isdir(dirnameB):
        for file in names:
            if os.path.isfile(os.path.join(dirname,file)) and not os.path.isfile(os.path.join(dirnameB,file)):
                if not os.path.isdir(dirnameC):
                    os.system("mkdir %s"%(dirnameC))
                shutil.copy2(os.path.join(dirname,file), os.path.join(dirnameC,file))
            elif os.path.isdir(os.path.join(dirname,file)) and not os.path.isdir(os.path.join(dirnameB,file)):
                if not os.path.isdir(os.path.join(dirnameC,file)):
                    os.system("mkdir %s"%(os.path.join(dirnameC,file)))
    else:
        if not os.path.isdir(dirnameC):
            os.system("mkdir %s"%(dirnameC))
   
        for file in names:
            shutil.copy2(os.path.join(dirname,file), os.path.join(dirnameC,file))

#============================================================
            
if __name__ == '__main__':
    #Test1("D:\workspace\openfilesync")
    #print  
    #Test2("D:\\")
    
    walk(PathA, visit, 0)
