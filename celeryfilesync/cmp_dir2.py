'''
Created on 2013-8-8

@author: Administrator
'''
import sys, os

def get_intersection(arg1, arg2):
    return set(arg1).intersection(arg2)

def cmp_file(file1, file2):
    #print "cmp_file"
    if open(file1, "r").read() != open(file2, "r").read():
        #cmd = "diff " + file1 + " " + file2 + " > " + file1 + ".dif"
        #os.system(cmd)
        print "diff: %s, %s" % (file1, file2)
    else:
        print "same: %s, %s" % (file1, file2)    


def cmp_dir(arg1, arg2):
    files1 = os.listdir(arg1)
    files2 = os.listdir(arg2)
    files = get_intersection(files1, files2)
    print files1
    print files2
    print files

    set1 = set(files1)
    set2 = set(files2)
    print "set1: ", set1
    print "set2: ", set2
    
    setdiff = set2 - set1
    print "setdiff: ", setdiff
    
    print "set_sym_diff: ", set1 ^ set2
    
    for file in files:
        file1 = os.path.join(arg1, file)
        file2 = os.path.join(arg2, file)

        print file1, file2

        if os.path.isdir(file1) and os.path.isdir(file2):
            cmp_dir(file1, file2)
        else:
            cmp_file(file1, file2)
    
    print

def main():
    argc = len(sys.argv)

    if argc < 2:
        sys.exit("Usage: cmpdir [srcdir] <destdir>")

    if argc == 2:
        srcdir = os.getcwd()
        dstdir = sys.argv[1]
    else:
        srcdir = sys.argv[1]
        dstdir = sys.argv[2]

    cmp_dir(srcdir, dstdir)


if __name__ == "__main__":
    #main()  
    cmp_dir("E:\FullData", "E:\IncomplData") 
    
    set1 = set([1,2,3,4])
    set2 = set([3,4,5,6])
    print set1 - set2
    print set2 - set1
    
    print set2 & set1
    print set2 | set1
    print set1 ^ set2