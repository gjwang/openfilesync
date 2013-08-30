import urllib
import urllib2
#import requests
from os.path import basename
from urlparse import urlsplit

def download(url, localFileName = None):
    def url2name(url):
        return basename(urlsplit(url)[2])
    
    req = urllib2.Request(url)
    r = urllib2.urlopen(req)
    
    if localFileName: 
        # we can force to save the file as specified name
        localName = localFileName    
    elif r.info().has_key('Content-Disposition'):
        # If the response has Content-Disposition, we take file name from it
        localName = r.info()['Content-Disposition'].split('filename=')[1]
        if localName[0] == '"' or localName[0] == "'":
            localName = localName[1:-1]
    else: 
        # if we were redirected, the real file name we take from the final URL
        localName = url2name(r.url)
        
        
    print localName    
    with open(localName, "wb") as file:
        file.write(r.read())
    
    #urllib.urlretrieve ("http://www.example.com/songs/mp3.mp3", localName)        

#Yet another one, with a "progressbar"
def download2(url, localFileName = None):
    
    if localFileName:
        file_name = localFileName
    else:
        file_name = url.split('/')[-1]
        
    u = urllib2.urlopen(url)
    f = open(file_name, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    print "Downloading: %s Bytes: %s" % (file_name, file_size)
    
    file_size_dl = 0
    block_sz = 8192
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break
    
        file_size_dl += len(buffer)
        f.write(buffer)
        status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
        status = status + chr(8)*(len(status)+1)
        print status,
    
    f.close()    
    
if __name__ == "__main__":
    download("http://www.blog.pythonlibrary.org/wp-content/uploads/2012/06/wxDbViewer.zip")
    download("http://192.168.5.60/cctv/tencent.mp4");
    #download2("http://192.168.5.60/cctv/tencent.mp4");    
