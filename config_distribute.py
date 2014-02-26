#public configure
exclude_exts=['.swx', '.swp', '.txt~', '.tmp', '.svn']


#source/client configue
wwwroot = '/data/ysten'
monitorpath = wwwroot
httphostname = 'http://223.82.137.215'
broker = "redis://223.82.137.215:6379//"
backend = "redis://223.82.137.215:6379//"


hash_num = 8
hash_config = {
#multity to multiy relationship
#hash_code : node array
    0: ['192.168.0.1', '192.168.0.2'],
    1: ['192.168.0.1', '192.168.0.2'],
    2: ['192.168.0.3', '192.168.0.4'],
    3: ['192.168.0.3', '192.168.0.4'],
    4: ['192.168.0.5', '192.168.0.6'],
    5: ['192.168.0.5', '192.168.0.6'],
    6: ['192.168.0.7', '192.168.0.8'],
    7: ['192.168.0.7', '192.168.0.8'],
}

