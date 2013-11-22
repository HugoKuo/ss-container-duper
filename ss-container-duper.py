#! /usr/bin/python
# 
#
# Name : ss-container-duper.py
# A simple script for handling new containers / object duplicate to 
# Recovery and Versioning containers
# Maintainer : Hugo Kuo
# The script must be running on one of Proxy nodes. Only for SwiftStack nodes. 
# You can set up a cronjob to execute it by root in a time period
# Reruies :  /var/log/swift/ , /etc/ss-container-duper/ss-container-duper.conf, /var/log/swift/ss-container-duper.log

import sys
import hashlib
import optparse
import urllib2
import pickle
import requests
import os 
import logging

from ast import literal_eval as eval
from ConfigParser import ConfigParser

from swift.common.ring import Ring
from swift.common.utils import hash_path


#```
#Example of conf file
#[default]
#USERNAME = maldivica
#PASSWORD = password
#HOST = http://localhost:8080
#AUTH_PATH = http://localhost:8080/auth/v1.0
#```
RECORD = "/etc/ss-container-duper/record-pickle.db"
CONF_FILE = "/etc/ss-container-duper/ss-container-duper.conf"
swift_dir = "/etc/swift/"
log_path = "/var/log/swift/ss-container-duper.log"
log_level = logging.INFO

logger = logging.getLogger('__name__')
hdlr = logging.FileHandler(log_path)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(log_level)

def get_new_conf(conf_file):
    #Parse config from configuration file
    c = ConfigParser()
    if not c.read(conf_file):
        print "Unable to read config file %s" % conf_file
        sys.exit(1)
    d = dict(c.items("default"))
    r = requests.get(d["auth_path"], headers={"x-auth-user": d["username"], "x-auth-key": d["password"]})
    _TOKEN = r.headers["x-storage-token"]
    _STORAGE_URI = d["host"]+"/v1/"
    _ACCOUNT_NAME = "AUTH_"+d["username"] 
    return _TOKEN, _ACCOUNT_NAME, _STORAGE_URI
    
def check_connection():
    #emit a account head to proxy and check the status code if != 204 call get_new_conf
    URL = STORAGE_URI+ACCOUNT_NAME+"/"
    rr = 0
    r = requests.head(URL, headers={"x-auth-token": TOKEN})
    if r.status_code == 204:
        rr = 1
    return rr

def gen_rev_container(container):
    #This function will create .rev & .ver containers
    #And setup the .ver as the versioning dest of .rev
    #Return status code of each request 
    # rev : recovery container
    # ver : versioning container 
    # version_header : set versioning header on the recovery container
    rev = ".rev_"+container
    ver = ".ver_"+container
    URL = STORAGE_URI+ACCOUNT_NAME+"/"
    rev_req = requests.put(URL+rev, headers={"x-auth-token":TOKEN})
    ver_req = requests.put(URL+ver, headers={"x-auth-token":TOKEN})
    
    logger.info("Create Recovery & Version Containers for %s , status %s:%s" % (container,rev_req.status_code,ver_req.status_code))
    if rev_req.status_code >= 200 and rev_req.status_code < 350 and ver_req.status_code >= 200 and ver_req.status_code < 350:
        version_header = requests.put(URL+rev, headers={"x-auth-token":TOKEN, "X-Versions-Location":ver})

    return rev_req.status_code, ver_req.status_code, version_header.status_code

def get_container_list(account):
    #Require a account eg. AUTH_ss
    #Return a list of containers within this account
    account_ring = Ring(swift_dir, ring_name="account")
    container_ring = Ring(swift_dir, ring_name="container")
    object_ring = Ring(swift_dir, ring_name="object")
    part, nodes = account_ring.get_nodes(account)

    #[FIXME] change the library to requests 
    URL="http://%s:%s/%s/%s/%s" % (nodes[0]['ip'], nodes[0]['port'], nodes[0]['device'], part, account)
    req = urllib2.Request(URL)
    #Direct connect to account server bypass proxy
    resp = urllib2.urlopen(req)
    content = resp.read()
    headers = resp.info()
    container_list_hash = hashlib.md5(content).hexdigest()
    content = content.split("\n")
    content.remove('')
    return content, container_list_hash

def container_filter(containers, st="."):
    containers_temp = containers[:]
    for i in containers_temp:
            if i.startswith(st):
                containers.remove(i)
    return containers

def get_obj_etag_dict(container):
    URL = STORAGE_URI+ACCOUNT_NAME+"/"
    objs = requests.get(URL+container+"?format=JSON", headers={"x-auth-token":TOKEN})
    objs_list = eval(objs.text)
    objs_dict = {}
    for i in objs_list:
        objs_dict[i["name"]]=i["hash"]
    return objs_dict

def x_copy_object(container, obj):
    #Performing the server-side copy of the object to .rev container
    URL = STORAGE_URI+ACCOUNT_NAME
    REV = "/.rev_"+container+"/"+obj
    objs = requests.put(URL+REV, headers={"x-auth-token":TOKEN, 
                                         "x-copy-from": "/"+container+"/"+obj, 
                                         "content-length": "0"})
    cp = requests.head(URL+REV, headers={"x-auth-token":TOKEN})
    cp_etag = cp.headers["etag"]
    #print "HUGO: xcopy %s" % objs.status_code
    logger.info("New version of file %s in %s is flying to recovery %s" % (obj, container, REV))
    return objs.status_code, cp_etag

 

if __name__ == '__main__':
    
    #Please make sure record-pickle.db was created in /etc/kill-teddy/
    if not os.path.isfile(RECORD):
        DB_tuple = ({}, {})
        pickle.dump(DB_tuple, open(RECORD, "wb"), True)
    
    #Check if there's a list_hash in the DB, if not, add a key
    #The pickle stores a tuple object, the first item is confifuration cache
    #The second item is a dict of data
    # DB_tuple[0] : includes prefetched configuration
    # DB_tuple[1] : obtains data key:value map
    DB_tuple = pickle.load(open(RECORD, "rb"))
    DB_conf = DB_tuple[0]
    DB_dict = DB_tuple[1]

    if not DB_dict.has_key("pre_list_hash"):
        DB_dict["pre_list_hash"] = None 

    #Configuration Parser from pickle DB
    TOKEN = DB_conf.get("TOKEN", "empty")
    ACCOUNT_NAME = DB_conf.get("ACCOUNT_NAME", "")
    STORAGE_URI = DB_conf.get("STORAGE_URI", "http://0.0.0.0")

    if not check_connection():
        TOKEN, ACCOUNT_NAME, STORAGE_URI = get_new_conf(CONF_FILE)
        DB_conf["TOKEN"] = TOKEN
        DB_conf["ACCOUNT_NAME"] = ACCOUNT_NAME
        DB_conf["STORAGE_URI"] = STORAGE_URI

 
    #Retrieve container list 
    containers, containers_hash = get_container_list(ACCOUNT_NAME)

    #Check new added containers 
    #Compare each container_name with DB, if not there, add it
    if containers_hash != DB_dict["pre_list_hash"]: 
        containers_temp = containers[:]
        for i in containers_temp:
            if i.startswith('.'):
                containers.remove(i)
        
        #Create rev, ver containers for the new one
        for i in containers:
            if not DB_dict.has_key(i):
                gen_rev_container(i) 
        logger.info("Found new container, update the list hash in DB")
    #Retrieve/renew the new containers list
    #Write new data into pickleDB
    containers, containers_hash = get_container_list(ACCOUNT_NAME)
    DB_dict["pre_list_hash"] = containers_hash
    

    for i in containers:
        if not DB_dict.has_key(i):
            DB_dict[i]="New Added"
        else:
            DB_dict[i]="re-checked"
    
    #Verifying all containers has .rev & .ver under the account
    pure_list = container_filter(containers=containers)
    for i in pure_list:
        if (not DB_dict.has_key(".rev_"+i)) or (not DB_dict.has_key(".ver_"+i)):
            gen_rev_container(i)
            DB_dict[".rev_"+i] = "new added"
            DB_dict[".ver_"+i] = "new added"

    pickle.dump(DB_tuple, open(RECORD, "wb"), True)

    
    #Compare etag of each object between origin and recovery container
    for i in pure_list:
        #Get object list of each container
        objs_etag_dict = get_obj_etag_dict(i)
        objs_etag_dict_rev = get_obj_etag_dict(".rev_"+i)
        for obj in objs_etag_dict:
            if not objs_etag_dict_rev.has_key(obj):
                #x-copy
                st, etag = x_copy_object(i,obj)
            elif objs_etag_dict[obj] != objs_etag_dict_rev[obj]:
                #x-copy
                st, etag = x_copy_object(i,obj)
            else: 
                passing = "True"
            DB_dict[i+"/"+obj] = objs_etag_dict[obj]
                    
    pickle.dump(DB_tuple, open(RECORD, "wb"), True)
    #logger.info(DB_tuple)
