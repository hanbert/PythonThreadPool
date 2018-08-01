#encoding: utf-8

import time
import os
import urllib2
import json
import sys
import glob

reload(sys)
sys.setdefaultencoding('utf8')

today = ""
now = 0
hour = 0
minute = 0
second = 0


def timer():
    global today
    global now
    global hour
    global minute
    global second
    while True:
        now = int(time.time())
        tmp = time.strftime('%Y-%m-%d %H %M %S', time.localtime(now)).split(" ")
        today = tmp[0]
        hour = tmp[1]
        minute = tmp[2]
        second = tmp[3]
        time.sleep(1)


def log(kind, info):
    with open(os.getcwd() + "/logs/" + kind + "-" + today + ".txt", 'a+') as f:
        f.write(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+" "+info+"\n")


def log_hour(kind, info):
    with open(os.getcwd() + "/logs/" + kind + "-" + today + "-" + hour + ".txt", 'a+') as f:
        f.write(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+" "+info+"\n")


def println(path, kind, info):
    path_day = os.getcwd() + "/" + path + "/" + today + "/"
    if not os.path.exists(path_day):
        os.mkdir(path_day)
    with open(path_day + kind + ".txt", 'a+') as f:
        f.write(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+" "+info+"\n")


def get_all(url):
    try:
        req = urllib2.Request(url)
        res_data = urllib2.urlopen(req)
        res = json.loads(res_data.read())
        return res
    except Exception, e:
        log("error", str(e))


def get_record(url):
    try:
        req = urllib2.Request(url)
        res_data = urllib2.urlopen(req)
        res = json.loads(res_data.read())
        records = res["record"]
        return records
    except Exception, e:
        log("error", str(e))


def files(curr_dir = '.', ext = '*.exe'):
    """当前目录下的文件"""
    for i in glob.glob(os.path.join(curr_dir, ext)):
        yield i


def all_files(rootdir, ext):
    """当前目录下以及子目录的文件"""
    for name in os.listdir(rootdir):
        if os.path.isdir(os.path.join(rootdir, name)):
            try:
                for i in all_files(os.path.join(rootdir, name), ext):
                    yield i
            except:
                pass
    for i in files(rootdir, ext):
        yield i


def remove_files(rootdir, ext, show = False):
    """删除rootdir目录下的符合的文件"""
    for i in files(rootdir, ext):
        if show:
            print i
        os.remove(i)


def remove_all_files(rootdir, ext, show = False):
    """删除rootdir目录下以及子目录下符合的文件"""
    for i in all_files(rootdir, ext):
        if show:
            print i
        os.remove(i)


def remove_all_logs(kind, day, show = False):
    remove_all_files(os.getcwd() + "/logs/", kind+"-"+str(day)+"*.log", show)


def get_FileModifyTime(filePath):
    try:
        filePath = unicode(filePath,'utf8')
        t = os.path.getmtime(filePath)
        return int(t)
    except Exception:
        return 0