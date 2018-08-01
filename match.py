#encoding: utf-8

import thread
import time
import threadpool
import common
import sys
import traceback
# import sendmsg

reload(sys)
sys.setdefaultencoding('utf8')

MAX_RETRY = 1

hostEve = "http://10.27.215.111:8080/search"
hostLucene = ""
keyId = "shopname"
keyScore = "score"
logFile = "/data/applogs/baymax-mtdeal-searcher-service/query.log"

total = 0
valid = 0
hvalid = 0

latest_changed = 0

def tailf(filename, h):
    try:
        f = open(filename, 'r')
        f.seek(0,2)
        while h == common.hour:
            line = f.readline()
            if not line:
                time.sleep(0.1)
                continue
            yield line
        common.log("sys", "tailf " + common.hour + " quit")
    except Exception, e3:
        print 'str(e):\t\t', str(e3)


def requestid(line):
    try:
        res = line.split("&")
        for str in res:
            if str.startswith("requestId="):
                tmp = str.split("=")
                return tmp[1]
        return ""
    except Exception:
        return ""


def match(query):
    try:
        global valid
        global hvalid
        count = 1
        while not compaire("unmatch", query):
            count = count + 1
            if count > MAX_RETRY:
                common.log_hour("query", query)
                return
            time.sleep(10)
        valid = valid + 1
        hvalid = hvalid + 1
    except Exception, e2:
        print 'str(e):\t\t', str(e2)
        return False


def compaire(kind, query):
    try:
        listEve = common.get_record(hostEve + query)
        listLucene = common.get_record(hostLucene + query)
        for eve, lucene in zip(listEve, listLucene):
            if eve.has_key(keyId) and lucene.has_key(keyId) and eve.has_key(keyScore) and lucene.has_key(keyScore):
                if eve[keyId] == lucene[keyId] and eve[keyScore] == lucene[keyScore]:
                    continue
                else:
                    common.log(kind, "requestId="+requestid(query)+",eve(id="+eve[keyId]+",score="+eve[keyScore]+"),lucene(id="+lucene[keyId]+",score="+lucene[keyScore]+")")
                    return False
            else:
                return False
        return True
    except Exception, e2:
        print 'str(e):\t\t', str(e2)
        return False


def hourproc(h):
    try:
        global latest_changed
        global total
        global hvalid
        htotal = 0
        hvalid = 0
        common.log("sys", "query.log " + h + " begin")
        pool = threadpool.ThreadPool(100)
        for i in tailf(logFile, h):
            latest_changed = int(common.hour) * 3600 + int(common.minute) * 60 + int(common.second)
            total = total + 1
            htotal = htotal + 1
            rate = 100 * hvalid / htotal
            if rate < 90:
                if (htotal % 10000) == 0:
                    msg = "EVE实时数据匹配成功率=" + str(rate)
                    # sendmsg.sendMsg(msg, ['zhangbo56', 'charlie.wang', 'sai.lv', 'zhaoyafeng'])
            tmp = i.split(" ")
            if len(tmp) < 3:
                continue
            query = tmp[2]
            pool.run(func=match, args=(query,))
            if (htotal % 1000) == 0:
                info = str(valid) + "/" + str(total) + "=" + str(valid*100/total) + "%," \
                       + str(hvalid) + "/" + str(htotal) + "=" + str(hvalid*100/htotal) + "%"
                common.log("result", info)
        pool.terminate()
    except Exception:
        print traceback.format_exc()


try:
    thread.start_new_thread(common.timer, ())
    latest_hour = 0
    latest_day = 0
    while True:
        if latest_day != common.today and latest_day > 0:
            common.remove_all_logs("unmatch-query", latest_day, show=True)
            latest_day = common.today
        # if latest_hour != common.hour:
        #     latest_hour = common.hour
        #     while True:
        #         if int(common.second) > 10:
        #             thread.start_new_thread(hourproc, (common.hour, ))
        #             break
        #         else:
        #             time.sleep(1)
        now = int(common.hour) * 3600 + int(common.minute) * 60 + int(common.second)
        if now - latest_changed > 10:
            updated = common.get_FileModifyTime(logFile)
            if updated > 0 and updated - latest_changed > 10:
                thread.start_new_thread(hourproc, (common.hour,))
                # print "latest_changed="+str(latest_changed), "updated="+str(updated)
                latest_changed = updated
        time.sleep(10)
except Exception:
    print traceback.format_exc()

