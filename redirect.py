#encoding: utf-8


import thread
import time
import common
import threadpool
import sys

reload(sys)
sys.setdefaultencoding('utf8')


host1 = "http://10.27.215.111:8080/search"
host2 = ""
logFile = "/data/applogs/baymax-deal-searcher-service/query.log"
# logFile = "/data/applogs/baymax-ad-searcher/query.log"
latest_changed = 0

def tailf(filename, h):
    try:
        f = open(filename,'r')
        f.seek(0, 2)
        while h == common.hour:
            line = f.readline()
            if not line:
                time.sleep(0.1)
                continue
            yield line
        common.log("sys", "redirect tailf " + common.hour + " quit")
    except Exception, e:
        print 'str(Exception):\t', str(Exception)
        print 'str(e):\t\t', str(e)


def send(type, query, send_times):
    try:
        count = 0
        # wait = 1.0 / send_times
        while count < send_times:
            count = count + 1
            common.get_record(query)
            # time.sleep(wait)
    except Exception, e:
        print 'str(e):\t\t', type, str(e)


def hourproc(h, send_times):
    try:
        global latest_changed
        common.log("sys", "redirect V0.3.1 query.log " + h + " begin,send_times="+str(send_times))
        total = 0
        n1 = 0
        n2 = 0
        pool = threadpool.ThreadPool(100)
        for i in tailf(logFile, h):
            latest_changed = common.now
            if len(i) < 200:
                continue
            total = total + 1
            info = "total=" + str(total)
            query = i.split(" ")[2]
            if query == "":
                continue
            if host1 != "":
                pool.run(func=send, args=("host1", host1 + query, send_times,))
                n1 = n1 + send_times
                info += ",n1=" + str(n1)
            if host2 != "":
                pool.run(func=send, args=("host2", host2 + query, send_times,))
                n2 = n2 + send_times
                info += ",n2=" + str(n2)
            if (total % 1000) == 0:
                common.log("redirect", info)
    except Exception, e:
        print 'str(Exception):\t', str(Exception)
        print 'str(e):\t\t', str(e)


if __name__ == '__main__':
    try:
        thread.start_new_thread(common.timer, ())
        common.now = int(time.time())
        latest = 0
        send_times = 1
        if len(sys.argv) > 1:
            send_times = int(sys.argv[1])
        while True:
            if common.now - latest_changed > 10:
                updated = common.get_FileModifyTime(logFile)
                if updated > 0 and updated - latest_changed > 10:
                    thread.start_new_thread(hourproc, (common.hour, send_times,))
                    # print "latest_changed=" + str(latest_changed), "updated=" + str(updated)
                    latest_changed = updated
            time.sleep(10)
    except Exception, e:
        print 'str(Exception):\t', str(Exception)
        print 'str(e):\t\t', str(e)

