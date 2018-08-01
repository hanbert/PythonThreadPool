import urllib2
import thread
import json
import sys
import time
import traceback
import common

reload(sys)
sys.setdefaultencoding('utf-8')

batch = 0
total = 0
result_none = 0
eve_none = 0
lucene_none = 0
eve_unexist = 0
lucene_unexist = 0
invalid = 0

#deal
area = "mtdealcpc"
hostEve = "http://10.27.215.112:8080/search?"
hostLucene = "http://10.32.79.164:8080/search?"
indexEve = "http://10.73.98.179:8080/inctrigger?area=evemtdealcpc&targetId=unit&isDelete=false"
indexLucene = "http://10.69.188.253:8080/inctrigger?area=mtdealcpc&targetId=unit&isDelete=false"




def getrecord(url, response):
    try:
        req = urllib2.Request(url)
        res_data = urllib2.urlopen(req)
        if response:
            res = json.loads(res_data.read())
            records = res["record"]
            return records
    except Exception, e:
        print 'str(e):\t\t', str(e), url
        return ""


# def error(info):
#     with open(os.getcwd()+"/error.txt", 'a+') as f:
#         f.write(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))+" "+info+"\n")


info = ""
targetid = ""

def compaire(query):
    global batch
    global total
    global result_none
    global eve_none
    global lucene_none
    global eve_unexist
    global lucene_unexist
    global invalid
    global targetid
    recordEve = getrecord(hostEve + query, True)
    recordLucene = getrecord(hostLucene + query, True)
    global info
    info = ""
    if isinstance(recordEve, list) is False or isinstance(recordLucene, list) is False:
        result_none = result_none + 1
        return True
    elif len(recordEve) == 0:
        print hostEve + query
        eve_none = eve_none + 1
        return True
    elif len(recordLucene) == 0:
        lucene_none = lucene_none + 1
        return True
    elif len(recordEve) > 1 or len(recordLucene) > 1:
        info += "eve.record=" + str(len(recordEve)) + ",lucene.record=" + str(len(recordLucene))
    else:
        dictEve = recordEve[0]
        dictLucene = recordLucene[0]
        if dictEve.has_key('targetid'):
            targetid = dictEve["targetid"]
        dictEve.pop('_docid')
        dictLucene.pop('_docid')
        if dictEve.has_key('poi'):
            dictEve.pop('poi')
        if dictLucene.has_key('poi'):
            dictLucene.pop('poi')
        if dictLucene.has_key('mtpoi'):
            dictEve.pop('mtpoi')
        if dictLucene.has_key('dealdiscounttype'):
            dictEve.pop('dealdiscounttype')
        if dictLucene.has_key('ctrlist'):
            dictEve.pop('ctrlist')

        # if dictEve.has_key('shoppoi'):
        #     dictEve.pop('shoppoi')
        # if dictLucene.has_key('shoppoi'):
        #     dictLucene.pop('shoppoi')

        # if dictEve.has_key('gpoi'):
        #     dictEve.pop('gpoi')
        # if dictLucene.has_key('gpoi'):
        #     dictLucene.pop('gpoi')

        # if dictEve.has_key('deals'):
        #     dictEve.pop('deals')
        # if dictLucene.has_key('deals'):
        #     dictLucene.pop('deals')

        # if dictEve.has_key('score'):
        #     dictEve.pop('score')
        # if dictLucene.has_key('score'):
        #     dictLucene.pop('score')

        # if dictEve.has_key('poi') and dictLucene.has_key('poi') and dictEve["poi"] == dictLucene["poi"]:
        #     info = "id=" + record["id"] + ",eve=" + dictEve["poi"] + ",lucene=" + dictLucene["poi"]
        #     print(info)
        #     return True


        # for key, value in dictLucene.items():
        #     if dictEve.has_key(key) == False:
        #         eve_unexist = eve_unexist + 1
        #         break
        unexist = ""
        for key, value in dictEve.items():
            if dictLucene.has_key(key) == False:
                unexist += key + ","
                # lucene_unexist = lucene_unexist + 1
                # break
            elif value != dictLucene[key]:
                info += "key=" + key + "\neve=" + value + "\nlucene=" + dictLucene[key] + "\n"
        # if unexist != "":
        #     info += "lucene unexist keys=" + unexist + "\n"
    if info != "":
        info = "id=" + record["id"] + "\n" + info
        return False
    else:
        return True


try:
    thread.start_new_thread(common.timer, ())
    records = getrecord(hostLucene+"query=all()&info=app:"+area+"&fl=id&limit=0,100000", True)
    for record in records:
        total = total + 1
        if total % 100 == 0:
            info = common.today + " " + common.hour + ":" + common.second + ",total=" + str(total) + ",result_none=" + str(result_none)\
                   + ",eve_none=" + str(eve_none) + ",lucene_none=" + str(lucene_none)\
                   + ",eve_unexist=" + str(eve_unexist) + ",lucene_unexist=" + str(lucene_unexist) + ",invalid=" + str(invalid)
            print(info)
        query = "query=term(id," + record["id"] + ")&info=app:"+area+"&fl=*"
        count = 0
        while not compaire(query):
            count = count + 1
            if count > 1:
                invalid = invalid + 1
                info = str(invalid) + "/" + str(total) + "," + info
                print(info)
                common.log("result", info)
                break
            getrecord(indexEve.replace("unit", targetid), False)
            getrecord(indexLucene.replace("unit", targetid), False)
            time.sleep(10)
except Exception, e:
    exstr = traceback.format_exc()
    print exstr
