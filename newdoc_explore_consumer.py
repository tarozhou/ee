#coding=utf-8
from newdoc_explore_doclist import DocStruct,DocDictStruct
from collections import Counter
from RedisManager import RedisManager, REDIS_SERVER0, REDIS_SERVER1
import utils.TimeUtils as TimeUtils
import urllib2
import json


def GetClickStatDebug(plist):

    pclickDict = {}
    for p in plist:
        pclickDict[p] = 20

    return pclickDict

def GetClickStat(plist):

    json_str = {"size": 0,
                "query":
                    {"bool":
                        {"must":
                            [
                                {"term": {"algorithm": "ee"}},
                                {"terms": {"programId": plist}}
                            ]
                        }
                    },
                "aggs":
                    {"result":
                         {"terms":
                              {"field": "programId.keyword"}
                          }
                     }
                }

    data = json.dumps(json_str)

    #url = 'http://10.167.136.12:9201/mgwhkjyxgs_click_201911/_search?pretty'
    url = 'http://10.167.136.12:9201/mgsxkjyxgs_click_total/_search?pretty'

    request = urllib2.Request(url, data=data)
    response = urllib2.urlopen(request)

    json_result = json.loads(response.read())

    bkt = json_result["aggregations"]["result"]["buckets"]

    pclickDict = {}

    for k in bkt:
        pclickDict[k["key"]] = int(k["doc_count"])

    return pclickDict


def GetClickStatRedis():


    res = {}
    rkey = "pick_v2_click_queue"
    redis1_ = RedisManager(REDIS_SERVER1, 'Migu@2020')
    plist = redis1_.Handle().lrange(rkey,0,-1)
    for p in plist:
        arr = p.split("#")
        program_id = arr[0]
        cnt = int(arr[1])
        if res.has_key(program_id):
            res[program_id] = res[program_id]+cnt
        else:
            res[program_id]=cnt

    return res

def CaculateFullDoc(fullDoc):

    plist = fullDoc.keys()

    pclickDict = GetClickStat(plist)

    pclickDictRedis = GetClickStatRedis()

    #用于测试Debug
    for k,v in pclickDictRedis.items():
        if pclickDict.has_key(k):
            pclickDict[k] = pclickDict[k] + v
        else:
            pclickDict[k] = v

    moreActionDoc = DocDictStruct()

    for programid,doc in fullDoc.items():

        if pclickDict.has_key(doc.id):
            ctr = float(pclickDict[doc.id])/doc.max_cnt
            doc.ctr = ctr
            print doc.id,doc.max_cnt,ctr,doc.create_time,pclickDict[doc.id]
            if ctr >= 0.08:
                if doc.max_cnt == 50:
                    doc.max_cnt = 200
                elif doc.max_cnt == 200:
                    doc.max_cnt = 500
                elif doc.max_cnt == 500:
                    doc.max_cnt = 1000
                elif doc.max_cnt == 1000:
                    doc.max_cnt = 5000
                elif doc.max_cnt == 5000:
                    doc.max_cnt=10000000

                moreActionDoc[programid] = doc


    try:
        with open("./data/offline_program.txt","a") as f:
            for k, v in fullDoc.items():
                if False == moreActionDoc.has_key(k):
                    f.write(k+"\n")
    except:
        pass

    return moreActionDoc


def UpLoadDocRedis(rkey,iDoc):

    redis1_ = RedisManager(REDIS_SERVER1, 'Migu@2020')
    plist = [y.__str__() for x,y in iDoc.items()]
    redis1_.Delete(rkey)
    if len(iDoc) > 0:
        redis1_.SetList(rkey, plist, 864000)


def DownLoadDoc():

    rkey = "pick_v2_ee_prepare"
    redis1_ = RedisManager(REDIS_SERVER1, 'Migu@2020')

    plist = redis1_.Handle().lrange(rkey, 0, -1)

    print "plist",len(plist)

    dds = DocDictStruct()
    for p in plist:
        arr = p.split("@")
        if len(arr) == 5:
            ds = DocStruct(arr[0],int(arr[1]),int(arr[2]),float(arr[3]),arr[4])
            dds[arr[0]] = ds

    return dds


def Stat(iDoc):

    res = {}

    for k,v in iDoc.items():

        if res.has_key(v.max_cnt):
            res[v.max_cnt] += 1
        else:
            res[v.max_cnt] = 1
    print "=======Stat======="
    for k,v in res.items():
        print "====max_cnt:",k," ===cnt:",v

def SetmList(mlist):

    rkey = "pick_v2_ee"
    redis1_ = RedisManager(REDIS_SERVER1, 'Migu@2020')
    redis1_.Delete(rkey)
    if len(mlist) > 0:
        redis1_.SetList(rkey, mlist, 864000)

    try:
        with open("./data/monitor_newdoc_explore.txt","w") as f:
            for m in mlist:
                f.write("%s\n"%m)
    except:
        pass


def GetExposeDocList():

    plist = []
    rkey = "pick_v2_exposure_queue"
    redis1_ = RedisManager(REDIS_SERVER1, 'Migu@2020')
    while redis1_.Handle().llen(rkey) > 0 and len(plist) < 10000:
        plist.append(redis1_.Handle().rpop(rkey))

    return plist

def GetNewDoc():

    dds = DocDictStruct()

    offline_program = {}
    try:
        with open("./data/offline_program.txt","r") as f:
            for line in f:
                program_id = line.replace("\n","")
                offline_program[program_id] = True
    except:
        pass

    try:
        with open("./data/newdoc_v2.txt", "rb") as f:
            for line in f:
                arr = line.replace("\n","").split("@")
                if len(arr) == 5 and False == offline_program.has_key(arr[0]):
                    ds = DocStruct(arr[0], int(arr[1]), int(arr[2]), float(arr[3]), arr[4])
                    dds[arr[0]] = ds
    except:
        pass

    return dds

if "__main__" == __name__:

    #Step0 从Redis中获取当前队列
    prepareDoc = DownLoadDoc()
    print "Step0 Init From Redis ========================= length of prepareDoc",len(prepareDoc)

    #Step1 将满足条件的新节目加入字典中
    newDoc = GetNewDoc()
    for k,v in newDoc.items():
        prepareDoc.AddNewElem(v)

    print "Step1 Merge New Doc =========================length of prepareDoc",len(prepareDoc)

    #Step2 消费队列，并将消费情况计入到字典中
    click_queue=GetExposeDocList()
    prepareDoc.UpdateCnt(dict(Counter(click_queue)))

    #Step3 将消费完的字典内容提出并加入到备选队列中，并通过ES统计反馈的点击计算点击率
    fullDoc = prepareDoc.ClearFullList()
    print "length of fullDoc=====", len(fullDoc)
    moreActionDoc = CaculateFullDoc(fullDoc)
    for k,v in moreActionDoc.items():
        prepareDoc.AddNewElem(v)
    print "Step3 Clear OffLine Doc=========================length of prepareDoc", len(prepareDoc)

    #Step4 清除过期的节目
    prepareDoc.ClearExpTimeList(3*24*60*60)
    print "Step4 Clear ExpTime Doc=========================length of prepareDoc", len(prepareDoc)

    Stat(prepareDoc)

    #Step5 从字典中输出满足条件的节目List到前线
    mlist = prepareDoc.GetIdList()
    SetmList(mlist)

    UpLoadDocRedis("pick_v2_ee_prepare",prepareDoc)