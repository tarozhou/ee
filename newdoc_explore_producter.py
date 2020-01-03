#coding=utf-8
from RedisManager import RedisManager, REDIS_SERVER0, REDIS_SERVER1
from newdoc_explore_doclist import DocStruct
import utils.TimeUtils as tu



def LoadHistory():

    historyDict = {}

    try:
        with open("./data/history.txt", "rb") as f:
            for line in f:
                program_id = line.replace("\n","")
                historyDict[program_id] = True
    except:
        pass

    return historyDict

def LoadHotInDoc():

    rkey = "pick_v2_inhot"
    redis1_ = RedisManager(REDIS_SERVER1, 'Migu@2020')
    plist = redis1_.Handle().lrange(rkey, 0,-1)

    return [x.split("#")[0] for x in plist]


def LoadHotOutDoc():

    rkey = "pick_v2_outhot"
    redis1_ = RedisManager(REDIS_SERVER1, 'Migu@2020')

    return redis1_.Handle().lrange(rkey, 0,-1)


def LoadNewDoc():

    docDict = {}

    with open("./data/newdoc_v1.txt", "rb") as f:

        for line in f:
                arr = line.replace("\n","").split("\t")
                program_id = arr[0]
                create_time = arr[1]
                ds = DocStruct(program_id,50,0,0.0,create_time)
                docDict[program_id] = ds

    return docDict

def SaveNewDoc(docDict):

    fp = open("./data/newdoc_v2.txt","w")
    for k,v in docDict.items():
        fp.write("%s\n"%v.__str__())
    fp.close()


    fp = open("./data/history.txt","a")
    for k,v in docDict.items():
        fp.write("%s\n"%k)
    fp.close()

if "__main__" == __name__:

    hotout = LoadHotOutDoc()
    print "hotout length:",len(hotout)

    hotin = LoadHotInDoc()
    print "hotin length:", len(hotin)

    history = LoadHistory()
    print "history length:",len(history)

    newDoc = LoadNewDoc()
    print "newDoc length:", len(newDoc)

    res = {}

    nowTime = tu.GetNowUnixTime()

    for k,v in newDoc.items():

        if k in hotout:
            continue

        if k in hotin:
            continue

        if k in history:
            continue

        cTime = tu.String2UnixTime(v.create_time,"%Y-%m-%d %H:%M:%S")

        if nowTime - cTime > 24*60*60:
            continue

        res[k] = v

    print "res length:", len(res)

    SaveNewDoc(res)