#coding=utf-8

import TimeUtils
import math

class MiguRecommElem():

    def __init__(self,
                 id="",
                 title="",
                 create_time="",
                 score=0.0,
                 recomm_tag="",
                 kws="",
                 seg_words="",
                 detail = "",
                 level1 = "",
                 duration = ""
                 ):
        self.id = id
        self.title = title
        self.create_time = create_time
        self.recomm_tag = recomm_tag
        self.score = score
        self.kws = kws
        self.seg_words = seg_words
        self.detail = detail
        self.level1 = level1
        self.duration = duration

class MiguRecommList(list):

    def __init__(self,*args):
        super(MiguRecommList,self).__init__(*args)


    def UpdateScoreByHalfLife(self):

        def NewScore(elem):

            create_time = TimeUtils.String2UnixTime(elem.create_time,"%Y-%m-%d %H:%M:%S")
            now_time = TimeUtils.GetNowUnixTime()
            param = ((now_time-create_time)/(3600*12))*0.1
            elem.score = elem.score*math.pow(0.5,param)
            return elem

        map(NewScore,self)

    def FilterByTime(self,seconds):

        res = []
        nowUtime = TimeUtils.GetNowUnixTime()
        for i in self:
            createUtime = TimeUtils.String2UnixTime(i.create_time,"%Y-%m-%d %H:%M:%S")
            if nowUtime - createUtime < seconds:
                res.append(i)

        return MiguRecommList(res)



    def FilterByTitle(self,keyword):

        return MiguRecommList([x for x in self if x.title.find(keyword) >= 0 ])


    def GetIdList(self):

        return [x.id for x in self]

    def SortByScore(self,reverse=False):

        self.sort(key=lambda x:x.score,reverse=reverse)

    def SortByTime(self,reverse=False):

        self.sort(key=lambda x:TimeUtils.String2UnixTime(x.create_time,"%Y-%m-%d %H:%M:%S"),reverse=reverse)


    def FindById(self,id):

        return MiguRecommList([x for x in self if x.id==id])

    def Taken(self,n):
        return self[0:n]


if "__main__" == __name__:

    a = MiguRecommElem(id="1",title="aaa",score=35.0,create_time="2019-11-14 09:01:01")
    b = MiguRecommElem(id="2",title="bbb",score=35.0,create_time="2019-11-14 03:01:01")
    c = MiguRecommElem(id="3",title="ccc",score=37.0,create_time="2019-11-15 03:01:01")

    lMig = MiguRecommList()
    lMig.append(a)
    lMig.append(b)
    lMig.append(c)

    lMig.UpdateScoreByHalfLife()


    for l in lMig:
        print l.id,l.create_time,l.score
