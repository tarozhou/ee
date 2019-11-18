#coding=utf-8

class DocStruct():

    def __init__(self,id,max_cnt,now_cnt,ctr,create_time):
        self.id = id
        self.max_cnt = max_cnt
        self.now_cnt = now_cnt
        self.ctr = ctr
        self.create_time = create_time

    def IsFull(self):

        return self.max_cnt <= self.now_cnt

    def NowCntPlus(self,cnt):

        self.now_cnt+=cnt

    def __str__(self):

        return "%s@%s@%s@%s@%s"%(self.id,self.max_cnt,self.now_cnt,self.ctr,self.create_time)

class DocDictStruct(dict):

    def __init__(self,*args):
        super(DocDictStruct,self).__init__(*args)

    def GetAvailableList(self):

        return DocDictStruct({k:v for k,v in self.items() if False==v.IsFull()})

    def ClearFullList(self):

        f_dict = DocDictStruct({k:v for k,v in self.items() if True==v.IsFull()})
        for k,v in f_dict.items():
            self.pop(k)

        return f_dict

    def UpdateCnt(self,cnt_dict):

        for k,v in cnt_dict.items():
            if self.has_key(k):
                self[k].NowCntPlus(v)

    def UpdateCtr(self,id,ctr):

        if self.has_key(id):
            self[id].ctr = ctr

    def GetIdList(self):

        return self.keys()

    def AddNewElem(self,elem):

        if False == self.has_key(elem.id):
            self[elem.id] = elem

    def See(self):

        for k,v in self.items():
            print k, v.max_cnt,v.now_cnt,v.ctr,v.create_time


if "__main__" == __name__:

    mydict = DocDictStruct()
    elem1 = DocStruct("1",10,0,0.0,"2019-11-01")
    elem2 = DocStruct("2",20,0,0.0,"2019-11-02")

    mydict["1"] = elem1
    mydict["2"] = elem2

    mydict.See()
    print "========================="

    mydict.UpdateCtr("1",0.5)

    mydict.See()




