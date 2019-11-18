import datetime
import time

def String2UnixTime(date_str,format):

    dt = datetime.datetime.strptime(date_str,format)
    tt = dt.timetuple()

    return int(time.mktime(tt))

def GetNow00UnixTime():

    today = datetime.date.today()
    return int(time.mktime(today.timetuple()))


def GetNowUnixTime():

    return time.mktime(datetime.datetime.now().timetuple())

def GetNowFTime():

    return time.strftime('%Y-%m-%d', time.localtime(time.time()))

if "__main__" == __name__:

    print String2UnixTime("2019-11-11 12:10:10","%Y-%m-%d %H:%M:%S")

    a = [1,2]
    stop = False
    while stop == False:
        try:
            print a.pop()
        except:
            stop = True

