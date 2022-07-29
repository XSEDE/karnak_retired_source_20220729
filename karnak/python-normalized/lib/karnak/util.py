
import calendar
import datetime
import logging
import math
import pytz
import sys
import time

logger = logging.getLogger(__name__)

##############################################################################################################

class tzoffset(datetime.tzinfo):

    def __init__(self, offset=0):
        self._offset = datetime.timedelta(seconds=offset)
    
    def utcoffset(self, dt):
        return self._offset

    def dst(self, dt):
        #return self._dstoffset
        return datetime.timedelta(0)

    def tzname(self, dt):
        return self._name

##############################################################################################################

def getText(doc, tag):
    nodes = doc.getElementsByTagName(tag)
    if len(nodes) < 1:
        return ""
    if nodes[0].firstChild == None:
        return ""
    if nodes[0].firstChild.data == None:
        return ""
    return nodes[0].firstChild.data

def strToNaiveDateTime(dateTime):
    if dateTime == "":
        return None
    # 2009-06-04T17:44:02Z
    return datetime.datetime.strptime(dateTime,"%Y-%m-%dT%H:%M:%SZ")

def epochToNaiveDateTime(epoch):
    if epoch is None or epoch <= 0:
        return None
    dt = datetime.datetime.utcfromtimestamp(epoch)
    #print("%d -> %s" % (epoch,dt))
    return dt

def strToDateTime(dateTime):
    if dateTime == "":
        return None
    # 2009-06-04T17:44:02Z
    dateTime = dateTime[:len(dateTime)-1]+" UTC"
    return datetime.datetime.strptime(dateTime,"%Y-%m-%dT%H:%M:%S %Z").replace(tzinfo=tzoffset(0))

def epochToDateTime(epoch, tz=tzoffset(0)):
    if epoch is None or epoch <= 0:
        return None
    return datetime.datetime.fromtimestamp(epoch,tz)

def dateTimeToEpoch(dt):
    if dt is None:
        return None
    if dt.utcoffset():
        print("  offset is %s" % dt.utcoffset())
        udt = dt - dt.utcoffset()
    else:
        udt = dt
    return time.mktime(udt.timetuple())

def hms(secs):
    hours = math.trunc(secs / (60*60))
    secs = secs - hours * 60*60
    mins = math.trunc(secs / 60)
    secs = int(secs - mins * 60)
    return "%02d:%02d:%02d" % (hours,mins,secs)

##############################################################################################################

if __name__ == "__main__":

    # 1372673894 -> 2013-07-01 10:18:14
    orig_epoch = 1372673894
    orig_dt_str = "2013-07-01T10:18:14Z"

    dt = datetime.datetime.strptime(orig_dt_str,"%Y-%m-%dT%H:%M:%SZ")
    print(orig_dt_str)
    print(dt)

    print(orig_epoch)
    print(calendar.timegm(dt.timetuple()))

    sys.exit(0)



    dt_str = "2009-06-04T17:44:02Z"
    print(dt_str)
    dt = strToDateTime(dt_str)
    print(dt.isoformat())
    epoch = dateTimeToEpoch(dt)
    print(epoch) # wrong
    print(int(dt.strftime("%s")))
    dt2 = epochToDateTime(epoch)
    print(dt2.isoformat())

    print("--------")
    dt = datetime.datetime.strptime(dt_str[:len(dt_str)-1],"%Y-%m-%dT%H:%M:%S")
    print(dt.isoformat())
    epoch = int(dt.strftime("%s"))
    print(epoch)
    dt2 = datetime.datetime.fromtimestamp(epoch)
    print(dt2.isoformat())

    print("--------")
    dt = datetime.datetime.strptime(dt_str[:len(dt_str)-1],"%Y-%m-%dT%H:%M:%S")
    print(dt.isoformat())

    dt = dt.replace(tzinfo=pytz.UTC)
    print(dt.isoformat())

    epoch = int(dt.strftime("%s"))
    print(epoch)

    dt2 = datetime.datetime.utcfromtimestamp(epoch)
    print(dt2.isoformat())
