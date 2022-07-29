###############################################################################
#   Copyright 2015 The University of Texas at Austin                          #
#                                                                             #
#   Licensed under the Apache License, Version 2.0 (the "License");           #
#   you may not use this file except in compliance with the License.          #
#   You may obtain a copy of the License at                                   #
#                                                                             #
#       http://www.apache.org/licenses/LICENSE-2.0                            #
#                                                                             #
#   Unless required by applicable law or agreed to in writing, software       #
#   distributed under the License is distributed on an "AS IS" BASIS,         #
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  #
#   See the License for the specific language governing permissions and       #
#   limitations under the License.                                            #
###############################################################################

import calendar
import datetime
import logging
import math
import pytz
import sys
import time

logger = logging.getLogger(__name__)

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

def strToDateTime(date_time):
    if date_time is None or date_time == "":
        return None
    # 2009-06-04T17:44:02Z
    # just do naive time zones - everything is in UTC
    return datetime.datetime.strptime(date_time[:len(date_time)-1],"%Y-%m-%dT%H:%M:%S")

# assume UTC and produce naive
def epochToDateTime(epoch):
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
    orig_dt_str2 = "2013-07-01T10:19:03Z"


    dt = datetime.datetime.strptime(orig_dt_str,"%Y-%m-%dT%H:%M:%SZ")
    dt2 = datetime.datetime.strptime(orig_dt_str2,"%Y-%m-%dT%H:%M:%SZ")
    diff = dt2-dt
    print(orig_dt_str)
    print(dt)
    print( dt2-dt)
    print( diff > 284)

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
