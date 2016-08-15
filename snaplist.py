#!/usr/bin/python

import os, sys, pprint, time, socket, ConfigParser, datetime, calendar

from datetime import timedelta 
from purestorage import FlashArray
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-id", "--id", help="Array Name")
parser.add_argument("-s", "--snapshot", help="List all snapshot volumes of the given snapshot", required=False)
parser.add_argument("-d", "--destroy", help="Destroy the given snapshot", required=False, action="store_true")
parser.add_argument("-e", "--eradicate", help="Eradicate the given snapshot", required=False, action="store_true")
parser.add_argument("-p", "--pgroup", help="Optional -- Protection Group/Snapshot Group to show.", required=False)
parser.add_argument("-c", "--comma", help="Optional -- Print results in CSV format.", required=False, action="store_true")
args = parser.parse_args()

Config = ConfigParser.ConfigParser()
if os.path.isfile("/etc/pure.ini"):
  pureini="/etc/pure.ini"
else:
  pureini="/usr/local/purestorage/pure.ini"
Config.read(pureini)         

if args.id == None:
  try:
    PureAddr = os.environ['PURE']
  except:
    PureAddr=Config.get("default", "array")
else:
  PureAddr=args.id

try:
  api_token=Config.get(PureAddr, "api_key")
except:
  print "No Key found for {}".format(PureAddr)
  sys.exit(-1)

array = FlashArray(PureAddr, api_token=api_token)

SnapshotGroup_list = []
Snapschedule = ConfigParser.ConfigParser()
try:
  Snapschedule.read("/etc/snapsched.ini")
  for SnapshotGroup in Snapschedule.sections():
    SnapshotGroup_list.append(SnapshotGroup)
except:
  SnapshotGroup_list = []

utcnow = time.mktime(datetime.datetime.utcnow().timetuple())
now = time.mktime(datetime.datetime.now().timetuple())
utcoffset = datetime.timedelta(seconds=(utcnow - now))

def strfdelta(tdelta, fmt):
    d = {"days": tdelta.days}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    d["hours"] = "{:>02}".format(d["hours"])
    d["minutes"] = "{:>02}".format(d["minutes"])
    d["seconds"] = "{:>02}".format(d["seconds"])
    return fmt.format(**d)

SnapshotList = array.list_volumes(snap=True)
if type(SnapshotList).__name__ != 'list':
  SnapshotList = (SnapshotList, )

Volume_Snap_Sizes = array.list_volumes(snap=True, space=True)
if type(Volume_Snap_Sizes).__name__ != 'list':
  Volume_Snap_Sizes = (Volume_Snap_Sizes, )

PGroups = array.list_pgroups()
if type(PGroups).__name__ != 'list':
  PGroups = (PGroups, )
PGroupNames =  []
for i, pgroup in enumerate(PGroups):
  PGroupNames.append(pgroup["name"])

Snapshots=dict()
Snapshot_epochs=[]
keys={}
keys2={}
j=0
for i, snapshot in enumerate(SnapshotList):
  # pgroup snapshot names are in the form <pgroup>.<suffix>.<sourcename>
  # manual snapshot names are in the form <sourcename>.<suffix>
  # Note that if the volume name gets changed the <sourcename> in the snapshot doesn't get changed, but <source> does.
  snapshot_name_split=snapshot["name"].split(".")
  if snapshot_name_split[0] in PGroupNames:
    # This is a pgroup snapshot"
    SnapTitle = snapshot_name_split[0] + "." + snapshot_name_split[1]
  else:
    # This must be a manually created snapshot
    SnapTitle = snapshot_name_split[1]

  # group replicated pgroup snapshots
  try:
    Source="."+snapshot["source"].split(":")[1]
    SnapTitle=SnapTitle.replace(Source,"",1)
  except:
    SnapTitle=SnapTitle

  TZ=os.environ["TZ"]
  os.environ["TZ"] = "UTC"
  created_time=datetime.datetime.strptime(snapshot["created"], "%Y-%m-%dT%H:%M:%SZ")
  tm=time.mktime(time.strptime(snapshot["created"], "%Y-%m-%dT%H:%M:%SZ"))
  os.environ["TZ"] = TZ
  created_epoch=int(round(tm))
  created_time = datetime.datetime.fromtimestamp(created_epoch)
  key='%d' % created_epoch
  key=key+SnapTitle
  keys[SnapTitle]=key
  keys2[snapshot["name"]]=key
  if Snapshots.has_key(key):
    Snapshots[key]["volumes"]=Snapshots[key]["volumes"]+1
    Snapshots[key]["volumelist"].append(snapshot)
  else:
    Snapshot_epochs.append(key)
    Snapshots[key]=dict()
    Snapshots[key]["title"]=SnapTitle
    Snapshots[key]["created_time"]=created_time
    Snapshots[key]["created_epoch"]=created_epoch
    Snapshots[key]["volumelist"]=[]
    Snapshots[key]["volumelist"].append(snapshot)
    Snapshots[key]["volumes"]=1
    Snapshots[key]["snapshot_space"] = 0
for i, snapshot in enumerate(Volume_Snap_Sizes):
  key=keys2[snapshot["name"]]
  Snapshots[key]["snapshot_space"] = Snapshots[key]["snapshot_space"] + snapshot["snapshots"]
  
Snapshot_epochs.sort()
now=round(time.time())
if args.snapshot == None:
  if args.comma:
    print "array,created,age,snapshot,vols,Retention,Time_Left,size_mb"
  else:
    print "array      created                       age snapshot                        vols        Retention     Time Left  size_mb"
  for i, Snapshot in enumerate(Snapshot_epochs):
    level=""
    time_til_expirestr=""
    time_til_expire=""
    retention_time=""
    now = datetime.datetime.now()
    age=now - Snapshots[Snapshot]["created_time"]
    for j, SnapshotGroup in enumerate(SnapshotGroup_list):
      snapgroup_level_name = SnapshotGroup + ".L" 
      if snapgroup_level_name in Snapshots[Snapshot]["title"]:
        if ":" in Snapshots[Snapshot]["title"]:
          level = Snapshots[Snapshot]["title"].split(":")[1].replace(snapgroup_level_name,"",1)[0]
          option = "level" + level + "_target_retention"
        else:
          level = Snapshots[Snapshot]["title"].replace(snapgroup_level_name,"",1)[0]
          option = "level" + level + "_local_retention"


        if Snapschedule.has_option(SnapshotGroup, option):
          retention_time = datetime.timedelta(minutes=int(Snapschedule.get(SnapshotGroup, option)))
          if age > retention_time:
            time_til_expire = "ANYTIME NOW"
            time_til_expire = retention_time - age
            time_til_expire = datetime.timedelta(seconds=round(time_til_expire.total_seconds()))
          else:
            time_til_expire = retention_time - age
            time_til_expire = datetime.timedelta(seconds=round(time_til_expire.total_seconds()))
    created_tm = Snapshots[Snapshot]["created_time"].strftime("%m/%d/%Y %H:%M:%S")
    created_epoch = Snapshots[Snapshot]["created_epoch"]
    agestr = strfdelta(age, " {days}d-{hours}:{minutes}:{seconds}")
    agestr = agestr.replace(" 0d-", "", 1)
    agestr = agestr.replace(" ", "", 1)

    if time_til_expire == "":
      time_til_expirestr = ""
      time_til_expireseconds = ""
    else:
      time_til_expirestr = strfdelta(time_til_expire, " {days}d-{hours}:{minutes}:{seconds}")
      time_til_expirestr = time_til_expirestr.replace(" 0d-", "", 1)
      time_til_expirestr = time_til_expirestr.replace(" ", "", 1)
      time_til_expireseconds = int(time_til_expire.total_seconds())
  
    #if type(age).__name__ != 'datetime.timedelta' and type(retention_time).__name__ != 'datetime.timedelta'  and age > retention_time:
    try:
      if age > retention_time:
        time_til_expirestr = "Expiring soon"
    except:
        time_til_expirestr = ""

    if retention_time == "": 
      retention_timestr = ""
      retention_time_seconds = ""
    else:
      retention_timestr = strfdelta(retention_time, " {days}d-{hours}:{minutes}:{seconds}")
      retention_timestr = retention_timestr.replace(" 0d-", "", 1)
      retention_timestr = retention_timestr.replace(" ", "", 1)
      retention_time_seconds = int(retention_time.total_seconds())

    snapshot_space=round(float(Snapshots[Snapshot]["snapshot_space"])/1024/1024)

    if (args.pgroup != None and args.pgroup in Snapshots[Snapshot]["title"]) or args.pgroup == None:
      if args.comma:
        #print "{},{},{},{},{},{},{},{:1.0f}".format(PureAddr,created_tm,agestr,Snapshots[Snapshot]["title"],Snapshots[Snapshot]["volumes"],retention_timestr,time_til_expirestr, snapshot_space)
        print "{},{},{},{},{},{},{},{:1.0f}".format(PureAddr,created_epoch,int(age.total_seconds()),Snapshots[Snapshot]["title"],Snapshots[Snapshot]["volumes"],retention_time_seconds,time_til_expireseconds, snapshot_space)
      else:
        print "{:<10} {} {:>13} {:<32} {:>3} {:>16} {:>13} {:8.0f}".format(PureAddr,created_tm,agestr,Snapshots[Snapshot]["title"],Snapshots[Snapshot]["volumes"],retention_timestr,time_til_expirestr, snapshot_space)
else:
  hosts=array.list_hosts()
  if type(hosts).__name__ != 'list':
    hosts = (hosts, )
  allconnections=[]
  for i, hostrecord in enumerate(hosts):
    connections=array.list_host_connections(hostrecord["name"])
    if type(connections).__name__ != 'list':
      connections = [connections, ]
    allconnections.extend(connections)
  VolumeConnections = dict()
  for i, connection in enumerate(allconnections):
    if VolumeConnections.has_key(connection["vol"]):
      VolumeConnections[connection["vol"]].append(connection)
    else:
      VolumeConnections[connection["vol"]] = []
      VolumeConnections[connection["vol"]].append(connection)

  if args.comma:
    print "array,created,volume,size_gb,serial,hosts,hostgroup"
  else:
    print "array      created             volume                   size_gb serial                   hosts                hostgroup"
  for i, volume in enumerate(Snapshots[keys[args.snapshot]]["volumelist"]):
    created_tm=datetime.datetime.strptime(volume["created"], "%Y-%m-%dT%H:%M:%SZ") - utcoffset
    # pgroup snapshot names are in the form <pgroup>.<suffix>.<sourcename>
    # manual snapshot names are in the form <sourcename>.<suffix>
    snapshot_name_split = volume["name"].split(".")
    if len(snapshot_name_split) == 3:
      # This is a pgroup snapshot
      volname = snapshot_name_split[2]
    else:
      # This is a manual snapshot
      volname = snapshot_name_split[0]
    # volname=volume["source"]
    hostlist=""
    hostgroup=""
    try:
      for j, connection in enumerate(VolumeConnections[volname]):
        if hostlist == "":
          hostlist=connection["name"]
        else:
          hostlist=hostlist+";"+connection["name"]
        if connection["hgroup"] != None:
          hostgroup=connection["hgroup"]
    except:
      hostlist=""
      hostgroup=""
    if args.comma:
      #print "{},{},{},{},{},{},{}".format(PureAddr,created_tm,volname,volume["size"]/1024/1024/1024,volume["serial"],hostlist,hostgroup)
      print "{},{},{},{},{},{},{}".format(PureAddr,created_epoch,volname,volume["size"]/1024/1024/1024,volume["serial"],hostlist,hostgroup)
    else:
      print "{:<10} {} {:<24} {:>7} {} {:<20.20} {}".format(PureAddr,created_tm,volname,volume["size"]/1024/1024/1024,volume["serial"],hostlist,hostgroup)
    
  if args.destroy or args.eradicate:
    potential_pgroup = args.snapshot.rsplit('.')[0]
    try:
      pgroup = array.get_pgroup(potential_pgroup)
      print "destroying {}".format(args.snapshot)
      array.destroy_pgroup(args.snapshot)
      if args.eradicate:
        array.eradicate_pgroup(args.snapshot)
    except:
      for i, volume in enumerate(Snapshots[keys[args.snapshot]]["volumelist"]):
        print "destroying {}".format(volume["name"])
        snapshot = array.destroy_volume(volume["name"])
        if args.eradicate:
          snapshot = array.eradicate_volume(volume["name"])
     

array.invalidate_cookie()
