#!/usr/bin/env python

import os, sys, pprint, time, socket, ConfigParser
from datetime import timedelta

from purestorage import FlashArray

import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-id", "--id", help="Array Name")
parser.add_argument("-s", "--snapshot", help="Snapshot Name that will be copied", required=True)
parser.add_argument("-t", "--target", help="Name of Target Host or Host Group to which the new volumes will be connected")
parser.add_argument("-a", "--audit", help="Only perform audit. Do not copy snapshot or connect copied volume to target", action="store_true")
parser.add_argument("-e", "--epochtimestamp", help="Append snapshot epoch timestamp to new volume name", action="store_true")
parser.add_argument("-p", "--prefix", help="Optional Prefix to add to new volume name.")
parser.add_argument("-f", "--force", help="Force the copy to overwrite if the new volume name is an existing volume", action="store_true")
parser.add_argument("-o", "--objecttype", help="ObjectType of source used, host, hgroup, or volume string. Default=volstr", choices=["host","hgroup","volstr","vollist"])
parser.add_argument("-ss", "--source", help="Source Host, Host Group, VolumeString, or list of volumes of OBJECTTYPE")
parser.add_argument("-C", "--CMD", help="Show CLI Commands that could be executed to perform the work", action="store_true")
parser.add_argument("-R", "--REST", help="Show REST Commands that could be executed to perform the work", action="store_true")

#parser.add_argument("type", help="Type of source used, host, hgroup, or volume string", choices=["host","hgroup","volstr"])
#parser.add_argument("source", help="Source Host, Host Group, or Volume String")
args = parser.parse_args()

if args.prefix == None:
  if not args.epochtimestamp and not args.force:
    print
    print "The new volume names will be the same as the old, which will overwrite existing volumes!!"
    print "Either specify a -p/--prefix option or -e/--epochtimestamp option to copy to a new volume name."
    print "Or, if you really wish to overwrite the existing volumes specify the -f/--force option instead."
    print "Cannot continue. Exiting."
    sys.exit(-1)
  args.prefix=""

if args.audit:
  print "Argument -a/--audit detected. Will not execute copy or connection commands."

if ":" in args.snapshot:
  snapshot = args.snapshot.split(":")[1]
else:
  snapshot = args.snapshot

snapshot1=snapshot + "."
snapshot2="." + snapshot

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

if args.objecttype == None:
  args.objecttype = "volstr"

vols=[]
if args.objecttype == "host" or args.objecttype == "hgroup":
  try:
    connections=array.list_host_connections(args.source)
  except:
    connections=[]
  for i, hostrecord in enumerate(connections):
    vols.append(hostrecord["vol"])
elif args.objecttype == "vollist":
  vols = args.source.replace(" ","",1).split(",")

targethostfound=False
targethgroupfound=False
if args.target != None:
  try:
    hosts=array.list_hosts()                         
  except:
    hosts=[]
  if type(hosts).__name__ != 'list':
    hosts = (hosts, )
  for i, hostrecord in enumerate(hosts):
    if args.target == hostrecord["name"]:
      targethostfound=True
    if args.target == hostrecord["hgroup"]:
      targethgroupfound=True
  if not targethostfound and not targethgroupfound:
    print "Target host or host group {} was not found on the array. Specify and existing host or hgroup".format(args.target)
    sys.exit(-1)

COPY_CMD = []
CONNECT_CMD = []
REST_COPY_CMD = []
REST_CONNECT_CMD = []
snapshots=array.list_volumes(snap=True)
if type(snapshots).__name__ != 'list':
  snapshots = (snapshots, )
for j, snapshot_record in enumerate(snapshots):
  if snapshot1 in snapshot_record["name"] or snapshot2 in snapshot_record["name"]: # This is a potential volume snapshot to copy
    # get the source volume name of this snapshot
    volname = snapshot_record["source"]
    if type(volname).__name__ == 'NoneType':
      # Let's figure out the source volume name then
      snapshotname_list = snapshot_record["name"].split('.')
      if len(snapshotname_list) == 3:
        # Then this is a Protection Group Snapshot and the source volume name is the last (3rd) member of the previously splitted field
        volname=snapshotname_list[2]
      else:
        # Then this is a Manual Snapshot and the source volume name is the first member of the previously splitted field
        volname=snapshotname_list[0]
    if ":" in volname:
      volname = volname.split(":")[1]
    # Check to see if this volume is in the list that we can copy
    copy_this_volume=False
    if args.source == None:
      copy_this_volume=True
    elif volname in vols:
      copy_this_volume=True
    elif args.objecttype == "volstr" and args.source in volname:
      copy_this_volume=True

    if copy_this_volume:
      TZ=os.environ["TZ"]
      os.environ["TZ"] = "UTC"
      tm=time.mktime(time.strptime(snapshot_record["created"], "%Y-%m-%dT%H:%M:%SZ"))
      os.environ["TZ"] = TZ
      created_epoch=int(round(tm))
      created_tm = time.strftime("%m/%d/%Y %H:%M:%S %Z" ,time.localtime(tm))
      if args.epochtimestamp:
        newvol=args.prefix + volname + "_" + '%s' % created_epoch
      else:
        newvol=args.prefix + volname

      try:
        newvol_record = array.get_volume(newvol)
        newvol_found = True
      except:
        newvol_record = ""
        newvol_found = False

      if newvol_found:
        if args.force:
          print "Snapshot {} of Volume {}, from {}, is being restored back to the source volume {}".format(snapshot_record["name"],volname,created_tm,newvol)

          REST_COPY_CMD.append("copy_volume {} {} overwrite=True".format(snapshot_record["name"], newvol))
          COPY_CMD.append("purevol copy {} {} --overwrite".format(snapshot_record["name"], newvol))

          if not args.audit:
            array.copy_volume(snapshot_record["name"], newvol, overwrite=True)
        else:
          print "Cannot restore Snapshot {} of Volume {} to existing volume {} without the -f/--force option.".format(snapshot_record["name"],volname,newvol)
      else:
        print "Snapshot {} of Volume {}, from {}, is being copied to volume {}".format(snapshot_record["name"],volname,created_tm,newvol)
        REST_COPY_CMD.append("copy_volume {} {}".format(snapshot_record["name"], newvol))
        COPY_CMD.append("purevol copy {} {}".format(snapshot_record["name"], newvol))
        if not args.audit:
          array.copy_volume(snapshot_record["name"], newvol)
        if targethostfound:
          print "  Connecting new Volume, {} to Host, {}".format(newvol,args.target)
          print
          REST_CONNECT_CMD.append("connect_host {} {}".format(args.target,newvol))
          CONNECT_CMD.append("purehost connect --vol {} {}".format(newvol,args.target))
          if not args.audit:
            array.connect_host(args.target,newvol)
        if targethgroupfound:
          print "  Connecting new Volume, {} to Host Group, {}".format(newvol,args.target)
          print
          REST_CONNECT_CMD.append("connect_hgroup {} {}".format(args.target,newvol))
          CONNECT_CMD.append("purehost connect --vol {} {}".format(newvol,args.target))
          if not args.audit:
            array.connect_hgroup(args.target,newvol)

if args.CMD and COPY_CMD != []:
  print
  print "Here are the Pure Storage CLI commands to use to do the work above"
  for i, cmd  in enumerate(COPY_CMD):
    print cmd
  for i, cmd  in enumerate(CONNECT_CMD):
    print cmd

if args.REST and REST_COPY_CMD != []:
  print
  print "Here are the REST commands to use to do the work above"
  for i, cmd  in enumerate(REST_COPY_CMD):
    print cmd
  for i, cmd  in enumerate(REST_CONNECT_CMD):
    print cmd

array.invalidate_cookie()
