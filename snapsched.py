#!/usr/bin/env python

###################
# snapsched.py
# Purpose:
#       Runs the Snapshot Scheduler for Pure Storage
version=2.10
# Author: Russell Peters
# Modified:
# When       Who                What
# ---------- ------------------ -------------------------------------------------
# 10/29/2015 Russell Peters     v2.10 added workaround for getboolean bug. That caused threads to crash
#                               Changed query for checking replication state of a snapshot group from 
#                               source to target.
# 10/29/2015 Russell Peters     v2.11 Added mechanism to fix defect so that config file read is "atomic" 
#                               and thread safe
# 11/18/2015 Russell Peters     v2.12 Added new noexpirelist option. Snapshots in the list will no expire
# 12/03/2015 Russell Peters     v2.13 Fixed defect for infrequent snapshots. Added eradicate feature
#
###################

import os, sys, time, socket, ConfigParser, math, datetime, argparse, threading, re
from purestorage import FlashArray

class SnapshotGroupThread (threading.Thread):
  def __init__(self, threadID, name, SnapshotGroup):
    threading.Thread.__init__(self)
    self.threadID = threadID
    self.name = name
    self.SnapshotGroup = SnapshotGroup
    self.number = 1
    self.LogName = LogDir + "/" + name + "." + str(self.threadID) + ".log"
    self.Log = open(self.LogName, 'w', 0)
    self.level = 1
    self.ERROR = False
    self.replicating = False
    self.TargetProgress = 1
    self.level1_interval = 15
    try:
      PureAddr = Snaps.get(self.SnapshotGroup, "array")
    except:
      try:
        PureAddr = Config.get("default", "array")
      except:
        LogWrite(self.Log, "ERROR", "No array specified. Cannot continue.")
        self.ERROR = True
    try:
      api_token=Config.get(PureAddr, "api_key")
    except:
      LogWrite(self.Log, "ERROR", "No Key found for {}".format(PureAddr))
      self.ERROR = True
    try:
      self.array = FlashArray(PureAddr, api_token=api_token)
    except:
      LogWrite(self.Log, "ERROR", "API Key found for {} doesn't work. Cannot continue".format(PureAddr))
      self.ERROR = True

    if Snaps.has_option(self.SnapshotGroup, "target_array"):
      self.TargetSnapshotGroup = PureAddr + ":" + SnapshotGroup
      self.TargetArray = Snaps.get(self.SnapshotGroup, "target_array")
      try:
        api_token=Config.get(self.TargetArray, "api_key")
      except:
        LogWrite(self.Log, "ERROR", "No Key found for {}. Will not replicate.".format(self.TargetArray))
        self.TargetArray = ""
        return
      try:
        self.target_array = FlashArray(self.TargetArray, api_token=api_token)
      except:
        LogWrite(self.Log, "ERROR", "API Key found for {} doesn't work. Will not replicate".format(self.TargetArray))
        self.TargetArray = ""
        return
      try:
        pgroup = self.array.set_pgroup(self.SnapshotGroup, targetlist=self.TargetArray.split("982z*&923km@A"))
      except:
        try:
          pgroup = self.array.create_pgroup(self.SnapshotGroup)
          pgroup = self.array.set_pgroup(self.SnapshotGroup, targetlist=self.TargetArray.split("982z*&923km@A"))
        except:
          LogWrite(self.Log, "ERROR", "Could not create pgroup {}. It probably has been destroyed and is pending eradication.".format(self.SnapshotGroup))
          self.ERROR = True
      try:
        pgroup = self.target_array.set_pgroup(self.TargetSnapshotGroup, allowed=True)
      except:
        pgroup = ""
    else:
      self.TargetArray = ""
    return
      
  ## End of __init__ ##


  # This function refreshes the following option variables
  # enabled
  # replicate
  # starttime
  # vol_includestr
  # vol_excludestr
  # hosts
  # snapscript
  # snapscript_levels
  # level1_local_retention
  # level2_local_retention
  # level3_local_retention
  # level1_target_retention
  # level2_target_retention
  # level3_target_retention
  # level1_interval
  # level2_interval
  # level3_interval
  # noexpirelist
  # 
  def refresh_from_cfg(self):
    if not Snaps.has_section(self.SnapshotGroup):
      LogWrite(self.Log, "INFO", "The snapshot group, {} has been removed from the configuration.".format(self.SnapshotGroup))
      self.Exit()

    self.enabled = True
    if Snaps.has_option(self.SnapshotGroup, "enabled"):
      try:
        if not Snaps.getboolean(self.SnapshotGroup, "enabled"):
          self.enabled = False
          LogWrite(self.Log, "INFO", "The snapshot group, {} has been disabled.".format(self.SnapshotGroup))
          self.Exit()
      except:
        pass
    try:
      if Snaps.has_option(self.SnapshotGroup, "replicate") and Snaps.getboolean(self.SnapshotGroup, "replicate") and self.TargetArray != "":
        self.replicate = True
      else:
        self.replicate = False
    except:
      pass

    now = datetime.datetime.now()
    try:
      self.starttime=datetime.datetime.strptime(Snaps.get(self.SnapshotGroup, "starttime"), "%H:%M")
      self.starttime=self.starttime.replace(now.timetuple().tm_year,now.timetuple().tm_mon,now.timetuple().tm_mday)-datetime.timedelta(days=1)
    except:
      LogWrite(self.Log, "WARN", "No named variable starttime found or error in starttime in {}. Defaulting to 00:00".format(SnapsConfigFile))
      self.starttime=datetime.datetime.strptime("00:00", "%H:%M")
      self.starttime=self.starttime.replace(now.timetuple().tm_year,now.timetuple().tm_mon,now.timetuple().tm_mday)-datetime.timedelta(days=1)

    try:
      self.vol_excludestr = Snaps.get(self.SnapshotGroup, "vol_excludestr").replace(" ","",1).split(",")
    except:
      LogWrite(self.Log, "DEBUG", "No valid vol_excludestr found in {}. Will not explicitly exclude any volumes".format(SnapsConfigFile))
      self.vol_excludestr = [ "}zym2cx)^efb)#290r5x:<" ]

    try:
      self.vol_includestr = Snaps.get(self.SnapshotGroup, "vol_includestr").replace(" ","",1).split(",")
    except:
      LogWrite(self.Log, "DEBUG", "No valid vol_includestr found in {}. Will not explicitly include any volumes".format(SnapsConfigFile))
      self.vol_includestr = [ "87sdkj.9874ks<$sk942" ]

    try:
      self.hosts = Snaps.get(self.SnapshotGroup, "hosts").replace(" ","",1).split(",")
    except:
      LogWrite(self.Log, "DEBUG", "No valid hosts value found in {}. Will not explicitly include any host volumes".format(SnapsConfigFile))
      self.hosts = []

    try:
      self.snapscript = Snaps.get(self.SnapshotGroup, "snapscript")
      self.snapscript = self.snapscript.replace("SnapshotGroup",self.SnapshotGroup,1)
    except:
      self.snapscript = ""

    try:
      self.snapscript_levels = Snaps.get(self.SnapshotGroup, "snapscript_levels").replace(" ","",1).split(",")
    except:
      self.snapscript_levels = []

    try:
      self.noexpirelist = Snaps.get(self.SnapshotGroup, "noexpirelist").replace(" ","",1).split(",")
    except:
      self.noexpirelist = []

    try:
      self.level1_local_retention = int(Snaps.get(self.SnapshotGroup, "level1_local_retention"))
    except:
      self.level1_local_retention = 0

    try:
      self.level2_local_retention = int(Snaps.get(self.SnapshotGroup, "level2_local_retention"))
    except:
      self.level2_local_retention = 0

    try:
      self.level3_local_retention = int(Snaps.get(self.SnapshotGroup, "level3_local_retention"))
    except:
      self.level3_local_retention = 0

    try:                         
      self.level1_target_retention = int(Snaps.get(self.SnapshotGroup, "level1_target_retention"))
    except:             
      self.level1_target_retention = self.level1_local_retention
                        
    try:                
      self.level2_target_retention = int(Snaps.get(self.SnapshotGroup, "level2_target_retention"))
    except:             
      self.level2_target_retention = self.level2_local_retention
                        
    try:                
      self.level3_target_retention = int(Snaps.get(self.SnapshotGroup, "level3_target_retention"))
    except:             
      self.level3_target_retention = self.level3_local_retention

    prev_level1_interval = self.level1_interval
    try:
      self.level1_interval = int(Snaps.get(self.SnapshotGroup, "level1_interval"))
    except:
      self.level1_interval = 0
    
    if self.level1_interval == 0:
      self.level1_interval = prev_level1_interval
      self.level2_interval = 0
      self.level3_interval = 0
    else:
      try:
        self.level2_interval = int(Snaps.get(self.SnapshotGroup, "level2_interval"))
        if self.level2_interval % self.level1_interval != 0:
          LogWrite(self.Log, "ERROR", "level2_interval is not a multiple of level1_interval in {}".format(SnapsConfigFile))
          self.Exit()
      except:
        self.level2_interval = 0

    if self.level2_interval == 0:
      self.level3_interval = 0
    else:
      try:
        self.level3_interval = int(Snaps.get(self.SnapshotGroup, "level3_interval"))
        if self.level3_interval % self.level2_interval != 0:
          LogWrite(self.Log, "ERROR", "level3_interval is not a multiple of level2_interval in {}".format(SnapsConfigFile))
          self.Exit()
      except:
        self.level3_interval = 0

    return    

  # This function determines the snapshot number to be assigned to the next upcoming snapshot
  def next_snapshot_number(self, snapshot_list):
    number=1
    prevnumber=1
    latest = datetime.timedelta(days=10000)
    snapgroup_level_name = self.SnapshotGroup + ".L"
    newest = datetime.timedelta(days=10000)
    for i, snapshot in enumerate(snapshot_list):
      try:
        number = int(re.search('[0-9]+$', snapshot["name"]).group())
      except:
        number=number
      if number > prevnumber:
        prevnumber = number    
    number = prevnumber + 1
    if number == 10000:
      number = 1
    return number

  # This function determines now many seconds until the next snapshot
  def next_snapshot_interval(self, now, level, snapshot_list):
    utcnow = datetime.datetime.utcnow()
    created_timestamps = []
    # put in a really old timestamp to guarantee at least one exists (and is really old)
    created_timestamps.append(datetime.datetime.strptime("2014-10-15T11:15:00Z", "%Y-%m-%dT%H:%M:%SZ"))
    for i, snapshot in enumerate(snapshot_list):
      created_timestamps.append(datetime.datetime.strptime(snapshot["created"], "%Y-%m-%dT%H:%M:%SZ"))
    most_recent_created_snapshot_time = max(created_timestamps)
    # Get the interval time, in minutes, between each snapshot at the given level
    if level == 1:
      interval = self.level1_interval
    elif level == 2:
      if self.level2_interval > 0:
        interval = self.level2_interval
      else:
        interval = self.level1_interval
    else:
      if self.level3_interval > 0:
        interval = self.level3_interval
      else:
        interval = self.level1_interval
    age_of_most_recent_created_snapshot = utcnow - most_recent_created_snapshot_time
    # Convert to seconds
    interval_secs = interval * 60
    if interval >= 1440:  # if this is a snapshot that occurs infrequently
      if age_of_most_recent_created_snapshot.total_seconds() > interval_secs:
        # my most recent snapshot is very old (or doesn't exist). I want to take a snapshot within 24 hours based on the designated starttime
        interval = 1440
        interval_secs = interval * 60
      else:
        # my most recent snapshot is not too old. But I need to adjust the self.starttime to use the day of that most recent snapshot (if it is greater than a day old)
        if age_of_most_recent_created_snapshot.total_seconds() > 86400:
          self.starttime=self.starttime.replace(most_recent_created_snapshot_time.timetuple().tm_year,most_recent_created_snapshot_time.timetuple().tm_mon,most_recent_created_snapshot_time.timetuple().tm_mday)
    
    # Calculate the difference so we can compute the number of intervals
    difference = now - self.starttime
    # Calculate the count of intervals
    interval_count = difference.total_seconds() / interval_secs
    # Round up the interval count so we can calculate the next interval time
    interval_count_ceil = math.ceil(interval_count)
    # Calculate the seconds to the next interval from the starttime
    startseconds_from_starttime = interval_count_ceil * interval_secs
    # Calculate the number of seconds to the next interval from now
    sleepseconds = startseconds_from_starttime - difference.total_seconds()
    return sleepseconds

  # This function determines which level will be for the next snapshot
  def level_of_next_snapshot(self, snapshot_list):
    now = datetime.datetime.now()
    seconds_til_next_L1 = self.next_snapshot_interval(now, 1, snapshot_list)
    if self.level2_interval > 0:
      seconds_til_next_L2 = self.next_snapshot_interval(now, 2, snapshot_list)
    else:
      seconds_til_next_L2 = 999999999
    if self.level3_interval > 0:
      seconds_til_next_L3 = self.next_snapshot_interval(now, 3, snapshot_list)
    else:
      seconds_til_next_L3 = 999999999
    if seconds_til_next_L3 <= seconds_til_next_L1:
      level = 3
    elif seconds_til_next_L2 <= seconds_til_next_L1:
      level = 2
    else:
      level = 1
    return level

  # This function returns the list of pgroup snapshots on the array
  def get_pgroup_snapshots(self):
    snapshots = []
    try:
      snapshots = self.array.get_pgroup(self.SnapshotGroup, snap=True, transfer=True )
    except:
      LogWrite(self.Log, "ERROR", "Unable to list snapshots for snapshot group, {}. I will have trouble making new and expiring old snapshots.".format(self.SnapshotGroup))
      snapshots = []
    if type(snapshots).__name__ != 'list':
      snapshots = (snapshots, )
    return snapshots
  
  # This function returns the list of fully replicated snapshots for the snapshot group on the Target_Array (throws out snapshots currently replicating)
  def get_target_snapshots(self):
    snapshots = []
    try:
      snapshots = self.target_array.get_pgroup(self.TargetSnapshotGroup, snap=True, transfer=True)
    except:
      LogWrite(self.Log, "ERROR", "Unable to list target snapshots for snapshot group, {}. Will not be able to expire snapshots at target.".format(self.SnapshotGroup))

    if type(snapshots).__name__ != 'list':
      snapshots = (snapshots, )
    self.replicating = False
    self.TargetProgress = 1
    for i, snapshot in enumerate(snapshots):
      if snapshots[i]["progress"] != None and snapshot["progress"] < 1: # this snapshot is currently replicating
        del snapshots[i] # Remove this particular snapshot from the list since it is currently replicating
        self.replicating = True
        self.TargetProgress = snapshot["progress"]
    return snapshots

  def Exit(self):
    LogWrite(self.Log, "INFO", "{}: Exiting at {}".format(self.name, datetime.datetime.now()))
    self.Log.close()
    exit()

  # This function Updates the Protection Group with the various options designated in the config file for the SnapshotGroup
  def update_pgroup(self):
    ### Create the volumes list (list of volumes to protect according to the conf file options)
    allconnections = []
    self.volumes = []
    for i, host in enumerate(self.hosts):
      try:
        connections = self.array.list_host_connections(host)
      except:
        LogWrite(self.Log, "ERROR", "Unable to list volumes for host, {} in snapshot group, {}. Updates to pgroup volume list will not occur.".format(host, self.SnapshotGroup))
        return
      if type(connections).__name__ != 'list':
        connections = [connections, ]
      allconnections.extend(connections)
 
    for i, connection in enumerate(allconnections):
      exclude = False
      for j, exclusdestr in enumerate(self.vol_excludestr):
        if exclusdestr.upper() in connection["vol"].upper():
          exclude = True
      if not exclude:
        self.volumes.append(connection["vol"])
    try:
      array_volumes = self.array.list_volumes()
    except:
      LogWrite(self.Log, "ERROR", "Unable to list volumes for snapshot group, {}. Updates to pgroup volume list will not occur.".format(self.SnapshotGroup))
      return
    for i, volume in enumerate(array_volumes):

      include = False
      for j, includestr in enumerate(self.vol_includestr):
        if includestr.upper() in volume["name"].upper():
          include = True
      if include and self.volumes.count(volume["name"]) == 0: 
        exclude = False
        for j, exclusdestr in enumerate(self.vol_excludestr):
          if exclusdestr.upper() in volume["name"].upper():
            exclude = True
        if not exclude:
          self.volumes.append(volume["name"])
    vols=", ".join(self.volumes)
    try: 
      LogWrite(self.Log, "DEBUG", "Updating pgroup {}. With volume list {}.".format(self.SnapshotGroup,vols))
      pgroup = self.array.set_pgroup(self.SnapshotGroup, vollist=self.volumes)
    except:
      try:
        LogWrite(self.Log, "DEBUG", "Creating pgroup {}".format(self.SnapshotGroup))
        pgroup = self.array.create_pgroup(self.SnapshotGroup)
        LogWrite(self.Log, "DEBUG", "Updating pgroup {}. With volume list {}.".format(self.SnapshotGroup,vols))
        pgroup = self.array.set_pgroup(self.SnapshotGroup, vollist=self.volumes)
      except:
        LogWrite(self.Log, "ERROR", "Could not create pgroup {}. It probably has been destroyed and is pending eradication.".format(self.SnapshotGroup))
        self.Exit()
    return


  # This function takes the snapshot for a given SnapshotGroup, volume list, and level
  def takesnapshot(self):
    snapname = "L{}-{:>04}".format(self.level,self.number)
    self.snapscript = self.snapscript.replace("snapname",snapname,1)


    LogWrite(self.Log, "DEBUG", "snapscript={}  snaplevels = {}".format(self.snapscript, self.snapscript_levels))


    if self.snapscript != "" and self.snapscript_levels.count(str(self.level)) > 0:
      LogWrite(self.Log, "INFO", "{}: Calling external command, \"{}\"".format(self.SnapshotGroup, self.snapscript))
      os.system(self.snapscript)
    else:
      LogWrite(self.Log, "INFO", "{}: Creating {}.{}".format(self.SnapshotGroup, self.SnapshotGroup, snapname))
      replicate=self.replicate
      if self.replicate:
        target_snapshot_list = self.get_target_snapshots()
        if self.replicating:
          LogWrite(self.Log, "WARN", "{}: A previous snapshot is still replicating. Will not replicate this snapshot for {}".format(self.SnapshotGroup, snapname))
          LogWrite(self.Log, "WARN", "{}: progress {}".format(self.SnapshotGroup, self.TargetProgress))
          replicate=False
        else:
          replicate=True
      LogWrite(self.Log, "INFO", "create_pgroup_snapshot({}, snap=True, replicate={}, apply_retention=False, suffix={})".format(self.SnapshotGroup,replicate,snapname))
      try:
        snapshot = self.array.create_pgroup_snapshot(self.SnapshotGroup, snap=True, replicate=replicate, apply_retention=False, suffix=snapname)
      except:
        LogWrite(self.Log, "ERROR", "Problem taking snapshot for snapshot group {}!".format(self.SnapshotGroup))
    return

  # This function expires (eradicates) snapshots according to the configured retention schedule by level
  def expire_snapshot_level(self, array, snapshot_list, level, retention_time):
    snapgroup_level_name = self.SnapshotGroup + level
    utcnow = datetime.datetime.utcnow()
    try:
      snapshot_count=len(snapshot_list)
    except:
      snapshot_count=0
    for i, snapshot in enumerate(snapshot_list):
      SnapTitle = snapshot["name"]
      if snapgroup_level_name in SnapTitle:
        created = datetime.datetime.strptime(snapshot["created"], "%Y-%m-%dT%H:%M:%SZ")
        age = utcnow - created
        replicating = False
        if snapshot["progress"] != None and snapshot["progress"] < 1:
          replicating = True
        if age >= retention_time and not replicating: # Only expire snapshots that are both older than the retention period and are not currently replicating
          if snapshot["name"] in self.noexpirelist:
            LogWrite(self.Log, "INFO", "{}: Will not expire snapshot, {} whose age is {} which is older than retention of {} since it is in the noexpirelist".format(self.SnapshotGroup, snapshot["name"], age, retention_time))
          else:
            LogWrite(self.Log, "INFO", "{}: Expiring snapshot, {} whose age is {} which is older than retention of {}".format(self.SnapshotGroup, snapshot["name"], age, retention_time))
            if snapshot_count == 1:
              LogWrite(self.Log, "WARN", "{}: Will Not Expire snapshot, {}, because it is the last remaining snapshot available at the target. ".format(self.SnapshotGroup, snapshot["name"]))
            else:
              try:
                snapshot = array.destroy_pgroup(snapshot["name"])
                if Snaps.has_section("default") and Snaps.has_option("default", "eradicate") and Snaps.getboolean("default", "eradicate"):
                  snapshot = array.eradicate_pgroup(snapshot["name"])
                snapshot_count=snapshot_count-1
              except:
                LogWrite(self.Log, "ERROR", "Expiration failed.")
    return

  # This function determines the retention periods and calls the above function to expire each snapshot by level
  def expire_snapshots(self):
    if self.level3_local_retention > 0:
      snapshot_list = self.get_pgroup_snapshots()
      retention_time = datetime.timedelta(minutes=self.level3_local_retention)
      self.expire_snapshot_level(self.array, snapshot_list, ".L3", retention_time)
    if self.level2_local_retention > 0:
      snapshot_list = self.get_pgroup_snapshots()
      retention_time = datetime.timedelta(minutes=self.level2_local_retention)
      self.expire_snapshot_level(self.array, snapshot_list, ".L2", retention_time)
    if self.level1_local_retention > 0:
      snapshot_list = self.get_pgroup_snapshots()
      retention_time = datetime.timedelta(minutes=self.level1_local_retention)
      self.expire_snapshot_level(self.array, snapshot_list, ".L1", retention_time)

    if self.TargetArray != "":
      if self.level3_target_retention > 0:
        target_snapshot_list = self.get_target_snapshots()
        retention_time = datetime.timedelta(minutes=self.level3_target_retention)
        self.expire_snapshot_level(self.target_array, target_snapshot_list, ".L3", retention_time)
      if self.level2_target_retention > 0:
        target_snapshot_list = self.get_target_snapshots()
        retention_time = datetime.timedelta(minutes=self.level2_target_retention)
        self.expire_snapshot_level(self.target_array, target_snapshot_list, ".L2", retention_time)
      if self.level1_target_retention > 0:
        target_snapshot_list = self.get_target_snapshots()
        retention_time = datetime.timedelta(minutes=self.level1_target_retention)
        self.expire_snapshot_level(self.target_array, target_snapshot_list, ".L1", retention_time)
    return          

  # This function runs the thread. It stays in a loop until the SnapshotGroup is disabled or removed from the config
  def run(self):
    if self.ERROR:
      self.Exit()
    self.refresh_from_cfg()
    self.update_pgroup()
    now = datetime.datetime.now()
    LogWrite(self.Log, "INFO", "{}: Starting as threadID, {}, at {}".format(self.name, self.threadID, now))

    # Compute the number of seconds til the next snapshot
    snapshot_list = self.get_pgroup_snapshots()
    sleepseconds = self.next_snapshot_interval(now, 1, snapshot_list)
    next_interval = now + datetime.timedelta(seconds=sleepseconds)
    LogWrite(self.Log, "INFO",  "{}: The next snapshot interval will be {}, which is in {} seconds ... sleeping".format(self.SnapshotGroup,next_interval, sleepseconds))
    self.number = self.next_snapshot_number(snapshot_list)
    while Snaps.has_section(self.SnapshotGroup):
      # As long as the next snapshot is greater than 60 seconds, loop and sleep
      # This allows for checking if the config file has changed
      count = 0
      while sleepseconds > 60 and Snaps.has_section(self.SnapshotGroup):
        count += 1
        time.sleep(60)
        self.refresh_from_cfg()
        now = datetime.datetime.now()
        sleepseconds = self.next_snapshot_interval(now, 1, snapshot_list)
        if count >= 240:
          next_interval = now + datetime.timedelta(seconds=sleepseconds)
          LogWrite(self.Log, "INFO",  "{}: The next snapshot interval will be {}, which is in {} seconds ... sleeping".format(self.SnapshotGroup,next_interval, sleepseconds))
          count = 0

      # If we get here, then the next snapshot is less than 60 seconds away
      self.level = self.level_of_next_snapshot(snapshot_list)
      self.update_pgroup()
      now = datetime.datetime.now()
      sleepseconds = self.next_snapshot_interval(now, 1, snapshot_list)
      # Calculate the time it will be at the next interval
      next_interval = now + datetime.timedelta(seconds=sleepseconds)
      LogWrite(self.Log, "INFO", "{}: It is now {}. At the next interval it will be {}. Sleeping {} seconds".format(self.SnapshotGroup,now,next_interval,sleepseconds))
      time.sleep(sleepseconds)
      if not Snaps.has_section(self.SnapshotGroup):
        self.Exit()
      LogWrite(self.Log, "INFO", "{}: Taking Level {} snapshot at {}".format(self.SnapshotGroup,self.level, datetime.datetime.now()))
      self.takesnapshot()
      self.expire_snapshots()
      self.number = self.number + 1
      snapshot_list = self.get_pgroup_snapshots()
      now = datetime.datetime.now()
      sleepseconds = self.next_snapshot_interval(now, 1, snapshot_list)
      next_interval = now + datetime.timedelta(seconds=sleepseconds)
      LogWrite(self.Log, "INFO",  "{}: The next snapshot interval will be {}, which is in {} seconds ... sleeping".format(self.SnapshotGroup,next_interval, sleepseconds))
    self.Exit()
  ## END OF CLASS ##

def LogWrite(logfile, loglevel, logstring):
  now = datetime.datetime.fromtimestamp(round(time.time()))

  if loglevel == "DEBUG":
    try:
      if Snaps.has_section("default") and Snaps.has_option("default", "debug") and Snaps.getboolean("default", "debug"):
        logfile.write("{}: {:<5}: {}\n".format(now, loglevel, logstring))
    except:
      pass
  else:
    logfile.write("{}: {:<5}: {}\n".format(now, loglevel, logstring))
  return

################################################################################
### MAIN #####
################################################################################

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--cfg", help="Config File Name (default = /etc/snapsched.ini)")
parser.add_argument("-l", "--logdir", help="Log Directory (default = /var/snapsched)")
args = parser.parse_args()

Config = ConfigParser.SafeConfigParser()
if os.path.isfile("/etc/pure.ini"):
  pureini="/etc/pure.ini"
else:
  pureini="/usr/local/purestorage/purestorage/pure.ini"
Config.read(pureini)
Config.read(pureini)

if args.cfg == None:
  SnapsConfigFile = "/etc/snapsched.ini"
else:
  SnapsConfigFile = args.cfg

if args.logdir == None:
  LogDir = "/var/snapsched"
else:
  LogDir = args.logdir

Snaps = ConfigParser.SafeConfigParser()
Snaps.read(SnapsConfigFile)

threadID = 1
threads = {}
SnapshotGroups = []
try:
  os.mkdir(LogDir, 0700)
except:
  LogDir = LogDir
MainLogName = LogDir + "/" + "snapsched.main.log"
MainLog = open(MainLogName, 'w', 0)
LogWrite(MainLog, "INFO", "Starting")

# Create the threads, one per enabled SnapshotGroup/pgroup (but do not start them)
for SnapshotGroup in Snaps.sections():
  try:
    if Snaps.has_option(SnapshotGroup, "enabled") and Snaps.getboolean(SnapshotGroup, "enabled"):
      threads[SnapshotGroup] = SnapshotGroupThread(threadID, SnapshotGroup, SnapshotGroup)
      SnapshotGroups.append(SnapshotGroup)
      threadID += 1
  except:
    pass

# Start the SnapshotGroup threads that do not have the "enabled = False" setting
for i, SnapshotGroup in enumerate(SnapshotGroups):
  LogWrite(MainLog, "INFO", "Starting threadID, {}, for SnapshotGroup, {}".format(threads[SnapshotGroup].threadID, SnapshotGroup))
  threads[SnapshotGroup].start()
  time.sleep(1)

while True:
  time.sleep(5)
  SnapsRefresher = ConfigParser.SafeConfigParser()
  SnapsRefresher.read(SnapsConfigFile)
  Snaps = SnapsRefresher
  for SnapshotGroup in Snaps.sections():
    if threads.has_key(SnapshotGroup):
      if not threads[SnapshotGroup].isAlive():
        LogWrite(MainLog, "INFO", "Thread for SnapshotGroup, {} has stopped".format(SnapshotGroup))
        threads.pop(SnapshotGroup, None)
        SnapshotGroups.remove(SnapshotGroup)
    else:
      try:
        if Snaps.has_option(SnapshotGroup, "enabled") and Snaps.getboolean(SnapshotGroup, "enabled"):
          # This is a new SnapshotGroup
          threads[SnapshotGroup] = SnapshotGroupThread(threadID, SnapshotGroup, SnapshotGroup) 
          SnapshotGroups.append(SnapshotGroup)
          LogWrite(MainLog, "INFO", "Starting ThreadID, {}, for SnapshotGroup, {}".format(threads[SnapshotGroup].threadID, SnapshotGroup))
          threads[SnapshotGroup].start()
          threadID += 1
      except:
        pass
  for SnapshotGroup in SnapshotGroups:
    if threads.has_key(SnapshotGroup):
      if not threads[SnapshotGroup].isAlive():
        LogWrite(MainLog, "INFO", "Thread for SnapshotGroup, {} has stopped".format(SnapshotGroup))
        threads.pop(SnapshotGroup, None)
        SnapshotGroups.remove(SnapshotGroup)
    else:
      LogWrite(MainLog, "INFO", "Thread for SnapshotGroup, {} has stopped".format(SnapshotGroup))
      SnapshotGroups.remove(SnapshotGroup)
