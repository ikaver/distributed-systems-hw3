import argparse;
import json;

CONFIG = "config";
MASTER_HOST = "master-host";
PARTICIPANTS = "participants";

def setup_args():
  parser = argparse.ArgumentParser();
  parser.add_argument("--json_file", type=str, required=True);
  return parser.parse_args();

def validate_participants(participants):
  if len(participants) < 1:
    print "Atleast one participant must be specified";
    return False;
  
  for participant in participants:
    if "ip" not in participant:
      print "IP should be specified for each participant";
      return False;
    if "port" not in participant or not isinstance(participant["port"], int) or participant["port"] < 0:
      print "Port for each participant must be greater than zero.";
      return False;
  return True;
    
def validate_json(content):
  d = json.loads(content);
  
  if len(d) != 1 and CONFIG not in d:
    print "Only and only one configuration should be specified in the json";
    return False;
  
  integer_children = [
        "master-port",
        "max-retries-before-job-failure",
        "time-to-check-for-job-state",
        "time-to-check-for-nodes-state",
        "time-to-check-for-data-nodes-state",
        "workers-per-node",
        "replication-factor",
        "chunk-size-in-MB",
        "max-dfs-read-retries"];

  if MASTER_HOST not in d[CONFIG]:
    print "Master host should be specified";
    return False;
 
  for child in integer_children:
    if child not in d[CONFIG]:
      print "{0} not specified under config".format(child);
      return False;
      value = d[CONFIG][child];
      if not isinstance(int, value) or value <= 0:
        print "{0} should be an integer > 0".format(child);
        return False;
   
  return validate_participants(d[CONFIG]["participants"]);

def read_file(filename):
  f = open(filename);
  contents = f.read();
  return contents.replace('\n','');

def main():
  args = setup_args();
  if (validate_json(read_file(args.json_file))):
    print "JSON validated successfully :)";
  else:
    print "JSON parsing was not successful. :(";

if __name__ == "__main__":
  main();
 
