import argparse;
import getpass;
import json;
import os;
import paramiko;
import sys;

# Importing user defined modules.
import json_validator;

def get_file_name(path):
  return path.split("/")[-1];

def create_dir(sftp, dir_path): 
  # Create the remote directory, if it doesn't exist.
  try:
    sftp.mkdir(dir_path);
    sftp.chmod(dir_path, 0777);
  except IOError: 
    print "Working directory exists. Skipping..";

def copy_files(remote, username, password, source_paths, rwd, dfs_dir, localfs_dir):
  """ Copies a bunch of files from host to the remote machine. 
      The files are copied inside the directory pointed by the rwd (which
      is an abbreviation for remote working directory)"""

  ssh = paramiko.SSHClient();

  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
 
  print remote; 
  ssh.connect(remote, username=username, password=password);

  sftp = ssh.open_sftp()

  create_dir(sftp, rwd);
  create_dir(sftp, dfs_dir);
  create_dir(sftp, localfs_dir);

  for source_path in source_paths:
    destination = rwd;

    if (not rwd.startswith("/")):
      destination += "/";

    destination = destination + get_file_name(source_path);
    print "Destination file path: {0}".format(destination);
    sftp.put(source_path, destination);
    try:
      sftp.chmod(destination, 0777);
    except IOError:
      print "Permission denied exception while changing file permissions. Ideally, this shouldn't affect system functioning.";
  sftp.close();

 
def get_login_credentials():
  """Gets login credentials from the system administrator. Username is the
   andrew id without @andrew.cmu.edu suffix and password is the andrew 
   password"""
  username = raw_input('Andrew ID: ');
  password = getpass.getpass(prompt='Andrew Password: ');
  return [username,password];

def set_up_args():
  parser = argparse.ArgumentParser();
  parser.add_argument("--master_binary_path", type=str);
  parser.add_argument("--slave_binary_path", type=str);
  parser.add_argument("--mr_map_binary_path", type=str);
  parser.add_argument("--mr_reduce_binary_path", type=str);

  parser.add_argument("--kill_all", type=bool, default=False);
  parser.add_argument("--config", type=str, default=str);
  parser.add_argument("--skip_copy", type=bool, default=False);

  parser.add_argument("--compile_local", type=bool, default=False);
  parser.add_argument("--compile_ghc", type=bool, default=False);
 
  # Cleanup remote directories which are created.
  parser.add_argument("--cleanup", type=bool, default=False);

  parser.add_argument("--dfs_base_path", type=str, default='/tmp/ankitaga-ikaveror-mr-dfs/');
  parser.add_argument("--localfs_base_path", type=str, default='/tmp/ankitaga-ikaveror-mr-local');
  parser.add_argument("--rwd", type=str, help='Remote server working directory i.e.'
     + 'the directory into which the files have to be copied.');
  return parser.parse_args();

def copy_master_binary(args, username, password, js):
  master = get_master_ip(js);
  print "Copying binary files to the MR master: {0}".format(master);
  copy_files(master, username, password, [args.master_binary_path,
       'run_master.sh', args.config], args.rwd, args.dfs_base_path, args.localfs_base_path);

def copy_slave_binary(args, username, password, js):
  slave_ips = get_slaves(js);
  for ip in slave_ips:
    ip = ip.strip();
    print "Copying binary files to the MR slaves: {0}".format(ip);
    copy_files(ip, username, password, [args.slave_binary_path,
        'run_slave.sh', args.config, args.mr_map_binary_path, args.mr_reduce_binary_path],
        args.rwd, args.dfs_base_path, args.localfs_base_path);

def setup(rwd, setupfile, remote, username, password, fileArgs):
  """Makes an ssh connection and runs the setup script on the remote
     host."""
  ssh = paramiko.SSHClient();

  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
 
  ssh.connect(remote, username=username, password=password);
 
  # Change to a new directory and run the shell script.
  cmd = 'cd {0}; sh {1} {2}'.format(rwd, setupfile, fileArgs);
  
  print "Running command {0} on the remote server: {1}.".format(cmd, remote);
 
  ssh.exec_command(cmd); 
  
  ssh.close(); 

def kill(remote, username, password):
  ssh = paramiko.SSHClient();
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
  ssh.connect(remote, username=username, password=password);
  cmd = "pkill -f 'java -jar'";
  ssh.exec_command(cmd);
  ssh.close();  

def setup_master(args, username, password, js):
  setup(args.rwd, 'run_master.sh', get_master_ip(js), username, password, args.config);

def setup_slaves(args, username, password, js):
  ips = get_slaves(js);
  for ip in ips:
    script_args = "{0} {1}".format(args.config, get_port_for_slave(js, ip));
    print "Arguments" + script_args;
    setup(args.rwd, 'run_slave.sh', ip, username, password, script_args);
 
def compile(args):
  script_file = '';
  if (args.compile_local):
    script_file = 'localbuildall.sh';
  elif(args.compile_ghc):
    script_file = 'ghcbuildall.sh';
  else:
    print "Skipping compilation step.";
    return;

  cmd = 'cd ..; sh {0}; cd scripts/'.format(script_file);
  print "Executing command {0}".format(cmd);
  os.system(cmd);

def cleanup_remote(remote, dirs, username, password): 
  ssh = paramiko.SSHClient();
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
  ssh.connect(remote, username=username, password=password);
  sftp = ssh.open_sftp()
 
  print dirs;
  print dirs[0]; 
  for d in dirs:
    try:
      lf = sftp.listdir(str(d));
      for f in lf:
        try:
          print "Attempting to remove file: {0} on the remote server: {1}".format(d+f, remote);
          sftp.remove(d+f)
        except:
          print "Error while trying to delete remote file {0}".format(d+f);
    except:
      print "Error while trying to list directory: {0} for the remote ip: {1}".format(d, remote);
    try:
      print "Attempting to delete the directory: {0} on remote {1}".format(d, remote);
      sftp.rmdir(d);
    except:
      print "Unable to delete remote directory. Check if you are running as owner.";

def cleanup(args, username, password, js): 
  slaves = get_slaves(js);
  master = get_master_ip(js);
  dirs = [args.rwd, args.dfs_base_path, args.localfs_base_path];
  cleanup_remote(master, dirs, username, password);
  
  for slave in slaves:
    cleanup_remote(slave, dirs, username, password);

def get_master_ip(json):
  return json[json_validator.CONFIG][json_validator.MASTER_HOST];

# Returns a list of slaves.
def get_slaves(js):
  nodes = js[json_validator.CONFIG][json_validator.PARTICIPANTS];
  ip = [];
  for node in nodes:
    ip.append(node["ip"]);
  return ip;

def get_port_for_slave(js, ip): 
  nodes = js[json_validator.CONFIG][json_validator.PARTICIPANTS];
  print "Trying to locate port for IP {0}".format(ip);
  for node in nodes:
    print "Checking against IP: {0}".format(node["ip"]);
    if node["ip"] == ip:
      print node["port"];
      return node["port"];
  print "Error locating port for the remote slave {0}".format(ip);

def main():
  args = set_up_args();
  if not json_validator.validate_json(json_validator.read_file(args.config)):
    print "Error validating JSON";
    return;
  js = json.loads(json_validator.read_file(args.config));
  [username, password] = get_login_credentials();
  compile(args);

  if(args.cleanup):
    cleanup(args, username, password, js);
    print "Cleaned up any artificats of previous run by the user:{0}".format(username);

  if (args.kill_all):
    print "** Terminating the system **";
    kill(get_master_ip(js), username, password);
    ips = get_slaves(js);
    for ip in ips:
      kill(ip, username, password);
    print "** System termination successful **";  
  else:
    if not args.skip_copy:
      copy_master_binary(args, username, password, js);
      copy_slave_binary(args, username, password, js);
      print "** Copied relevant binaries **";
    else:
      print "** Skipping copy step **";

    setup_slaves(args, username, password, js);
    setup_master(args, username, password, js);
    print "** The system is up and running **";
if __name__ == "__main__":
   main(); 
