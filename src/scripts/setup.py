import argparse;
import getpass;
import os;
import paramiko;
import sys;


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
  parser.add_argument("--master_ip", type=str);
  parser.add_argument("--slave_ips", type=str,
      help='File containing list of IPs for running slave instances, each of which'
           + ' is separated by a newline character');
  parser.add_argument("--slave_binary_path", type=str);
  parser.add_argument("--mr_map_binary_path", type=str);
  parser.add_argument("--mr_reduce_binary_path", type=str);

  parser.add_argument("--master_port", type=int, default=3001);
  parser.add_argument("--slave_port", type=int, default=3000);
  
  parser.add_argument("--kill_all", type=bool, default=False);

  parser.add_argument("--skip_copy", type=bool, default=False);

  parser.add_argument("--compile_local", type=bool, default=False);
  parser.add_argument("--compile_ghc", type=bool, default=False);

  parser.add_argument("--dfs_base_path", type=str, default='/tmp/mrikav-ank-dfs/');
  parser.add_argument("--localfs_base_path", type=str, default='/tmp/mrikav-ank-local');
  parser.add_argument("--rwd", type=str, help='Remote server working directory i.e.'
     + 'the directory into which the files have to be copied.');
  return parser.parse_args();

def get_ips_from_file(filename):
  ips = [];
  f = open(filename, 'r');

  for ip in f:
   ips.append(ip.strip());
  
  return ips;

def copy_master_binary(args, username, password):
  print "Copying binary files to the MR master: {0}".format(args.master_ip);
  copy_files(args.master_ip, username, password, [args.master_binary_path,
       'run_master.sh'], args.rwd, args.dfs_base_path, args.localfs_base_path);

def copy_slave_binary(args, username, password):
  slave_ips = get_ips_from_file(args.slave_ips);
  for ip in slave_ips:
    ip = ip.strip();
    print "Copying binary files to the MR slaves: {0}".format(ip);
    copy_files(ip, username, password, [args.slave_binary_path,
        'run_slave.sh', args.mr_map_binary_path, args.mr_reduce_binary_path],
        args.rwd, args.dfs_base_path, args.localfs_base_path);

def setup(rwd, setupfile, remote, username, password, fileArgs):
  """Makes an ssh connection and runs the setup script on the remote
     host."""
  ssh = paramiko.SSHClient();

  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
 
  ssh.connect(remote, username=username, password=password);
 
  # Change to a new directory and run the shell script.
  cmd = 'cd {0}; sh {1} {2}'.format(rwd, setupfile, fileArgs);
  
  print "Running command {0} on the remote server{1}.".format(cmd, remote);
 
  ssh.exec_command(cmd); 
  
  ssh.close(); 

def kill(remote, username, password):
  ssh = paramiko.SSHClient();
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
  ssh.connect(remote, username=username, password=password);
  cmd = "pkill -f 'java -jar'";
  ssh.exec_command(cmd);
  ssh.close();  

def setup_master(args, username, password):
  csv_list = ','.join(str(ip) for ip in get_ips_from_file(args.slave_ips));
  script_args= "{0} {1}".format(args.master_port, csv_list);
  setup(args.rwd, 'run_master.sh', args.master_ip, username, password, script_args);

def setup_slaves(args, username, password):
  ips = get_ips_from_file(args.slave_ips);
  for ip in ips:
    script_args = "{0} {1} {2}".format(args.slave_port,args.master_ip , args.master_port); 
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

def main():
  args = set_up_args(); 
  [username, password] = get_login_credentials();
  compile(args);

  if (args.kill_all):
    print "** Terminating the system **";
    kill(args.master_ip, username, password);
    ips = get_ips_from_file(args.slave_ips);
    for ip in ips:
      kill(ip, username, password);
    print "** System termination successful **";  
  else:
    if not args.skip_copy:
      copy_master_binary(args, username, password);
      copy_slave_binary(args, username, password);
      print "** Copied relevant binaries **";
    else:
      print "** Skipping copy step **";

    setup_slaves(args, username, password);
    setup_master(args, username, password);
    print "** The system is up and running **";

if __name__ == "__main__":
   main(); 
