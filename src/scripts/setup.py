import argparse;
import getpass;
import paramiko;

def get_file_name(path):
  return path.split("/")[-1];

def copy_files(remote, username, password, source_paths, rwd, dfs_dir):
  """ Copies a bunch of files from host to the remote machine. 
      The files are copied inside the directory pointed by the rwd (which
      is an abbreviation for remote working directory)"""

  # Remove this.
  source_paths.append('run.py');

  ssh = paramiko.SSHClient();

  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
 
  print remote; 
  ssh.connect(remote, username=username, password=password);

  sftp = ssh.open_sftp()

  # Create the remote directory, if it doesn't exist.
  try:
    sftp.mkdir(rwd);
    sftp.mkdir(dfs_dir);
  except IOError:  
    print "Working directory exists. Skipping..";

  for source_path in source_paths:
    destination = rwd;

    if (not source_path.startswith("/")):
      destination += "/";

    destination = destination + get_file_name(source_path);
    print "Destination file path: {0}".format(destination);
    sftp.put(source_path, destination);

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
  # Ports at which the service runs. Assumed to be same for all master and slaves
  parser.add_argument("--port", type=int);
  parser.add_argument("--dfs_base_path", type=str, default='/tmp/mrikav-ank-dfs/');
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
       'run_master.sh'], args.rwd, args.dfs_base_path);

def copy_slave_binary(args, username, password):
  slave_ips = get_ips_from_file(args.slave_ips);
  for ip in slave_ips:
    ip = ip.strip();
    print "Copying binary files to the MR slaves: {0}".format(ip);
    copy_files(ip, username, password, [args.slave_binary_path,
        'run_slave.sh'], args.rwd, args.dfs_base_path);

def setup(rwd, setupfile, remote, username, password, fileArgs):
  """Makes an ssh connection and runs the setup script on the remote
     host."""
  ssh = paramiko.SSHClient();

  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
 
  ssh.connect(remote, username=username, password=password);
 
  # Change to a new directory and run the shell script.
  cmd = 'cd {0}; sh {1} {2}'.format(rwd, setupfile, fileArgs);
  
  print "Running command {0} on the remote server.".format(cmd);
 
  ssh.exec_command(cmd); 
  
  ssh.close(); 

def setup_master(args, username, password):
  script_args= "{0} {1}".format(args.port, args.slave_ips);
  setup(args.rwd, 'run_master.sh', args.master_ip, username, password, script_args);

def setup_slaves(args, username, password):
  script_args = "{0} {1}".format(args.port, args.master_ip); 
  ips = get_ips_from_file(args.slave_ips);
  for ip in ips:
    setup(args.rwd, 'run_slaves.sh', ip, username, password, script_args);
 
def main():
  args = set_up_args();
   
  [username, password] = get_login_credentials();

  copy_master_binary(args, username, password);
  copy_slave_binary(args, username, password);
  
  setup_master(args, username, password);
  setup_slaves(args, username, password);
  
  print "** Copied relevant binaries **";

if __name__ == "__main__":
   main(); 
