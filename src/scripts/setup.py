import argparse;
import getpass;
import paramiko;

def get_file_name(path):
  return path.split("/")[-1];

def copy_files(remote, username, password, source_paths, rwd):
  """ Copies a bunch of files from host to the remote machine. 
      The files are copied inside the directory pointed by the rwd (which
      is an abbreviation for remote working directory)"""

  # Remote this.
  source_paths.append('run.py');

  ssh = paramiko.SSHClient();

  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
 
  ssh.connect(remote, username=username, password=password);

  sftp = ssh.open_sftp()

  # Create the remote directory, if it doesn't exist.
  try:
    sftp.mkdir(rwd);
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
  parser.add_argument("--fs_master_binary_path", type=str);
  parser.add_argument("--fs_master_ip", type=str);
  parser.add_argument("--mr_master_binary_path", type=str);
  parser.add_argument("--mr_master_ip", type=str);
  parser.add_argument("--mr_slave_ips", type=str,
      help='File containing list of IPs for running mr slave instances');
  parser.add_argument("--fs_slave_ips", type=str,
      help='File containing list of IPs for running fs slave instances.');
  parser.add_argument("--mr_slave_binary_path", type=str);
  parser.add_argument("--fs_slave_binary_path", type=str);
  parser.add_argument("--rwd", type=str, help='Remote server working directory i.e.'
     + 'the directory into which the files have to be copied.');
  return parser.parse_args();

def copy_fs_master_binary(args, username, password):
  print "Copying binary files to the FS master: {0}".format(args.fs_master_ip);
  copy_files(args.fs_master_ip, username, password, [args.fs_master_binary_path,
      'fs_run_master.sh'], args.rwd);

def get_ips_from_file(filename):
  ips = [];
  f = open(filename, 'r');

  for ip in f:
   ips.append(ip.strip());
  
  return ips;

def copy_fs_slave_binary(args, username, password):
  slave_ips = get_ips_from_file(args.fs_slave_ips);
  for ip in slave_ips:
    print "Copying binary files to the FS slaves: {0}".format(ip);
    copy_files(ip, username, password, [args.fs_slave_binary_path,
        'fs_run_slave.sh'], args.rwd);

def copy_mr_master_binary(args, username, password):
  print "Copying binary files to the MR master: {0}".format(args.mr_master_ip);
  copy_files(args.mr_master_ip, username, password, [args.mr_master_binary_path,
       'mr_run_master.sh'], args.rwd);

def copy_mr_slave_binary(args, username, password):
  slave_ips = get_ips_from_file(args.mr_slave_ips);
  for ip in slave_ips:
    ip = ip.strip();
    print "Copying binary files to the MR slaves: {0}".format(ip);
    copy_files(ip, username, password, [args.mr_slave_binary_path,
        'mr_run_slave.sh'], args.rwd);

def setup(rwd, setupfile, remote, username, password):
  """Makes an ssh connection and runs the setup script on the remote
     host."""
  ssh = paramiko.SSHClient();

  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
 
  ssh.connect(remote, username=username, password=password);
 
  # Change to a new directory and run the shell script.
  cmd = 'cd {0}; sh {1}'.format(rwd, setupfile);
  
  print "Running command {0} on the remote server.".format(cmd);
 
  ssh.exec_command(cmd); 
  
  ssh.close(); 

def setup_fs_master(args, username, password):
  setup(args.rwd, 'fs_run_master.sh', args.fs_master_ip, username, password);

def setup_fs_slaves(args, username, password):
  ips = get_ips_from_file(args.fs_slave_ips);
  for ip in ips:  
    setup(args.rwd, 'fs_run_slaves.sh', ip, username, password);

def setup_mr_master(args, username, password):
  setup(args.rwd, 'mr_run_master.sh', args.mr_master_ip, username, password);

def setup_mr_slaves(args, username, password):
  ips = get_ips_from_file(args.mr_slave_ips);
  for ip in ips:
    setup(args.rwd, 'mr_run_slaves.sh', ip, username, password);
 
def main():
  args = set_up_args();
   
  [username, password] = get_login_credentials();

  copy_fs_master_binary(args, username, password);
  copy_fs_slave_binary(args, username, password);
  copy_mr_master_binary(args, username, password);
  copy_mr_slave_binary(args, username, password);
  
  setup_fs_master(args, username, password);
  setup_fs_slaves(args, username, password);
  
  setup_mr_master(args, username, password);
  setup_mr_slaves(args, username, password);
  
  print "** Copied relevant binaries **";

if __name__ == "__main__":
   main(); 
