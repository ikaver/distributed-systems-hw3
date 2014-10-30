import argparse;
import getpass;
import paramiko;

def copy_files(remote, username, password, source_paths):
  """ Copies a bunch of files from host to the remote machine. 
   Parameter source_paths is the location of the files on the 'local' host.
   The files are copied at the same path inside the /tmp of the remote host.""" 

  ssh = paramiko.SSHClient();

  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy());
 
  ssh.connect(remote, username=username, password=password);

  sftp = ssh.open_sftp()

  for path in source_paths:
    sftp.put(path, path);

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
  return parser.parse_args();

def copy_fs_master_binary(args, username, password):
  print "Copying binary files to the FS master: {0}".format(args.fs_master_ip);
  copy_files(args.fs_master_ip, username, password, [args.fs_master_binary_path]);

def copy_fs_slave_binary(args, username, password):
  slave_ips = open(args.fs_slave_ips, 'r');
  for ip in slave_ips:
    ip = ip.strip();
    print "Copying binary files to the FS slaves: {0}".format(ip);
    copy_files(ip, username, password, [args.fs_slave_binary_path]);

def copy_mr_master_binary(args, username, password):
  print "Copying binary files to the MR master: {0}".format(args.mr_master_ip);
  copy_files(args.mr_master_ip, username, password, [args.mr_master_binary_path]);

def copy_mr_slave_binary(args, username, password):
  slave_ips = open(args.mr_slave_ips, 'r');
  for ip in slave_ips:
    ip = ip.strip();
    print "Copying binary files to the MR slaves: {0}".format(ip);
    copy_files(ip, username, password, [args.mr_slave_binary_path]);

def main():
  args = set_up_args();
   
  [username, password] = get_login_credentials();

  copy_fs_master_binary(args, username, password);
  copy_fs_slave_binary(args, username, password);
  copy_mr_master_binary(args, username, password);
  copy_mr_slave_binary(args, username, password);
  
  print "** Copied relevant binaries **";

if __name__ == "__main__":
   main(); 
