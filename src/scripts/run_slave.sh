# This file should contain the command line for starting MR slave process.
# Usage $1=master_ip $2=port. Port is the port at which the service runs.
nohup python run.py $1 $2> /dev/null 2> /dev/null  &
