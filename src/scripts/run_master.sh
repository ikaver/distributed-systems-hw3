# This file should contain the command line of the binary which should start the master.
# Port at which the master should run.
nohup python run.py $1 $2> /dev/null 2> /dev/null  &
