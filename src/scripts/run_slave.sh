# This file should contain the command line for starting MR slave process.
# Usage $1=port $2=master_ip $3=master_port. Port is the port at which the service runs.
nohup java -jar mrnodemanager-1.0-SNAPSHOT-jar-with-dependencies.jar -port $1 -masterIP $2 -masterPort $3> slave.stdout 2> slave.stderr  &
