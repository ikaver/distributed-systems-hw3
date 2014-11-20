# This file should contain the command line of the binary which should start the master.
# Port at which the master should run.
nohup java -jar mrmaster-1.0-SNAPSHOT-jar-with-dependencies.jar -port $1 -slaves $2> master.stdout 2> master.stderr  &
chmod 777 master.stdout
chmod 777 master.stderr
