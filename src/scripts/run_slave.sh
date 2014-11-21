# This file should contain the command line for starting MR slave process.
nohup java -jar mrnodemanager-1.0-SNAPSHOT-jar-with-dependencies.jar -config $1 -port $2> slave.stdout 2> slave.stderr  &
chmod 777 slave.stdout
chmod 777 slave.stderr
