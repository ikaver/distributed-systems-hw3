# Start the MR Master Service.
java -jar mrnodemanager/target/mrnodemanager-1.0-SNAPSHOT-jar-with-dependencies.jar -port 3000 -masterIP localhost -masterPort 3001 &
java -jar mrmaster/target/mrmaster-1.0-SNAPSHOT-jar-with-dependencies.jar -port 3001 -slaves localhost &
