cd common
mvn clean install
cd ..
cd mrdfs
mvn clean install
cd ..
cd mrmaster
mvn clean
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
cd ..
cd mrnodemanager
mvn clean
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
cd ..
cd mrmap
mvn clean
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
cd ..
cd mrclient
mvn clean
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
cd ..
cd mrreduce
mvn clean
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
