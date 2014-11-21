cd common
/usr/local/netbeans-7.2.1/java/maven/bin/mvn clean install
cd ..
cd mrdfs
/usr/local/netbeans-7.2.1/java/maven/bin/mvn clean install
cd ..
cd mrmaster
/usr/local/netbeans-7.2.1/java/maven/bin/mvn clean
/usr/local/netbeans-7.2.1/java/maven/bin/mvn assembly:assembly -DdescriptorId=jar-with-dependencies
cd ..
cd mrnodemanager
/usr/local/netbeans-7.2.1/java/maven/bin/mvn clean
/usr/local/netbeans-7.2.1/java/maven/bin/mvn assembly:assembly -DdescriptorId=jar-with-dependencies
cd ..
cd mrmap
/usr/local/netbeans-7.2.1/java/maven/bin/mvn clean
/usr/local/netbeans-7.2.1/java/maven/bin/mvn assembly:assembly -DdescriptorId=jar-with-dependencies
cd ..
cd mrclient
/usr/local/netbeans-7.2.1/java/maven/bin/mvn clean
/usr/local/netbeans-7.2.1/java/maven/bin/mvn assembly:assembly -DdescriptorId=jar-with-dependencies
cd ..
cd mrreduce
/usr/local/netbeans-7.2.1/java/maven/bin/mvn clean
/usr/local/netbeans-7.2.1/java/maven/bin/mvn assembly:assembly -DdescriptorId=jar-with-dependencies
cd ..;
cd scripts;
cd ecdsa-0.11/ ;
python setup.py build ;
cp -r build/lib/ecdsa ../ ;
cd .. ;
cd pycrypto-2.6.1/ ;
python setup.py build ; 
cd build;
cd lib* ;
cp -r Crypto ../../../
cd .. ;
cd .. ;
cd .. ;
cd paramiko_dir ;
python setup.py build ; 
cp -r build/lib/paramiko ../ ; 
cd ..;
cd ..;
