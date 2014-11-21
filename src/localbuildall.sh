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
cd .. ;
cd .. ;
