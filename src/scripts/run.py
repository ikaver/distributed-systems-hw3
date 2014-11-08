import time;
import sys;


i = 0;

#write the args to a random file just for the
# sake of testing.
f = open("some_random_file.txt", 'w');
f.write(sys.argv[0]);
f.write(sys.argv[1]);
f.close();

while i < 1000:
 print "Alive!";
 time.sleep(1)
 i += 1;
