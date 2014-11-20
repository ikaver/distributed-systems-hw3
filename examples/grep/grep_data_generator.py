import string
import random
from optparse import OptionParser


parser = OptionParser(usage='Grep random file generator.\n\nUsage: %prog [options]\n    ')
parser.add_option('-p', '--pattern', action='store', type='string', \
        dest='pattern', help='Pattern that will be generated on some lines')
parser.add_option('-r', '--recordsize', action='store', type='int', \
        dest='record_size', help='Record size (length of each line)')
parser.add_option('-n', '--records', action='store', type='int', \
        dest='num_records', help='Amount of records in file')
options, args = parser.parse_args()
    
pattern = options.pattern
record_size = options.record_size
num_records = options.num_records

# returns a random string of length x.
def random_string(x):
  return ''.join(random.choice(string.ascii_uppercase) for i in range(x))

T = record_size - len(pattern);

for j in range(0,num_records):
  if random.randint(1,10) == 9:
   L = random.randint(2, T-1);
   R = T - L;
   print random_string(L) + pattern + random_string(R);
  else:
   print random_string(record_size);
