import string
import random
from optparse import OptionParser


parser = OptionParser(usage='Word count random file generator.\n\nUsage: %prog [options]\n    ')
parser.add_option('-r', '--recordsize', action='store', type='int', \
        dest='record_size', help='Record size (length of each line)')
parser.add_option('-n', '--records', action='store', type='int', \
        dest='num_records', help='Amount of records in file')
options, args = parser.parse_args()
    
record_size = options.record_size
num_records = options.num_records

# returns a random string of length x.
def random_string(x):
  return ''.join(random.choice('AB') for i in range(x))

for j in range(0,num_records):
    print random_string(record_size);
