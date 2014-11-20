import string
import random

# returns a random string of length x.
def random_string(x):
  return ''.join(random.choice(string.ascii_uppercase) for i in range(x))

wanted_word = raw_input('Grep pattern: ')
record_size = int(raw_input('Record size: '))
num_records = int(raw_input('Number of records: '))

T = record_size - len(wanted_word);

for j in range(0,num_records):
  if random.randint(1,10) == 9:
   L = random.randint(2, T-1);
   R = T - L;
   print random_string(L) + wanted_word + random_string(R);
  else:
   print random_string(record_size);
