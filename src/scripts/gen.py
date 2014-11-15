import string
import random

for j in range(0,4000000):
  print ''.join(random.choice(string.ascii_uppercase) for i in range(32))

