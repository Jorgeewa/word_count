import os
import glob
from collections import defaultdict, Counter

file = "inputs/my_test.txt"

size = os.path.getsize(file)

'''partition = 4
partition_size = size // partition
f = open(file, "r")
f.seek(2 * partition_size)
print(f.read(partition_size))
exit(0)
for i in range(partition):
    #print("where is it now", f.tell())
    #f.seek(i * partition_size)
    data = f.read(partition_size)
    line_by_line = data.split(" ")
    print(line_by_line)'''
    
    
    
    
'''f = open("inputs/my_test.txt", "r")
f.seek(int(2 * 24))
data = f.read(24)
line_by_line = data.split()
print(line_by_line)

for text in line_by_line:
    index = ord(text[0]) % 8
    print(text, text[0], ord(text[0]) % 8)'''

count = defaultdict(int)
for name in glob.glob('intermediate/*[0]'):
    print(name)
    f = open(name, "r")
    data = f.read()
    data = data.split()
    for text in data:
        count[text] +=1

for k,v in count.items():  
    if k == "profile":
        print(f"{k} {v}")
        


