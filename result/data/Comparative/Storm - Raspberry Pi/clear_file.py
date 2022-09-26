#this py clear outpud data to count elements

import re
import sys

if(len(sys.argv)>1):
    source = sys.argv[1]
    print("source: "+source)
else:
    print("do something as 'python clear_data.py source.data [new name if u want]")

end_file = ""
if(len(sys.argv)>2):
    end_file = sys.argv[2]
    print("end_file: " + end_file)

file = open(source,"r")

if(end_file!=""):
    file2 = open(end_file, "a")
else:
    file2 = open("count.data", "a")
for line in file:
    r = re.compile(r'\d{2}\:\d{2}\:\d{2} (\d)*\n', re.UNICODE)
    for match in r.finditer(line):
        #print(match.start())
        #print(line[match.start():])
        file2.write(line[match.start():])

file.close()
file2.close()
