import re
import sys

source = "stats.data"
end_file = "out.data"
file = open(source,"r")
file2 = open(end_file, "a")

def remove_from_string(a,b):
    while(a.find(b)>=0):
        aux = a.find(b)
        a = a[:aux]+a[aux+len(b):]
    return a

count_elements = 0

p_cpu = 0.0
p_mem = 0.0
top = ''
MiB_mem = ''
MiB_swap = ''

for line in file:
    finded = False
    first = True
    top = re.compile(r'top - \d{2}\:\d{2}\:\d{2}')
    for match in top.finditer(line):
        #print(match.start())
        top = line[match.start()+6:]
        top = remove_from_string(top,"\x1b(B\x1b[m\x1b[39;49m\x1b(B\x1b[m\x1b[39;49m\x1b[K")
        #print(top)
        finded = True
        first = False
        if(not first):
            file2.write(top)
            file2.write(MiB_mem)
            file2.write(MiB_swap)
            file2.write('CPU '+str(p_cpu)+'\n')
            file2.write('MEM '+str(p_mem)+'\n')
        p_cpu = 0
        p_mem = 0
    tasks = re.compile(r'%Cpu/(s/):')
    for match in tasks.finditer(line):
        tasks_t = line[match.start():]
        tasks_t = remove_from_string(tasks_t, "(B")
        finded = True
    cpu = re.compile(r'Cpu')
    for match in cpu.finditer(line):
        cpu_p = line[match.start():]
        cpu_p = remove_from_string(cpu_p, "(B")
        finded = True
    mem = re.compile(r'MiB Mem :')
    for match in mem.finditer(line):
        MiB_mem = line[match.start():]
        MiB_mem = remove_from_string(MiB_mem, "(B")
        MiB_mem = remove_from_string(MiB_mem, "\x1b[m\x1b[39;49m")
        MiB_mem = remove_from_string(MiB_mem, ",\x1b[1m")
        MiB_mem = remove_from_string(MiB_mem, ".\x1b[1m")
        MiB_mem = remove_from_string(MiB_mem, "\x1b[K")
        MiB_mem = remove_from_string(MiB_mem, "\x1b[1m")
        #print(MiB_mem)
        finded = True
    swap = re.compile(r'MiB Swap:')
    for match in swap.finditer(line):
        MiB_swap = line[match.start():]
        MiB_swap = remove_from_string(MiB_swap, "(B")
        MiB_swap = remove_from_string(MiB_swap, "\x1b[m\x1b[39;49m")
        MiB_swap = remove_from_string(MiB_swap, ",\x1b[1m")
        MiB_swap = remove_from_string(MiB_swap, ".\x1b[1m")
        MiB_swap = remove_from_string(MiB_swap, "\x1b[K")
        MiB_swap = remove_from_string(MiB_swap, "\x1b[1m")
        #print(MiB_swap)
        finded = True
    header = re.compile(r'PID')
    for match in header.finditer(line):
        h = line[match.start():]
        h = remove_from_string(h, "(B")
        finded = True



    not_find = ["top - ","PID","MiB Swap:","MiB Mem :"]

    if(not finded):
        line_split = line.split()
        if(len(line_split) == 14):
            if(line_split[12]=="java"):
                #print(line_split)
                p_cpu += float(line_split[9])
                p_mem += float(line_split[10])


file.close()
file2.close()