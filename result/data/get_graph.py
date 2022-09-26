import os

def simplify_data(entrace):
    split_data = entrace.split(":")
    sum = int(split_data[0])*60*60 + int(split_data[1])*60 + int(split_data[2])
    return sum


mark = ["-.","-","--",":","-.","-","--",":"]

path = '.'
directory_contents = os.listdir(path)

print(directory_contents)

print("Directories:")
directory_folders = []
for item in directory_contents:
    if os.path.isdir(item):
        directory_folders.append(item)
print(directory_folders)
print('')

#READ AND EXTRACT FILE INFO +++++++++++++++++++++++++++++++++++++++++++

big_vec = []

for d in directory_folders:
    source = d+"/out.data"
    file = open(source,"r")

    vec = []
    aux_time = []
    aux_cpu = []
    aux_mem = []
    vec.append(aux_time)
    vec.append(aux_cpu)
    vec.append(aux_mem)

    time = ''
    cpu = ''
    mem = ''
    for line in file:
        #print(line)
        if (line.find("up") >= 0):
            #print(line[:8])
            time = line[:8]
        if(line.find("CPU") >= 0):
            #print(line[4:-1])
            cpu = float(line[4:-1])/16
        if(line.find("MEM") >= 0):
            #print(line[4:-1])
            mem = float(line[4:-1])
            vec[0].append(time)
            vec[1].append(cpu)
            vec[2].append(mem)

    #print(vec)
    file.close()

#TRASNFORM DATA IN SECS +++++++++++++++++++++++++++++++++++++++++++



    lesser = simplify_data(vec[0][0])
    #print(lesser)

    aux_vec = []
    for k in vec[0]:
        k = simplify_data(k)-lesser
        aux_vec.append(k)
    vec[0] = aux_vec

    print(vec)

    big_vec.append(vec)

print(big_vec)

#PRINT GRAP +++++++++++++++++++++++++++++++++++++++++++++++++++++++

import matplotlib.pyplot as plt

def print_CPU():
    for i in range(0, len(big_vec)):
        plt.plot(big_vec[i][0],big_vec[i][1],label=directory_folders[i],linewidth = 2.5,linestyle=mark[i])

    plt.xlim([0,600])
    plt.grid()
    #plt.ylim([0, 400])
    #plt.title("CPU Consumer")
    plt.ylabel("CPU Consume (%)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()

def print_MEM():
    for i in range(0, len(big_vec)):
        plt.plot(big_vec[i][0],big_vec[i][2],label=directory_folders[i],linewidth = 2.5,linestyle=mark[i])

    plt.xlim([0,600])
    plt.ylim([0, 35])
    plt.grid()
    #plt.title("MEM Consumer")
    plt.ylabel("MEM Consume (%)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()

print_CPU()
print_MEM()