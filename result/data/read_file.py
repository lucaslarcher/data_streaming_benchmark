import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import os


def file_size(path_file):
    # try:
    f = open(path_file, "r")
    count = 0
    for x in f:
        count = count + len(x)
    return count


##except:
#  return -1

path = '.'
directory_contents = os.listdir(path)
print("Directories:")
print(directory_contents)
print('')

directory_folders = []
for item in directory_contents:
    if os.path.isdir(item):
        directory_folders.append(item)

files_in_directories = []
for item in directory_folders:
    #print("Files in " + item + ":")
    files = os.listdir(item)
    #print(files)
    aux_files = []
    for x in files:
        if (x != "count.data"):
            if (os.path.isfile(item + "/" + x)) and (
                    os.stat(item + "/" + x).st_size > 0):
                aux_files.append(x)
    files_in_directories.append(aux_files)

inds = []
datas = []

mark = ["-.","-","--",":","-.","-","--",":"]

for i in range(0, len(directory_folders)):
    data = []
    aux = []
    ind = []
    ind.append("time")
    data.append(aux)
    aux = []
    ind.append("directory")
    data.append(aux)
    jump = 0
    for j in range(0, len(files_in_directories[i])):
        try:
            if (os.path.isfile(directory_folders[i] + "/" + files_in_directories[i][j])) and (
                os.stat(directory_folders[i] + "/" + files_in_directories[i][j]).st_size > 0):
                #print(os.stat(item + "/" + files_in_directories[i][j]).st_size)
                aux = []
                ind.append(files_in_directories[i][j])
                data.append(aux)
                print(directory_folders[i] + "/" + files_in_directories[i][j])
                path_file = str(directory_folders[i] + "/" + files_in_directories[i][j])
                f = open(path_file, "r")
                for x in f:
                    try:
                        if(directory_folders[i]=="storm2"):
                            print(x)
                        info = x.split(",")
                        if (directory_folders[i] == "storm2"):
                            print(info)
                        if (j != 0):
                            data[j + 2 - jump].append(float(info[2][1:-2]))
                        else:
                            data[0].append(int(info[0]))
                            data[2].append(float(info[2][+1:-2]))
                            data[1].append(directory_folders[i])
                    except Exception as e:
                        print(e)
                        print("e")
                        continue
            else:
                jump = jump + 1
        except Exception as e:
            print(e)
    inds.append(ind)
    datas.append(data)

if(1 > 2):
    print("datas:")
    for y in datas:
        for x in y:
            print(x)
        for x in y:
            print(len(x))

dfs = []
for i in range(0,len(directory_folders)):
    datas[i] = [*zip(*datas[i])]
    df = pd.DataFrame(datas[i], columns=inds[i])
    dfs.append(df)
    aux = df.info()
    print(aux)
    #print(df["directory"])

print(inds)
#zip transpose the vector 2d
#data = [*zip(*data)]

#df = pd.DataFrame(data, columns=inds[3])
#df = df.transpose()

b = ['Storm','Kafka','Flink','Spark']



def cpu_load(dfs,b):
    a = ["java_lang_operatingsystem_processcpuload.data","java_lang_operatingsystem_processcpuload.data","flink_taskmanager_Status_JVM_CPU_Load.data","java_lang_operatingsystem_processcpuload.data"]
    for i in range(0, len(a)):
        sns.kdeplot(dfs[i][a[i]], label=b[i])
    plt.title("CPU Load")
    plt.ylabel("CPU Load(%)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()

def non_heap_memory(dfs,b):
    a = ["java_lang_memory_nonheapmemoryusage_used.data","java_lang_memory_nonheapmemoryusage_used.data","flink_jobmanager_Status_JVM_Memory_NonHeap_Used.data","java_lang_memory_nonheapmemoryusage_used.data"]
    for i in range(0, len(a)):
        sns.kdeplot(dfs[i][a[i]], label=b[i])
    plt.title("Non Heap Memory")
    plt.ylabel("Non Heap Memory(Bytes)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()

def memory_used(dfs,b):
    a = ["jvm_memory_bytes_used.data","jvm_memory_bytes_used.data","flink_jobmanager_Status_JVM_CPU_Time.data","jvm_memory_bytes_used.data"]
    for i in range(0, len(a)):
        if i == 2:
            continue
        sns.kdeplot(dfs[i][a[i]], label=b[i])
    plt.title("Memory Used")
    plt.ylabel("Memory Used(Bytes)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()


def fetch_latency(dfs,b):
    a = ["kafka_consumer_consumer_fetch_manager_metrics_fetch_latency_avg.data","kafka_consumer_consumer_fetch_manager_metrics_fetch_latency_avg.data","flink_taskmanager_job_task_operator_KafkaConsumer_fetch_latency_avg.data","kafka_consumer_consumer_fetch_manager_metrics_fetch_latency_avg.data"]
    for i in range(0, len(a)):
        if i == 0:
            continue
        sns.kdeplot(dfs[i][a[i]], label=b[i])
    plt.title("Fetch Latency")
    plt.ylabel("Fetch Latency(ms)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()

def cpu_load_dist(dfs,b):
    keys = ["flink_taskmanager_Status_JVM_CPU_Load.data","java_lang_operatingsystem_processcpuload.data"]
    a = []
    for i in range(0, len(inds)):
        for j in range(0,len(inds[i])):
            for k in keys:
                if (k==inds[i][j]):
                    a.append(j)

    print("a")
    print(a)
    #print(datas[3])

    for i in range(0, len(directory_folders)):
        plt.plot([*zip(*datas[i])][0],[*zip(*datas[i])][a[i]],mark[i],label=directory_folders[i])

    y = [0, 1.1]
    x = [600, 600]
    plt.plot(x, y, "r--")

    plt.xlim([0, 720])

    plt.title("CPU Load")
    plt.ylabel("CPU Load(%)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()

def non_heap_memory_dist(dfs,b):
    keys = ["flink_jobmanager_Status_JVM_Memory_NonHeap_Used.data","java_lang_memory_nonheapmemoryusage_used.data"]
    a = []
    for i in range(0, len(inds)):
        for j in range(0, len(inds[i])):
            for k in keys:
                if (k == inds[i][j]):
                    a.append(j)
                    break


    for i in range(0, len(a)):
        plt.plot([*zip(*datas[i])][0],[*zip(*datas[i])][a[i]],mark[i],label=directory_folders[i])

    y = [0, 100000000]
    x = [600, 600]
    plt.plot(x, y, "r--")

    plt.xlim([0, 720])

    plt.title("Non Heap Memory")
    plt.ylabel("Non Heap Memory(Bytes)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()


def memory_used_dist(dfs,b):
    keys = ["flink_jobmanager_Status_JVM_CPU_Time.data","jvm_memory_bytes_used.data"]
    a = []
    for i in range(0, len(inds)):
        for j in range(0, len(inds[i])):
            for k in keys:
                if (k == inds[i][j]):
                    a.append(j)
    for i in range(0, len(a)):
        if i == 2:
            continue
        plt.plot([*zip(*datas[i])][0],[*zip(*datas[i])][a[i]],label=directory_folders[i])
    plt.title("Memory Used")
    plt.ylabel("Memory Used(Bytes)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()


def fetch_latency_dist(dfs,b):
    keys = ["flink_taskmanager_job_task_operator_KafkaConsumer_fetch_latency_avg.data","kafka_consumer_consumer_fetch_manager_metrics_fetch_latency_avg.data"]
    a = []
    for i in range(0, len(inds)):
        for j in range(0, len(inds[i])):
            for k in keys:
                if (k == inds[i][j]):
                    a.append(j)
    for i in range(0, len(a)):
        if i == 0:
            continue
        plt.plot([*zip(*datas[i])][0],[*zip(*datas[i])][a[i]],label=directory_folders)
    plt.title("Fetch Latency")
    plt.ylabel("Fetch Latency(ms)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()


#cpu_load(dfs,b)
#non_heap_memory(dfs,b)
#memory_used(dfs,b)
#fetch_latency(dfs,b)
cpu_load_dist(dfs,b)
non_heap_memory_dist(dfs,b)
#memory_used_dist(dfs,b)
#fetch_latency_dist(dfs,b)
