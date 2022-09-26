import os
import datetime
import matplotlib.pyplot as plt


path = '.'
directory_contents = os.listdir(path)
print("Directories:")
print(directory_contents)
print('')

mark = ["-.","-","--",":","-.","-","--",":"]

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
        if (x == "count.data"):
            if (os.path.isfile(item + "/" + x)) and (
                    os.stat(item + "/" + x).st_size > 0):
                aux_files.append(x)
    files_in_directories.append(aux_files)

print(files_in_directories)

times = []
datas = []

for i in range(0, len(directory_folders)):
    data = []
    time = []
    for j in range(0, len(files_in_directories[i])):
        if (os.path.isfile(directory_folders[i] + "/" + files_in_directories[i][j])) and (os.stat(directory_folders[i] + "/" + files_in_directories[i][j]).st_size > 0):
            path_file = str(directory_folders[i] + "/" + files_in_directories[i][j])
            f = open(path_file, "r")
            first = True
            aux = 0
            for x in f:
                # print(x)
                info = x.split(' ')
                d = info[0].split(':')
                if(first):
                    first = False
                    aux = int(d[0])*60*60 + int(d[1])*60 + int(d[2])
                time.append(int(d[0])*60*60 + int(d[1])*60 + int(d[2])-aux)
                data.append(float(info[1][:-1]))
    datas.append(data)
    times.append(time)


print(times)
print(times[1][3]-times[1][1])

def count_plot():
    for i in range(0, len(directory_folders)):
        if(len(datas[i])>0):
            plt.plot(times[i],datas[i],label=directory_folders[i],linewidth = 2.5,linestyle=mark[i])
        else:
            continue

    plt.xlim([0,600])
    plt.grid()
    #plt.title("Processed elements")
    plt.ylabel("Processed elements(uni)")
    plt.xlabel("Time(s)")
    plt.legend()
    plt.show()

count_plot()