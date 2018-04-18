import multiprocessing, subprocess, sys, gc
from multiprocessing.pool import Pool
from multiprocessing import Queue, Process

ON_POSIX = 'posix' in sys.builtin_module_names
hadoop = ['hdfs', 'dfs']
limit = 1073741824 * 6 # 6GB file size

def readFiles(q, reader):
    '''Reads buffered data from stdout of the cat process and adds the strings to the multiprocessing Queue'''
    for i in range(0, 20):
        q.put(reader.stdout.read(limit/20))

def writeFiles(q):
    '''Pulls 20 items from the multiprocessing Queue then writes them to a single hdfs file'''
    contents = q.get()
    for i in range(0, 19):
        contents += q.get()
    copier = subprocess.Popen(hadoop + ['-put',  '-', '/user/hdfs/part-%s.txt' % fileNum], close_fds=ON_POSIX, stdin=subprocess.PIPE, stderr=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=-1)
    result = copier.communicate(contents)
    returnCode = copier.returncode
    print returnCode
    print result[1] # prints errors from writing the file

if __name__ == '__main__':
    q = Queue()
    writeQueue = []

    listTask = subprocess.Popen(['ls', './test-data'], stdout=subprocess.PIPE)
    localFiles = map(lambda f: './testdata/' + f, listTask.communicate()[0].split('\n')[:-1])

    reader = subprocess.Popen(['cat'] + localFiles[:], stdout=subprocess.PIPE, stderr=subprocess.PIPE , bufsize=1)
    print reader.poll()

    fileNum = 1
    while reader.poll() is None:
        print 'reading for file %s' % fileNum

        readProcess = Process(target=readFiles, args=(q, reader))
        readProcess.start()
        gc.collect()
        print "Read process completed, starting write process."

        writeProcess = Process(target=writeFiles, args=(q,))
        writeProcess.start()
        writeQueue.append(writeProcess) # Write process is put into a list for ensuring completion later
        print 'File %s started.' % fileNum

        fileNum += 1
        readProcess.join()
    
    q.close()

    while len(writeQueue) > 0:
        writeQueue.pop().join() # Ensures all write threads have joined the main thread
    subprocess.call('hdfs dfs -cat part-* | hdfs dfs -put - final.txt && hdfs dfs -rm -skipTrash part-*', shell=True)
    # Reads files in hdfs and pipes into a single file. Then deletes the smaller files.