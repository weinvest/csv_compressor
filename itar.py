#!/usr/bin/env python
import os
import sys
import time
import tarfile
import subprocess
import multiprocessing

def findFiles(path):
    if os.path.isfile(path):
        return ([],[path])
    else:
        allFiles = []
        allDirs = []
        childrens = os.listdir(path)
        for cf in childrens:
            fp = os.path.join(path, cf)
            if os.path.isdir(fp):
                allDirs.append(fp)
                (dirs, files) = findFiles(fp)
                allDirs.extend(dirs)
                allFiles.extend(files)
            elif os.path.isfile(fp):
                allFiles.append(fp)
        return allDirs,allFiles


def createDirs(root, dirs):
    if not os.path.exists(root):
        os.mkdir(root)

    for d in dirs:
        os.mkdirs(os.path.join(root,d))


def compress(queue, root):
    try:
        while (f = q.get()) is not None:
            cmds = 'csv_comressor -c true -i %s -o %s/%s' % (f, root, f)
            subprocess.call(cmds.split())
    except multiprocessing.Queue.Empty, ex:
        return
    except exceptions.Exception,e:
        print(e)
    
def decompress(queue, root):
    try:
        while (f = queue.get()) is not None:
            cmds = 'csv_compressor -c false -i %s -o %s' % (f)
    except multiprocessing.Queue.Empty, ex:
        return
    except exceptions.Exception,e:
        print(e)

if __name__ == '__main__':
    if 4 != len(sys.argv) or sys.argv[1] not in ['-c','-d']:
        print('''usage: itar -c compressed.tar [file or directory]
                        itar -d compressed.tar [dest directory]''')

    parallel = 5
    if '-c' == sys.argv[1]:
        #compress
        dirs, files = findFiles(sys.argv[3])
        files.sort(lambda x,y : os.path.getsize(y) - os.path.getsize(x))

        tmpPath = '/tmp/%f' % time.time()
        if os.path.exists(tmpPath):
            shutils.rmtree(tmpPath)
        os.mkdir(tmpPath)
        createDirs(dirs, tmpPath)

        queue = multiprocessing.Queue()
        for f in files:
            queue.put(f)

        workers = []
        for i in range(0,parallel):
            worker = multiprocessing.Process(target = compress, args = (queue, tmpPath))
            workder.start()
            workers.append(woker)

        for worker in workers:
            worker.join()

        tar = tarfile.open(sys.argv[2], 'w')
        for f in files:
            realPath = os.path.join(tmpPath,f)
            if os.path.exists(realPath):
                tar.add(realPath)
        tar.close()



