# -*- coding:utf-8 -*-

import os
import sys
import time
import Queue
import random

from multiprocessing.managers import BaseManager

TASKQUEUESERVERIP = '127.0.0.1'
TASKQUEUEPROT = 9000

class Jobs(object):
    def __init__(self, jobsLen = 10, startID = None):
        if not startID:
            startID = 0
        self.__startID = startID
        self.__jobsLen = jobsLen

    def getJobs(self, status):
        jobs = []
        for jobsid in range(self.__startID, self.__startID +  self.__jobsLen):
            dirs = [jobsid, status]
            dirs.extend([ str(x) for x in range(1, 10)])
            jobs.append(dirs)
        self.__startID += self.__jobsLen
        return jobs

class ServerManager(BaseManager):
    pass

class Master(object):
    def __init__(self, taskQueue = None, finishQueue =  None):
        if not taskQueue:
            taskQueue =  Queue.Queue()
        if not finishQueue:
            finishQueue = Queue.Queue()
        self.__taskQueue = taskQueue
        self.__finishQueue =  finishQueue
        self.__manager = None
        self.__dispatchtasks = None
        self.__finishtasks = None

        self.__jobs = Jobs()

    def _getTaskQueue(self):
        return self.__taskQueue

    def _getFinishQueue(self):
        return self.__finishQueue

    def start(self):
        ServerManager.register("getTaskQueue", callable=self._getTaskQueue)
        ServerManager.register("getFinishQueue", callable=self._getFinishQueue)

        self.__manager = ServerManager(address=('0.0.0.0', TASKQUEUEPROT), authkey=b'compass')
        self.__manager.start()
        #s = self.__manager.get_server()
        #s.serve_forever()

        self.__dispatchtasks = self.__manager.getTaskQueue()
        self.__finishtasks = self.__manager.getFinishQueue()

    def __shutdownServer(self):
        self.__manager.shutdown()

    def runJobs(self):
        while True:
            for jb in self.__jobs.getJobs(True):
                print "Dispatch tasks: [%d]" % jb[0], jb
                self.__dispatchtasks.put(jb)


            while not self.__dispatchtasks.empty():
                fj = self.__finishtasks.get(timeout = 60)
                print "Finished tasks: [%d]" % fj[0], fj

        self.__shutdownServer()

class Slave(object):
   def __init__(self):
       self.__dispatchtasks = None
       self.__finishtasks = None
       self.__manager = None

   def start(self):
       ServerManager.register("getTaskQueue")
       ServerManager.register("getFinishQueue")

       self.__manager = ServerManager(address=(TASKQUEUESERVERIP, TASKQUEUEPROT), authkey=b'compass')
       self.__manager.connect()

       self.__dispatchtasks = self.__manager.getTaskQueue()
       self.__finishtasks = self.__manager.getFinishQueue()
 
   def _runParser(self, jobs):
       time.sleep(3)

   def runJobs(self):
       while True:
           jobs = self.__dispatchtasks.get(timeout = 1)
           self._runParser(jobs)
           print "Finished tasks:", jobs
           self.__finishtasks.put(jobs)

if __name__ == '__main__':
    argv = sys.argv
    argv.append("-")

    def has_in_argv(keys):
        if type(keys) in [unicode or str]:
            return True if keys in argv else False
        for key in keys:
            if key in argv:
                return True
            return False

    if has_in_argv(["-s", "--slave", "slave"]):
        s = Slave()
        s.start()
        s.runJobs()
    elif has_in_argv(["-m", "--matser", "master"]):
        m = Master()
        m.start()
        m.runJobs()
