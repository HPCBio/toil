# Copyright (C) 2015-2016 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
import logging
import os
from abc import ABCMeta, abstractmethod
from threading import Thread

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class AbstractGridEngineWorker(Thread):
    
    # __metaclass__ = ABCMeta
    
    def __init__(self, newJobsQueue, updatedJobsQueue, killQueue, killedJobsQueue, boss):
        '''
        Abstract worker interface class. All instances are created with five
        initial arguments (below). Note the Queue instances passed are empty.
        
        :param newJobsQueue: a Queue of new (unsubmitted) jobs
        :param updatedJobsQueue: a Queue of jobs that have been updated 
        :param killQueue: a Queue of active jobs that need to be killed
        :param killedJobsQueue: Queue of killed jobs for this worker
        :param boss: the AbstractGridEngineBatchSystem instance that controls this AbstractGridEngineWorker
        
        '''
        Thread.__init__(self)
        self.newJobsQueue = newJobsQueue
        self.updatedJobsQueue = updatedJobsQueue
        self.killQueue = killQueue
        self.killedJobsQueue = killedJobsQueue
        self.waitingJobs = list()
        self.runningJobs = set()
        self.boss = boss
        self.allocatedCpus = dict()
        self.batchJobIDs = dict()

    @abstractmethod
    def getRunningJobIDs(self):
        '''
        Get a list of running job IDs.

        :rtype: list
        '''
        raise NotImplementedError()

    def getBatchSystemID(self, jobID):
        if not jobID in self.batchJobIDs:
            RuntimeError("Unknown jobID, could not be converted")
    
        (job, task) = self.batchJobIDs[jobID]
        if task is None:
            return str(job)
        else:
            return str(job) + "." + str(task)
            
    def forgetJob(self, jobID):
        self.runningJobs.remove(jobID)
        del self.allocatedCpus[jobID]
        del self.batchJobIDs[jobID]

    def killJobs(self):
        # Load hit list:
        killList = list()
        while True:
            try:
                jobId = self.killQueue.get(block=False)
            except Empty:
                break
            else:
                killList.append(jobId)

        if not killList:
            return False

        # Do the dirty job
        for jobID in list(killList):
            if jobID in self.runningJobs:
                logger.debug('Killing job: %s', jobID)
                
                # this call should be implementation-specific, all other
                # code is redundant w/ other implementations
                self.killJob(jobID)
            else:
                if jobID in self.waitingJobs:
                    self.waitingJobs.remove(jobID)
                self.killedJobsQueue.put(jobID)
                killList.remove(jobID)

        # Wait to confirm the kill
        while killList:
            for jobID in list(killList):
                if self.getJobExitCode(self.batchJobIDs[jobID]) is not None:
                    logger.debug('Adding jobID %s to killedJobsQueue', jobID)
                    self.killedJobsQueue.put(jobID)
                    killList.remove(jobID)
                    self.forgetJob(jobID)
            if len(killList) > 0:
                logger.warn("Some jobs weren't killed, trying again in %is.", self.boss.sleepSeconds())
                time.sleep(self.boss.sleepSeconds())

        return True

    
    @abstractmethod
    def createJobs(self, newJob):
        '''
        Create a new job with the Toil job ID
        
        :param string newJob: Toil job ID 
        '''
        raise NotImplementedError()

    def checkOnJobs(self):
        activity = False
        logger.debug('List of running jobs: %r', self.runningJobs)
        for jobID in list(self.runningJobs):
            status = self.getJobExitCode(self.batchJobIDs[jobID])
            if status is not None:
                activity = True
                self.updatedJobsQueue.put((jobID, status))
                self.forgetJob(jobID)
        return activity

    @abstractmethod
    def run(self):
        '''
        Run any new jobs in queue
        '''
        raise NotImplementedError()

    @abstractmethod    
    def getJobExitCode(self, batchJobID):
        '''
        Return job exit code.
        
        :param string batchjobID: batch system job ID
        '''
        raise NotImplementedError()
        