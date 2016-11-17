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

    @abstractmethod
    def killJobs(self):
        '''
        Delete all Toil jobs.
        '''
        raise NotImplementedError()
    
    @abstractmethod
    def createJobs(self, newJob):
        '''
        Create a new job with the Toil job ID
        
        :param string newJob: Toil job ID 
        '''
        raise NotImplementedError()

    @abstractmethod
    def checkOnJobs(self):
        '''
        Check status on all jobs
        '''
        raise NotImplementedError()

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
        