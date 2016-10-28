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

import os
import shutil
import logging
import time
from Queue import Queue, Empty

from bd2k.util.objects import abstractclassmethod

from toil.batchSystems.abstractBatchSystem import BatchSystemSupport

logger = logging.getLogger(__name__)

# TODO: should this be an attribute?  Used in the worker and the batch system

class AbstractGridEngineBatchSystem(BatchSystemSupport):
    """
    A partial implementation of BatchSystemSupport for batch systems run on a
    standard HPC cluster. By default worker cleanup and hot deployment are not
    implemented.
    """
    
    def __init__(self, config, maxCores, maxMemory, maxDisk):
        super(AbstractGridEngineBatchSystem, self).__init__(config, maxCores, maxMemory, maxDisk)
        # AbstractBatchSystem.__init__(self, config, maxCores, maxMemory, maxDisk)
        self.resultsFile = self._getResultsFileName(config.jobStore)
        # Carried over from the GridEngine implementation, see issue #1108
        prefix = 'file:'
        if self.resultsFile.startswith(prefix):
            self.resultsFile = self.resultsFile[len(prefix):]
        # Reset the job queue and results (initially, we do this again once we've killed the jobs)
        self.resultsFileHandle = open(self.resultsFile, 'w')
        # We lose any previous state in this file, and ensure the files existence
        self.resultsFileHandle.close()
        self.currentJobs = set()
        
        # NOTE: this may be batch system dependent, maybe move into the worker?
        # Doing so would effectively make each subclass of AbstractGridEngineBatchSystem
        # much smaller
        self.maxCPU, self.maxMEM = self.obtainSystemConstants()
        
        self.nextJobID = 0
        self.newJobsQueue = Queue()
        self.updatedJobsQueue = Queue()
        self.killQueue = Queue()
        self.killedJobsQueue = Queue()
        # get the associated worker class here
        self.worker = self.Worker(self.newJobsQueue, self.updatedJobsQueue, self.killQueue,
                              self.killedJobsQueue, self)
        self.worker.start()

    def __des__(self):
        # Closes the file handle associated with the results file.
        self.resultsFileHandle.close()
        
    @classmethod
    def supportsWorkerCleanup(cls):
        return False

    @classmethod
    def supportsHotDeployment(cls):
        return False
            
    def issueBatchJob(self, command, memory, cores, disk, preemptable):
        self.checkResourceRequest(memory, cores, disk)
        jobID = self.nextJobID
        self.nextJobID += 1
        self.currentJobs.add(jobID)
        self.newJobsQueue.put((jobID, cores, memory, command))
        logger.debug("Issued the job command: %s with job id: %s ", command, str(jobID))
        return jobID
    
    def killBatchJobs(self, jobIDs):
        """
        Kills the given jobs, represented as Job ids, then checks they are dead by checking
        they are not in the list of issued jobs.
        """
        jobIDs = set(jobIDs)
        logger.debug('Jobs to be killed: %r', jobIDs)
        for jobID in jobIDs:
            self.killQueue.put(jobID)
        while jobIDs:
            killedJobId = self.killedJobsQueue.get()
            if killedJobId is None:
                break
            jobIDs.remove(killedJobId)
            if killedJobId in self.currentJobs:
                self.currentJobs.remove(killedJobId)
            if jobIDs:
                sleep = self.sleepSeconds()
                logger.debug('Some kills (%s) still pending, sleeping %is', len(jobIDs),
                             sleep)
                time.sleep(sleep)

    def getIssuedBatchJobIDs(self):
        """
        Gets the list of issued jobs
        """
        return list(self.currentJobs)

    def getRunningBatchJobIDs(self):
        return self.worker.getRunningJobIDs()

    def getUpdatedBatchJob(self, maxWait):
        try:
            item = self.updatedJobsQueue.get(timeout=maxWait)
        except Empty:
            return None
        logger.debug('UpdatedJobsQueue Item: %s', item)
        jobID, retcode = item
        self.currentJobs.remove(jobID)
        return jobID, retcode, None

    def shutdown(self):
        """
        Signals worker to shutdown (via sentinel) then cleanly joins the thread
        """
        newJobsQueue = self.newJobsQueue
        self.newJobsQueue = None

        newJobsQueue.put(None)
        self.worker.join()

    @classmethod
    def getWaitDuration(self):
        return 5
    
    @classmethod
    def getRescueBatchJobFrequency(cls):
        return 30 * 60 # Half an hour
    
    @classmethod
    def sleepSeconds(cls):
        return 1
    
    @abstractclassmethod
    def obtainSystemConstants(cls):
        """
        Returns the max. memory and max. CPU for the system
        """
        raise NotImplementedError()
