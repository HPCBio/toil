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
    '''
    Abstract worker interface class.
    '''
    
    __metaclass__ = ABCMeta

    @abstractmethod
    def getRunningJobIDs(self):
        '''
        Get a list of running job IDs.

        :rtype: list
        '''
        raise NotImplementedError()
            
    @abstractmethod
    def getBatchSystemID(self, jobID):
        '''
        Get a batch system-specific job ID from Toil job IDs
        
        :param string jobID: Toil job ID
        '''
        raise NotImplementedError()

    @abstractmethod
    def forgetJob(self, jobID):
        '''
        Delete a specific Toil job from the list of submitted batch system jobs
        
        :param string jobID: the job identifier from the batch system
        '''
        raise NotImplementedError()

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
        Check job status
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
        