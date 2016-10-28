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
from pipes import quote
import subprocess
import time
import math
from Queue import Queue, Empty
from threading import Thread

from toil.batchSystems import MemoryString
from toil.batchSystems.abstractGridEngineBatchSystem import AbstractGridEngineBatchSystem
from toil.batchSystems.abstractGridEngineWorker import AbstractGridEngineWorker

logger = logging.getLogger(__name__)

class GridEngineBatchSystem(AbstractGridEngineBatchSystem):
    
    class Worker(Thread):
        def __init__(self, newJobsQueue, updatedJobsQueue, killQueue, killedJobsQueue, boss):
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
    
        def getRunningJobIDs(self):
            times = {}
            currentjobs = dict((str(self.batchJobIDs[x][0]), x) for x in self.runningJobs)
            process = subprocess.Popen(["qstat"], stdout=subprocess.PIPE)
            stdout, stderr = process.communicate()
    
            for currline in stdout.split('\n'):
                items = currline.strip().split()
                if items:
                    if items[0] in currentjobs and items[4] == 'r':
                        jobstart = " ".join(items[5:7])
                        jobstart = time.mktime(time.strptime(jobstart, "%m/%d/%Y %H:%M:%S"))
                        times[currentjobs[items[0]]] = time.time() - jobstart
    
            return times
    
        def getSgeID(self, jobID):
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
                    subprocess.check_call(['qdel', self.getSgeID(jobID)])
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
    
        def createJobs(self, newJob):
            activity = False
            # Load new job id if present:
            if newJob is not None:
                self.waitingJobs.append(newJob)
            # Launch jobs as necessary:
            while (len(self.waitingJobs) > 0
                   and sum(self.allocatedCpus.values()) < int(self.boss.maxCores)):
                activity = True
                jobID, cpu, memory, command = self.waitingJobs.pop(0)
                qsubline = self.prepareQsub(cpu, memory, jobID) + [command]
                sgeJobID = self.qsub(qsubline)
                self.batchJobIDs[jobID] = (sgeJobID, None)
                self.runningJobs.add(jobID)
                self.allocatedCpus[jobID] = cpu
            return activity
    
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
    
        def run(self):
            while True:
                activity = False
                newJob = None
                if not self.newJobsQueue.empty():
                    activity = True
                    newJob = self.newJobsQueue.get()
                    if newJob is None:
                        logger.debug('Received queue sentinel.')
                        break
                activity |= self.killJobs()
                activity |= self.createJobs(newJob)
                activity |= self.checkOnJobs()
                if not activity:
                    logger.debug('No activity, sleeping for %is', self.boss.sleepSeconds())
                    time.sleep(self.boss.sleepSeconds())
    
        def prepareQsub(self, cpu, mem, jobID):
            qsubline = ['qsub', '-V', '-b', 'y', '-terse', '-j', 'y', '-cwd',
                        '-N', 'toil_job_' + str(jobID)]
    
            if self.boss.environment:
                qsubline.append('-v')
                qsubline.append(','.join(k + '=' + quote(os.environ[k] if v is None else v)
                                         for k, v in self.boss.environment.iteritems()))
    
            reqline = list()
            if mem is not None:
                memStr = str(mem / 1024) + 'K'
                reqline += ['vf=' + memStr, 'h_vmem=' + memStr]
            if len(reqline) > 0:
                qsubline.extend(['-hard', '-l', ','.join(reqline)])
            sgeArgs = os.getenv('TOIL_GRIDENGINE_ARGS')
            if sgeArgs:
                sgeArgs = sgeArgs.split()
                for arg in sgeArgs:
                    if arg.startswith(("vf=", "hvmem=", "-pe")):
                        raise ValueError("Unexpected CPU, memory or pe specifications in TOIL_GRIDGENGINE_ARGs: %s" % arg)
                qsubline.extend(sgeArgs)
            if cpu is not None and math.ceil(cpu) > 1:
                peConfig = os.getenv('TOIL_GRIDENGINE_PE') or 'shm'
                qsubline.extend(['-pe', peConfig, str(int(math.ceil(cpu)))])
            return qsubline
    
        def qsub(self, qsubline):
            logger.debug("Running %r", " ".join(qsubline))
            process = subprocess.Popen(qsubline, stdout=subprocess.PIPE)
            result = int(process.stdout.readline().strip().split('.')[0])
            return result
    
        def getJobExitCode(self, sgeJobID):
            job, task = sgeJobID
            args = ["qacct", "-j", str(job)]
            if task is not None:
                args.extend(["-t", str(task)])
            logger.debug("Running %r", args)
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in process.stdout:
                if line.startswith("failed") and int(line.split()[1]) == 1:
                    return 1
                elif line.startswith("exit_status"):
                    logger.debug('Exit Status: %r', line.split()[1])
                    return int(line.split()[1])
            return None

    """
    The interface for SGE aka Sun GridEngine.
    """

    @classmethod
    def workerClass(self):
        return GridEngineWorker

    @classmethod
    def getWaitDuration(self):
        return 0.0

    @staticmethod
    def obtainSystemConstants():
        lines = filter(None, map(str.strip, subprocess.check_output(["qhost"]).split('\n')))
        line = lines[0]
        items = line.strip().split()
        num_columns = len(items)
        cpu_index = None
        mem_index = None
        for i in range(num_columns):
            if items[i] == 'NCPU':
                cpu_index = i
            elif items[i] == 'MEMTOT':
                mem_index = i
        if cpu_index is None or mem_index is None:
            RuntimeError('qhost command does not return NCPU or MEMTOT columns')
        maxCPU = 0
        maxMEM = MemoryString("0")
        for line in lines[2:]:
            items = line.strip().split()
            if len(items) < num_columns:
                RuntimeError('qhost output has a varying number of columns')
            if items[cpu_index] != '-' and items[cpu_index] > maxCPU:
                maxCPU = items[cpu_index]
            if items[mem_index] != '-' and MemoryString(items[mem_index]) > maxMEM:
                maxMEM = MemoryString(items[mem_index])
        if maxCPU is 0 or maxMEM is 0:
            RuntimeError('qhost returned null NCPU or MEMTOT info')
        return maxCPU, maxMEM

    def setEnv(self, name, value=None):
        if value and ',' in value:
            raise ValueError("GridEngine does not support commata in environment variable values")
        return super(GridEngineBatchSystem,self).setEnv(name, value)
