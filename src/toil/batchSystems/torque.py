# Copyright (C) 2015 UCSC Computational Genomics Lab
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
import sys
import xml.etree.ElementTree as ET
import tempfile

from Queue import Queue, Empty
from threading import Thread

from toil.batchSystems import MemoryString
from toil.batchSystems.abstractGridEngineBatchSystem import AbstractGridEngineBatchSystem
from toil.batchSystems.abstractGridEngineWorker import AbstractGridEngineWorker

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class TorqueBatchSystem(AbstractGridEngineBatchSystem):
    
    # class-specific Worker
    class Worker(AbstractGridEngineWorker):
    
        def getRunningJobIDs(self):
            times = {}
            currentjobs = dict((str(self.batchJobIDs[x][0]), x) for x in self.runningJobs)
            process = subprocess.Popen(["qstat"], stdout=subprocess.PIPE)
            stdout, stderr = process.communicate()
    
            # TODO: qstat supports XML output which is more comprehensive, so maybe use that?
            for currline in stdout.split('\n'):
                items = currline.strip().split()
                if items:
                    jobid = items[0].strip().split('.')[0]
                    if jobid in currentjobs and items[4] == 'R':
                        walltime = items[3]
                        # normal qstat has a quirk with job time where it reports '0'
                        # when initially running; this catches this case
                        if walltime == '0':
                            logger.debug("JobID:" + jobid + ", time:" + walltime)
                            walltime = time.mktime(time.strptime(walltime, "%S"))                        
                        else:
                            walltime = time.mktime(time.strptime(walltime, "%H:%M:%S"))
                        times[currentjobs[jobid]] = walltime
    
            return times
    
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
                    subprocess.check_call(['qdel', self.getBatchSystemID(jobID)])
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
            while len(self.waitingJobs) > 0 and sum(self.allocatedCpus.values()) < int(
                    self.boss.maxCores):
                activity = True
                jobID, cpu, memory, command = self.waitingJobs.pop(0)
                qsubline = self.prepareQsub(cpu, memory, jobID) + [self._generateTorqueWrapper(command)]
                batchJobIDs = self.qsub(qsubline)
                self.batchJobIDs[jobID] = (batchJobIDs, None)
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
            
            # TODO: passing $PWD on command line not working for -d, resorting to
            # $PBS_O_WORKDIR but maybe should fix this here instead of in script?
            
            # TODO: we previosuly trashed the stderr/stdout, as in the commented
            # code, but these may be retained by others, particularly for debugging.
            # Maybe an option or attribute w/ a location for storing the logs?
    
            # qsubline = ['qsub', '-V', '-j', 'oe', '-o', '/dev/null',
            #             '-e', '/dev/null', '-N', 'toil_job_{}'.format(jobID)]
            
            qsubline = ['qsub', '-V', '-N', 'toil_job_{}'.format(jobID)]
                    
            if self.boss.environment:
                qsubline.append('-v')
                qsubline.append(','.join(k + '=' + quote(os.environ[k] if v is None else v)
                                         for k, v in self.boss.environment.iteritems()))
                
            reqline = list()
            if mem is not None:
                memStr = str(mem / 1024) + 'K'
                reqline += ['-l mem=' + memStr]
    
            if cpu is not None and math.ceil(cpu) > 1:
                qsubline.extend(['-l ncpus=' + str(int(math.ceil(cpu)))])
            
            return qsubline
        
    
        def qsub(self, qsubline):
            logger.debug("Running %r", ' '.join(qsubline))
            process = subprocess.Popen(qsubline, stdout=subprocess.PIPE)
            so, se = process.communicate()
            # TODO: the full URI here may be needed on complex setups, stripping
            # down to integer job ID only may be bad long-term
            result = int(so.strip().split('.')[0])
            logger.debug("Result %r", result)
            return result
    
        def getJobExitCode(self, torqueJobID):
            job, task = torqueJobID
            args = ["qstat", "-f", str(job)]
    
            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            for line in process.stdout:
                line = line.strip()
                # logger.debug("Line: " + line)
                if line.startswith("failed") and int(line.split()[1]) == 1:
                    return 1
                if line.startswith("exit_status"):
                    status = line.split(' = ')[1]
                    logger.debug('Exit Status: ' + status)
                    return int(status)
            return None
        
        def _generateTorqueWrapper(self, command):
            """
            A very simple script generator that just wraps the command given; for
            now this goes to default tempdir
            """
            _, tmpFile = tempfile.mkstemp(suffix='.sh', prefix='torque_wrapper')
            fh = open(tmpFile , 'w')
            fh.write("$!/bin/bash\n\n")
            fh.write("cd $PBS_O_WORKDIR\n\n")
            fh.write(command + "\n")
            fh.close
            return tmpFile    
    
    """
    The interface for the PBS/Torque batch system
    """

    @staticmethod
    def obtainSystemConstants():

        maxCPU = 0
        maxMEM = MemoryString("0K")

        # parse XML output from pbsnodes
        root = ET.fromstring(subprocess.check_output(["pbsnodes","-x"]))

        # for each node, grab status line
        for node in root.findall('./Node/status'):
            # then split up the status line by comma and iterate
            status = {}
            for state in node.text.split(","):
                statusType, statusState = state.split("=")
                status[statusType] = statusState
            if status['ncpus'] is None or status['totmem'] is None:
                RuntimeError("pbsnodes command does not return ncpus or totmem columns")
            if status['ncpus'] > maxCPU:
                maxCPU = status['ncpus']
            if MemoryString(status['totmem']) > maxMEM:
                maxMEM = MemoryString(status['totmem'])

        if maxCPU is 0 or maxMEM is MemoryString("0K"):
            RuntimeError('pbsnodes returned null ncpus or totmem info')
        else:
            logger.info("Got maxCPU: %s and maxMEM: %s" % (maxCPU, maxMEM, ))

        return maxCPU, maxMEM

    def setEnv(self, name, value=None):
        if value and ',' in value:
            raise ValueError("Torque does not support commata in environment variable values")
        return super(TorqueBatchSystem,self).setEnv(name, value)

