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
import os
import socket
import subprocess
import logging

import time
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from boto.exception import BotoServerError, EC2ResponseError
from cgcloud.lib.ec2 import ec2_instance_types, retry_ec2, wait_spot_requests_active, a_short_time, \
    wait_transition, inconsistencies_detected, create_spot_instances, create_ondemand_instances
from itertools import islice, count

from toil.batchSystems.abstractBatchSystem import AbstractScalableBatchSystem
from toil.provisioners.abstractProvisioner import AbstractProvisioner, Shape
from toil.provisioners.aws import *
from cgcloud.lib.context import Context
from boto.utils import get_instance_metadata

from toil.provisioners import BaseAWSProvisioner

logger = logging.getLogger(__name__)


def expectedLaunchErrors(e):
    return e.status == 400 and ('iamInstanceProfile.arn is invalid' in e.body or 'has no associated IAM' in e.body)


def expectedShutdownErrors(e):
    return e.status == 400 and 'dependent object' in e.body


def groupNotFound(e):
    retry = (e.status == 400 and 'does not exist in default VPC' in e.body)
    if not retry:
        # I have seen this error go uncaught a couple times. Log the body so I can catch it next.
        logger.error(e.body)
    return retry


dockerInfo = os.environ['TOIL_APPLIANCE_SELF'] if 'TOIL_APPLIANCE_SELF' in os.environ else None


class AWSProvisioner(AbstractProvisioner, BaseAWSProvisioner):

    def __init__(self, config, batchSystem):
        self.ctx = Context(availability_zone='us-west-2a', namespace='/')
        self.instanceMetaData = get_instance_metadata()
        self.securityGroupName = self.instanceMetaData['security-groups']
        self.spotBid = None
        parsedBid = config.nodeType.split(':', 1)
        if len(config.nodeType) != len(parsedBid[0]):
            # there is a bid
            self.spotBid = parsedBid[1]
            self.instanceType = ec2_instance_types[parsedBid[0]]
        else:
            self.instanceType = ec2_instance_types[config.nodeType]
        self.batchSystem = batchSystem
        self.masterIP = self.instanceMetaData['local-ipv4']
        self.keyName = self.instanceMetaData['public-keys'].keys()[0]

    def setNodeCount(self, numNodes, preemptable=False, force=False):
        # get all nodes in cluster
        workerInstances = self._getWorkersInCluster(preemptable)
        instancesToLaunch = numNodes - len(workerInstances)
        logger.info('Adjusting cluster size by %s', instancesToLaunch)
        if instancesToLaunch > 0:
            self._addNodes(instancesToLaunch, preemptable=preemptable)
        elif instancesToLaunch < 0:
            self._removeNodes(instances=workerInstances, numNodes=numNodes, preemptable=preemptable, force=force)
        else:
            pass
        workerInstances = self._getWorkersInCluster(preemptable)
        return len(workerInstances)

    def getNodeShape(self, preemptable=False):
        instanceType = self.instanceType
        return Shape(wallTime=60 * 60,
                     memory=instanceType.memory * 2 ** 30,
                     cores=instanceType.cores,
                     disk=(instanceType.disks * instanceType.disk_capacity * 2 ** 30))

    @classmethod
    def sshLeader(cls, clusterName):
        master = cls._getMaster(clusterName)
        logger.info('SSH ready')
        cls._ssh(master.ip_address, 'bash')

    @classmethod
    def _ssh(cls, masterIP, command):
        command = "ssh -o \"StrictHostKeyChecking=no\" -t core@%s \"docker exec -it leader %s\"" % (masterIP, command)
        return subprocess.check_call(command, shell=True)

    @classmethod
    def _sshOuter(cls, masterIP, command):
        command = "ssh -o \"StrictHostKeyChecking=no\" -t core@%s \"%s\"" % (masterIP, command)
        ouput = subprocess.check_output(command, shell=True)
        return ouput

    @classmethod
    def _getMaster(cls, clusterName, wait=False):
        ctx = Context(availability_zone='us-west-2a', namespace='/')
        instances = cls.__getNodesInCluster(ctx, clusterName, both=True)
        instances.sort(key=lambda x: x.launch_time)
        master = instances[0]  # assume master was launched first
        if wait:
            logger.info('Waiting for master to run...')
            wait_transition(master, {'pending'}, 'running')
            logger.info('... master is running')
            cls._waitForIP(master)
            masterIP = master.ip_address
            cls._waitForSSHPort(masterIP)
            # wait here so docker commands can be used reliably afterwards
            cls._waitForDockerDaemon(masterIP)
            cls._waitForAppliance(masterIP)
        return master

    @classmethod
    def _waitForAppliance(cls, ip_address):
        logger.info('Waiting for master Toil appliance to start...')
        while True:
            output = cls._sshOuter(masterIP=ip_address, command='docker ps')
            if 'leader' in output:
                logger.info('...Toil appliance started')
                break
            else:
                logger.info('...Still waiting, trying again in 10sec...')
                time.sleep(10)

    @classmethod
    def _waitForIP(cls, instance):
        """
        Wait until the instances has a public IP address assigned to it.

        :type instance: boto.ec2.instance.Instance
        """
        logger.info('Waiting for master ip...')
        while True:
            time.sleep(a_short_time)
            instance.update()
            if instance.ip_address or instance.public_dns_name:
                logger.info('...got master ip')
                break

    @classmethod
    def _waitForDockerDaemon(cls, ip_address):
        logger.info('Waiting for docker to start...')
        command = 'ps aux | grep \\"docker daemon\\"'
        while True:
            output = cls._sshOuter(ip_address, command)
            time.sleep(5)
            if 'root' in output:
                # ps aux | grep x will always list itself. The actual docker daemon process will
                # be started by root whereas the ssh-ed command will be executed by the user 'core'
                break
            else:
                logger.info('... Still waiting...')
        logger.info('Docker daemon running')

    @classmethod
    def _waitForSSHPort(cls, ip_address):
        """
        Wait until the instance represented by this box is accessible via SSH.

        :return: the number of unsuccessful attempts to connect to the port before a the first
        success
        """
        logger.info('Waiting for master ssh port to open...')
        for i in count():
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                s.settimeout(a_short_time)
                s.connect((ip_address, 22))
                logger.info('...ssh port open')
                return i
            except socket.error:
                pass
            finally:
                s.close()

    @classmethod
    def launchCluster(cls, instanceType, keyName, clusterName, spotBid=None):
        ctx = Context(availability_zone='us-west-2a', namespace='/')
        profileARN = cls._getProfileARN(ctx, role='leader')
        # the security group name is used as the cluster identifier
        cls._createSecurityGroup(ctx, clusterName)
        bdm = cls._getBlockDeviceMapping(ec2_instance_types[instanceType])
        dockerLeaderData = dockerInfo.rsplit(':', 1)
        leaderRepo = dockerLeaderData[0]
        leaderTag = dockerLeaderData[1]
        leaderData = {'role': 'leader', 'tag': leaderTag,
                      'args': leaderArgs.format(name=clusterName), 'repo': leaderRepo}
        userData = AWSUserData.format(**leaderData)
        kwargs = {'key_name': keyName, 'security_groups': [clusterName],
                  'instance_type': instanceType,
                  'user_data': userData, 'block_device_map': bdm,
                  'instance_profile_arn': profileARN}
        if not spotBid:
            logger.info('Launching non-preemptable master')
            create_ondemand_instances(ctx.ec2, image_id=coreOSAMI,
                                      spec=kwargs, num_instances=1)
        else:
            kwargs['launch_group'] = clusterName
            logger.info('Launching preemptable master')
            # force generator to evaluate
            list(create_spot_instances(ec2=ctx.ec2, price=spotBid, image_id=coreOSAMI, spec=kwargs, num_instances=1))
        return cls._getMaster(clusterName=clusterName, wait=True)

    @classmethod
    def destroyCluster(cls, clusterName):
        ctx = Context(availability_zone='us-west-2a', namespace='/')
        instances = cls.__getNodesInCluster(ctx, clusterName, both=True)
        spotIDs = cls._getSpotRequestIDs(ctx, clusterName)
        if spotIDs:
            ctx.ec2.cancel_spot_instance_requests(request_ids=spotIDs)
        if instances:
            cls._terminateInstance(instances=instances, ctx=ctx)
        logger.info('Deleting security group...')
        for attempt in retry_ec2(retry_after=30, retry_for=300, retry_while=expectedShutdownErrors):
            with attempt:
                ctx.ec2.delete_security_group(name=clusterName)
        logger.info('... Succesfully deleted security group')

    @classmethod
    def _terminateInstance(cls, instances, ctx):
        instanceIDs = [x.id for x in instances]
        logger.info('Terminating instance(s): %s', instanceIDs)
        ctx.ec2.terminate_instances(instance_ids=instanceIDs)
        logger.info('Instance(s) terminated.')

    @classmethod
    def _deleteIAMProfiles(cls, instances, ctx):
        # this method is not currently used- roles and profiles are shared between clusters
        # so destroying a cluster while another is running can cause problems. Instead the
        # 2 profiles/roles are left behind to be reused by subsequent clusters.
        instanceProfiles = [x.instance_profile for x in instances]
        for profile in instanceProfiles:
            profile_name = profile['arn'].split('/', 1)[1]
            try:
                ctx.iam.remove_role_from_instance_profile(profile_name, 'toil-appliance-worker')
            except BotoServerError as e:
                if e.status == 404:
                    ctx.iam.remove_role_from_instance_profile(profile_name, 'toil-appliance-leader')
                else:
                    raise
            ctx.iam.delete_instance_profile(profile_name)

    def _addNodes(self, instancesToLaunch, preemptable=False):
        bdm = self._getBlockDeviceMapping(self.instanceType)
        arn = self._getProfileARN(self.ctx, role='worker')
        # quay.io/toil-leader:tag
        workerData = dockerInfo.rsplit(':', 1)
        workerRepo = workerData[0].rsplit('-', 1)[0] + '-worker'
        workerTag = workerData[1]
        workerData = {'role': 'worker', 'tag': workerTag,
                      'args': workerArgs.format(ip=self.masterIP, preemptable=preemptable),
                      'repo': workerRepo}
        userData = AWSUserData.format(**workerData)
        if not preemptable:
            logger.debug('Launching non-preemptable instance(s)')
            for instance in range(0, instancesToLaunch):
                for attempt in retry_ec2(retry_while=expectedLaunchErrors):
                    with attempt:
                        self.ctx.ec2.run_instances(image_id=coreOSAMI, key_name=self.keyName,
                                                   security_groups=[self.securityGroupName],
                                                   instance_type=self.instanceType.name,
                                                   instance_profile_arn=arn, user_data=userData,
                                                   block_device_map=bdm)
        else:
            logger.debug('Launching spot instance(s) with bid of %s', self.spotBid)
            requests = []
            for attempt in retry_ec2(retry_while=expectedLaunchErrors):
                with attempt:
                    # returns list of SpotInstanceRequests
                    requests = self.ctx.ec2.request_spot_instances(price=self.spotBid, image_id=coreOSAMI,
                                                                   count=instancesToLaunch, key_name=self.keyName,
                                                                   security_groups=[self.securityGroupName],
                                                                   instance_type=self.instanceType.name,
                                                                   instance_profile_arn=arn, user_data=userData,
                                                                   block_device_map=bdm)
            wait_spot_requests_active(ec2=self.ctx.ec2, requests=requests)

        logger.info('Launched %s new instance(s)', instancesToLaunch)

    @classmethod
    def _getBlockDeviceMapping(cls, instanceType):
        # determine number of ephemeral drives via cgcloud-lib
        bdtKeys = ['', '/dev/xvdb', '/dev/xvdc', '/dev/xvdd']
        bdm = BlockDeviceMapping()
        # the first disk is already attached for us so start with 2nd.
        for disk in xrange(1, instanceType.disks + 1):
            bdm[bdtKeys[disk]] = BlockDeviceType(
                ephemeral_name='ephemeral{}'.format(disk - 1))  # ephemeral counts start at 0

        logger.debug('Device mapping: %s', bdm)
        return bdm

    def _removeNodes(self, instances, numNodes, preemptable=False, force=False):
        # based off toil.provisioners.cgcloud.provisioner.CGCloudProvisioner._removeNodes()
        logger.debug('Attempting to delete nodes - force = %s', force)
        # If the batch system is scalable, we can use the number of currently running workers on
        # each node as the primary criterion to select which nodes to terminate.
        if isinstance(self.batchSystem, AbstractScalableBatchSystem):
            logger.debug('Using a scalable batch system')
            nodes = self.batchSystem.getNodes(preemptable)
            # Join nodes and instances on private IP address.
            nodes = [(instance, nodes.get(instance.private_ip_address)) for instance in instances]
            # Unless forced, exclude nodes with runnning workers. Note that it is possible for
            # the batch system to report stale nodes for which the corresponding instance was
            # terminated already. There can also be instances that the batch system doesn't have
            # nodes for yet. We'll ignore those, too, unless forced.
            nodes = [(instance, nodeInfo)
                     for instance, nodeInfo in nodes
                     if force or nodeInfo is not None and nodeInfo.workers < 1]
            # Sort nodes by number of workers and time left in billing cycle
            nodes.sort(key=lambda (instance, nodeInfo): (
                nodeInfo.workers if nodeInfo else 1,
                self._remainingBillingInterval(instance)))
            nodes = nodes[numNodes:]
            instancesTerminate = [instance for instance, nodeInfo in nodes]
        else:
            # Without load info all we can do is sort instances by time left in billing cycle.
            instances = sorted(instances,
                               key=lambda instance: (self._remainingBillingInterval(instance)))
            instancesTerminate = [instance for instance in islice(instances, numNodes)]
        if instancesTerminate:
            self._terminateInstance(instances=instancesTerminate, ctx=self.ctx)
        else:
            logger.debug('No nodes to delete')
        return len(instancesTerminate)

    @classmethod
    def __getNodesInCluster(cls, ctx, clusterName, preemptable=False, both=False):
        pendingInstances = ctx.ec2.get_only_instances(filters={'instance.group-name': clusterName,
                                                               'instance-state-name': 'pending'})
        runningInstances = ctx.ec2.get_only_instances(filters={'instance.group-name': clusterName,
                                                               'instance-state-name': 'running'})
        instances = set(pendingInstances)
        if not preemptable and not both:
            return [x for x in instances.union(set(runningInstances)) if x.spot_instance_request_id is None]
        elif preemptable and not both:
            return [x for x in instances.union(set(runningInstances)) if x.spot_instance_request_id is not None]
        elif both:
            return [x for x in instances.union(set(runningInstances))]

    def _getNodesInCluster(self, preeptable=False, both=False):
        if not both:
            return self.__getNodesInCluster(self.ctx, self.securityGroupName, preemptable=preeptable)
        else:
            return self.__getNodesInCluster(self.ctx, self.securityGroupName, both=both)

    def _getWorkersInCluster(self, preemptable):
        entireCluster = self._getNodesInCluster(both=True)
        logger.debug('All nodes in cluster %s', entireCluster)
        workerInstances = [i for i in entireCluster if i.private_ip_address != self.masterIP and
                           preemptable != (i.spot_instance_request_id is None)]
        logger.debug('Workers found in cluster after filtering %s', workerInstances)
        return workerInstances

    @classmethod
    def _getSpotRequestIDs(cls, ctx, clusterName):
        requests = ctx.ec2.get_all_spot_instance_requests()
        return [x.id for x in requests if x.launch_group == clusterName]

    @classmethod
    def _createSecurityGroup(cls, ctx, name):
        # security group create/get. ssh + all ports open within the group
        try:
            web = ctx.ec2.create_security_group(name, 'Toil appliance security group')
        except EC2ResponseError as e:
            if e.status == 400 and 'already exists' in e.body:
                pass  # group exists- nothing to do
            else:
                raise
        else:
            for attempt in retry_ec2(retry_while=groupNotFound, retry_for=300):
                with attempt:
                    # open port 22 for ssh-ing
                    web.authorize(ip_protocol='tcp', from_port=22, to_port=22, cidr_ip='0.0.0.0/0')
            for attempt in retry_ec2(retry_while=groupNotFound, retry_for=300):
                with attempt:
                    # the following authorizes all port access within the web security group
                    web.authorize(ip_protocol='tcp', from_port=0, to_port=65535, src_group=web)
            for attempt in retry_ec2(retry_while=groupNotFound, retry_for=300):
                with attempt:
                    # open port 5050 for mesos web interface
                    web.authorize(ip_protocol='tcp', from_port=5050, to_port=5051, cidr_ip='0.0.0.0/0')

    @classmethod
    def _getProfileARN(cls, ctx, role):
        roleName = 'toil-appliance-' + role
        policy = dict(iam_full=iam_full_policy, ec2_full=ec2_full_policy,
                      s3_full=s3_full_policy, sbd_full=sdb_full_policy)
        ctx.setup_iam_ec2_role(role_name=roleName, policies=policy)

        try:
            profile = ctx.iam.get_instance_profile(roleName)
        except BotoServerError as e:
            if e.status == 404:
                profile = ctx.iam.create_instance_profile(roleName)
                profile = profile.create_instance_profile_response.create_instance_profile_result
            else:
                raise
        else:
            profile = profile.get_instance_profile_response.get_instance_profile_result
        profile = profile.instance_profile
        profile_arn = profile.arn

        if len(profile.roles) > 1:
                raise RuntimeError('Did not expect profile to contain more than one role')
        elif len(profile.roles) == 1:
            # this should be profile.roles[0].role_name
            if profile.roles.member.role_name == roleName:
                return profile_arn
            else:
                ctx.iam.remove_role_from_instance_profile(roleName,
                                                          profile.roles.member.role_name)
        ctx.iam.add_role_to_instance_profile(roleName, roleName)
        return profile_arn
