"""
    Copyright 2016 Inmanta

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    Contact: code@inmanta.com
"""

import base64
from concurrent.futures.thread import ThreadPoolExecutor
import datetime
import hashlib
import logging
import os
import random
import time

from tornado import gen, locks
from inmanta import env
from inmanta import methods
from inmanta import protocol
from inmanta.agent.handler import Commander
from inmanta.loader import CodeLoader
from inmanta.protocol import Scheduler, AgentEndPoint
from inmanta.resources import Resource
from inmanta.agent.cache import AgentCache
from inmanta.agent import config as cfg
from inmanta.agent.reporting import collect_report
from inmanta.agent.executor import ResourceScheduler


LOGGER = logging.getLogger(__name__)
GET_RESOURCE_BACKOFF = 5


class AgentInstance():

    def __init__(self, process, name: str, hostname: str):
        self.process = process
        self.name = name
        self.hostname = hostname

        # inherit
        self.ratelimiter = process.ratelimiter
        self.critical_ratelimiter = process.critical_ratelimiter
        self.dryrunlock = locks.Semaphore(1)

        self._env_id = process._env_id
        self.thread_pool = process.thread_pool
        self.sessionid = process.sessionid

        # init
        self._cache = AgentCache()
        self._nq = ResourceScheduler(self, self.process._env_id, name, self._cache, ratelimiter=self.ratelimiter)
        self._enabled = None

        # do regular deploys
        self._deploy_interval = cfg.agent_interval.get()
        self._splay_interval = cfg.agent_splay.get()
        self._splay_value = random.randint(0, self._splay_interval)

        self._getting_resources = False
        self._get_resource_timeout = 0

    def get_client(self):
        return self.process._client

    def get_hostname(self):
        return self.hostname

    def is_local(self):
        return self.get_client().node_name == self.hostname or self.hostname == "localhost"

    def is_enabled(self):
        return self._enabled is not None

    def add_future(self, future):
        self.process.add_future(future)

    def unpause(self):
        if self._enabled is not None:
            return 200, "already running"

        LOGGER.info("Agent assuming primary role for %s" % self.name)

        @gen.coroutine
        def action():
            yield self.get_latest_version_for_agent()
        self._enabled = action
        self.process._sched.add_action(action, self._deploy_interval, self._splay_value)
        return 200, "unpaused"

    def pause(self):
        if self._enabled is None:
            return 200, "already paused"

        LOGGER.info("Agent lost primary role for %s" % self.name)

        token = self._enabled
        self.process._sched.remove(token)
        self._enabled = None
        return 200, "paused"

    def notify_ready(self, resourceid):
        self._nq.notify_ready(resourceid)

    def _can_get_resources(self):
        if self._getting_resources:
            LOGGER.info("%s Attempting to get resource while get is in progress", self.name)
            return False
        if time.time() < self._get_resource_timeout:
            LOGGER.info("%s Attempting to get resources during backoff %g seconds left, last download took %d seconds",
                        self.name, self._get_resource_timeout - time.time(), self._get_resource_duration)
            return False
        return True

    @gen.coroutine
    def get_latest_version_for_agent(self):
        """
            Get the latest version for the given agent (this is also how we are notified)
        """
        if not self._can_get_resources():
            return
        with (yield self.critical_ratelimiter.acquire()):
            if not self._can_get_resources():
                return
            LOGGER.debug("Getting latest resources for %s" % self.name)
            self._getting_resources = True
            start = time.time()
            try:
                result = yield self.get_client().get_resources_for_agent(tid=self._env_id, agent=self.name)
            finally:
                self._getting_resources = False
            end = time.time()
            self._get_resource_duration = end - start
            self._get_resource_timeout = GET_RESOURCE_BACKOFF * self._get_resource_duration + end
            if result.code == 404:
                LOGGER.info("No released configuration model version available for agent %s", self.name)
            elif result.code != 200:
                LOGGER.warning("Got an error while pulling resources for agent %s. %s", self.name, result.result)

            else:
                restypes = set([res["id_fields"]["entity_type"] for res in result.result["resources"]])
                resources = []
                yield self.process._ensure_code(self._env_id, result.result["version"], restypes)
                try:
                    for res in result.result["resources"]:
                        data = res["fields"]
                        data["id"] = res["id"]
                        resource = Resource.deserialize(data)
                        resources.append(resource)
                        LOGGER.debug("Received update for %s", resource.id)
                except TypeError as e:
                    LOGGER.error("Failed to receive update", e)

                self._nq.reload(resources)

    @gen.coroutine
    def dryrun(self, id, version):

        self.add_future(self.do_run_dryrun(version, id))

        return 200

    @gen.coroutine
    def do_run_dryrun(self, version, id):
        with (yield self.dryrunlock.acquire()):
            with (yield self.ratelimiter.acquire()):
                result = yield self.get_client().get_resources_for_agent(tid=self._env_id, agent=self.name, version=version)
                if result.code == 404:
                    LOGGER.warn("Version %s does not exist, can not run dryrun", version)
                    return

                elif result.code != 200:
                    LOGGER.warning("Got an error while pulling resources for agent %s and version %s", self.name, version)
                    return

                resources = result.result["resources"]

                restypes = set([res["id_fields"]["entity_type"] for res in resources])

                # TODO: handle different versions for dryrun and deploy!
                yield self.process._ensure_code(self._env_id, version, restypes)

                self._cache.open_version(version)

                for res in resources:
                    provider = None
                    try:
                        data = res["fields"]
                        data["id"] = res["id"]
                        resource = Resource.deserialize(data)
                        LOGGER.debug("Running dryrun for %s", resource.id)

                        try:
                            provider = Commander.get_provider(self._cache, self, resource)
                            provider.set_cache(self._cache)
                        except Exception:
                            LOGGER.exception("Unable to find a handler for %s" % resource.id)
                            self._client.dryrun_update(tid=self._env_id, id=id, resource=res["id"],
                                                       changes={}, log_msg="No handler available")
                            continue

                        results = yield self.thread_pool.submit(provider.execute, resource, dry_run=True)
                        yield self.get_client().dryrun_update(tid=self._env_id, id=id, resource=res["id"],
                                                              changes=results["changes"], log_msg=results["log_msg"])

                    except TypeError:
                        LOGGER.exception("Unable to process resource for dryrun.")
                        return 500
                    finally:
                        if provider is not None:
                            provider.close()

                self._cache.close_version(version)

    @gen.coroutine
    def do_restore(self, restore_id, snapshot_id, resources):
        with (yield self.ratelimiter.acquire()):

            LOGGER.info("Start a restore %s", restore_id)

            yield self.process._ensure_code(self._env_id, resources[0][1]["id_fields"]["version"],
                                            [res[1]["id_fields"]["entity_type"] for res in resources])

            version = resources[0][1]["id_fields"]["version"]
            self._cache.open_version(version)

            for restore, resource in resources:
                start = datetime.datetime.now()
                provider = None
                try:
                    data = resource["fields"]
                    data["id"] = resource["id"]
                    resource_obj = Resource.deserialize(data)
                    provider = Commander.get_provider(self._cache, self, resource_obj)
                    provider.set_cache(self._cache)

                    if not hasattr(resource_obj, "allow_restore") or not resource_obj.allow_restore:
                        yield self.get_client().update_restore(tid=self._env_id,
                                                               id=restore_id,
                                                               resource_id=str(resource_obj.id),
                                                               start=start,
                                                               stop=datetime.datetime.now(),
                                                               success=False,
                                                               error=False,
                                                               msg="Resource %s does not allow restore" % resource["id"])
                        continue

                    try:
                        yield self.thread_pool.submit(provider.restore, resource_obj, restore["content_hash"])
                        yield self.get_client().update_restore(tid=self._env_id, id=restore_id,
                                                               resource_id=str(resource_obj.id),
                                                               success=True, error=False,
                                                               start=start, stop=datetime.datetime.now(), msg="")
                    except NotImplementedError:
                        yield self.get_client().update_restore(tid=self._env_id, id=restore_id,
                                                               resource_id=str(resource_obj.id),
                                                               success=False, error=False,
                                                               start=start, stop=datetime.datetime.now(),
                                                               msg="The handler for resource "
                                                               "%s does not support restores" % resource["id"])

                except Exception:
                    LOGGER.exception("Unable to find a handler for %s", resource["id"])
                    yield self.get_client().update_restore(tid=self._env_id, id=restore_id,
                                                           resource_id=resource_obj.id.resource_str(),
                                                           success=False, error=False,
                                                           start=start, stop=datetime.datetime.now(),
                                                           msg="Unable to find a handler to restore a snapshot of resource %s" %
                                                           resource["id"])
                finally:
                    if provider is not None:
                        provider.close()
            self._cache.close_version(version)

            return 200

    @gen.coroutine
    def do_snapshot(self, snapshot_id, resources):
        with (yield self.ratelimiter.acquire()):
            LOGGER.info("Start snapshot %s", snapshot_id)

            yield self.process._ensure_code(self._env_id, resources[0]["id_fields"]["version"],
                                            [res["id_fields"]["entity_type"] for res in resources])

            version = resources[0]["id_fields"]["version"]
            self._cache.open_version(version)

            for resource in resources:
                start = datetime.datetime.now()
                provider = None
                try:
                    data = resource["fields"]
                    data["id"] = resource["id"]
                    resource_obj = Resource.deserialize(data)
                    provider = Commander.get_provider(self._cache, self, resource_obj)
                    provider.set_cache(self._cache)

                    if not hasattr(resource_obj, "allow_snapshot") or not resource_obj.allow_snapshot:
                        yield self.get_client().update_snapshot(tid=self._env_id, id=snapshot_id,
                                                                resource_id=resource_obj.id.resource_str(), snapshot_data="",
                                                                start=start, stop=datetime.datetime.now(), size=0,
                                                                success=False, error=False,
                                                                msg="Resource %s does not allow snapshots" % resource["id"])
                        continue

                    try:
                        result = yield self.thread_pool.submit(provider.snapshot, resource_obj)
                        if result is not None:
                            sha1sum = hashlib.sha1()
                            sha1sum.update(result)
                            content_id = sha1sum.hexdigest()
                            yield self.get_client().upload_file(id=content_id, content=base64.b64encode(result).decode("ascii"))

                            yield self.get_client().update_snapshot(tid=self._env_id, id=snapshot_id,
                                                                    resource_id=resource_obj.id.resource_str(),
                                                                    snapshot_data=content_id,
                                                                    start=start, stop=datetime.datetime.now(),
                                                                    size=len(result), success=True, error=False,
                                                                    msg="")
                        else:
                            raise Exception("Snapshot returned no data")

                    except NotImplementedError:
                        yield self.get_client().update_snapshot(tid=self._env_id, id=snapshot_id, error=False,
                                                                resource_id=resource_obj.id.resource_str(),
                                                                snapshot_data="",
                                                                start=start, stop=datetime.datetime.now(),
                                                                size=0, success=False,
                                                                msg="The handler for resource "
                                                                "%s does not support snapshots" % resource["id"])
                    except Exception:
                        LOGGER.exception("An exception occurred while creating the snapshot of %s", resource["id"])
                        yield self.get_client().update_snapshot(tid=self._env_id, id=snapshot_id, snapshot_data="",
                                                                resource_id=resource_obj.id.resource_str(), error=True,
                                                                start=start,
                                                                stop=datetime.datetime.now(),
                                                                size=0, success=False,
                                                                msg="The handler for resource "
                                                                "%s does not support snapshots" % resource["id"])

                except Exception:
                    LOGGER.exception("Unable to find a handler for %s", resource["id"])
                    yield self.get_client().update_snapshot(tid=self._env_id,
                                                            id=snapshot_id, snapshot_data="",
                                                            resource_id=resource_obj.id.resource_str(), error=False,
                                                            start=start, stop=datetime.datetime.now(),
                                                            size=0, success=False,
                                                            msg="Unable to find a handler for %s" % resource["id"])
                finally:
                    if provider is not None:
                        provider.close()

            self._cache.close_version(version)
            return 200

    @gen.coroutine
    def get_facts(self, resource):
        with (yield self.ratelimiter.acquire()):
            yield self.process._ensure_code(self._env_id, resource["id_fields"]["version"],
                                            [resource["id_fields"]["entity_type"]])

            provider = None
            try:
                data = resource["fields"]
                data["id"] = resource["id"]
                resource_obj = Resource.deserialize(data)

                version = resource_obj.version

                try:
                    self._cache.open_version(version)
                    provider = Commander.get_provider(self._cache, self, resource_obj)
                    provider.set_cache(self._cache)
                    result = yield self.thread_pool.submit(provider.check_facts, resource_obj)
                    parameters = [{"id": name, "value": value, "resource_id": resource_obj.id.resource_str(), "source": "fact"}
                                  for name, value in result.items()]
                    yield self.get_client().set_parameters(tid=self._env_id, parameters=parameters)

                except Exception:
                    LOGGER.exception("Unable to retrieve fact")
                finally:
                    self._cache.close_version(version)

            except Exception:
                LOGGER.exception("Unable to find a handler for %s", resource["id"])
                return 500
            finally:
                if provider is not None:
                    provider.close()
            return 200


class Agent(AgentEndPoint):
    """
        An agent to enact changes upon resources. This agent listens to the
        message bus for changes.
    """

    def __init__(self, io_loop, hostname=None, agent_map=None, code_loader=True, env_id=None, poolsize=1, cricital_pool_size=5):
        super().__init__("agent", io_loop, timeout=cfg.server_timeout.get(), reconnect_delay=cfg.agent_reconnect_delay.get())

        self.poolsize = poolsize
        self.ratelimiter = locks.Semaphore(poolsize)
        self.critical_ratelimiter = locks.Semaphore(cricital_pool_size)
        self._sched = Scheduler(io_loop=self._io_loop)
        self.thread_pool = ThreadPoolExecutor(poolsize)

        if agent_map is None:
            agent_map = cfg.agent_map.get()

        self.agent_map = agent_map
        self._storage = self.check_storage()

        if env_id is None:
            env_id = cfg.environment.get()
            if env_id is None:
                raise Exception("The agent requires an environment to be set.")
        self.set_environment(env_id)

        self._instances = {}

        if code_loader:
            self._env = env.VirtualEnv(self._storage["env"])
            self._env.use_virtual_env()
            self._loader = CodeLoader(self._storage["code"])
        else:
            self._loader = None

        if hostname is not None:
            self.add_end_point_name(hostname)

        else:
            # load agent names from the config file
            agent_names = cfg.agent_names.get()
            if agent_names is not None:
                names = [x.strip() for x in agent_names.split(",")]
                for name in names:
                    if "$" in name:
                        name = name.replace("$node-name", self.node_name)

                    self.add_end_point_name(name)

    def add_end_point_name(self, name):
        AgentEndPoint.add_end_point_name(self, name)

        hostname = name
        if name in self.agent_map:
            hostname = self.agent_map[name]

        self._instances[name] = AgentInstance(self, name, hostname)

    def unpause(self, name):
        if name not in self._instances:
            return 404, "No such agent"

        return self._instances[name].unpause()

    def pause(self, name):
        if name not in self._instances:
            return 404, "No such agent"

        return self._instances[name].pause()

    @protocol.handle(methods.AgentState.set_state)
    @gen.coroutine
    def set_state(self, agent, enabled):
        if enabled:
            return self.unpause(agent)
        else:
            return self.pause(agent)

    @gen.coroutine
    def on_reconnect(self):
        for name in self._instances.keys():
            result = yield self._client.get_state(tid=self._env_id, sid=self.sessionid, agent=name)
            if result.code == 200:
                state = result.result
                if "enabled" in state and isinstance(state["enabled"], bool):
                    self.set_state(name, state["enabled"])
                else:
                    LOGGER.warn("Server reported invalid state %s" % (repr(state)))
            else:
                LOGGER.warn("could not get state from the server")

    @gen.coroutine
    def get_latest_version(self):
        """
            Get the latest version of managed resources for all agents
        """
        for agent in self._instances.values():
            yield agent.get_latest_version_for_agent()

    @gen.coroutine
    def _ensure_code(self, environment, version, resourcetypes):
        """
            Ensure that the code for the given environment and version is loaded
        """
        if self._loader is not None:
            for rt in resourcetypes:
                result = yield self._client.get_code(environment, version, rt)

                if result.code == 200:
                    for key, source in result.result["sources"].items():
                        try:
                            LOGGER.debug("Installing handler %s for %s", rt, source[1])
                            yield self._install(key, source)
                            LOGGER.debug("Installed handler %s for %s", rt, source[1])
                        except Exception:
                            LOGGER.exception("Failed to install handler %s for %s", rt, source[1])

    @gen.coroutine
    def _install(self, key, source):
        yield self.thread_pool.submit(self._env.install_from_list, source[3], True)
        yield self.thread_pool.submit(self._loader.deploy_version, key, source)

    @protocol.handle(methods.AgentState.trigger)
    @gen.coroutine
    def trigger_update(self, tid, id):
        """
            Trigger an update
        """
        if id not in self._instances:
            return 200

        if not self._instances[id].is_enabled():
            return 500, "Agent is not _enabled"

        LOGGER.info("Agent %s got a trigger to update in environment %s", id, tid)
        future = self._instances[id].get_latest_version_for_agent()
        self.add_future(future)
        return 200

    @protocol.handle(methods.AgentResourceEvent.resource_event)
    @gen.coroutine
    def resource_event(self, tid, id: str, resource: str, state: str):
        if tid != self._env_id:
            LOGGER.warn("received unexpected resource event: tid: %s, agent: %s, resource: %s, state: %s, tid unknown",
                        tid, id, resource, state)
            return 200

        if id not in self._instances:
            LOGGER.warn("received unexpected resource event: tid: %s, agent: %s, resource: %s, state: %s, agent unknown",
                        tid, id, resource, state)
            return 200

        if state != "deployed":
            LOGGER.warn("received unexpected resource event: tid: %s, agent: %s, resource: %s, state: %s",
                        tid, id, resource, state)
        else:
            LOGGER.debug("Agent %s got a resource event: tid: %s, agent: %s, resource: %s, state: %s",
                         tid, id, resource, state)
            self._instances[id].notify_ready(resource)

        return 200

    @protocol.handle(methods.AgentDryRun.do_dryrun)
    @gen.coroutine
    def run_dryrun(self, tid, id, agent, version):
        """
           Run a dryrun of the given version
        """
        assert tid == self._env_id

        if agent not in self._instances:
            return 200

        LOGGER.info("Agent %s got a trigger to run dryrun %s for version %s in environment %s", agent, id, version, tid)

        return (yield self._instances[agent].dryrun(id, version))

    def check_storage(self):
        """
            Check if the server storage is configured and ready to use.
        """

        state_dir = cfg.state_dir.get()

        if not os.path.exists(state_dir):
            os.mkdir(state_dir)

        agent_state_dir = os.path.join(state_dir, "agent")

        if not os.path.exists(agent_state_dir):
            os.mkdir(agent_state_dir)

        dir_map = {"agent": agent_state_dir}

        code_dir = os.path.join(agent_state_dir, "code")
        dir_map["code"] = code_dir
        if not os.path.exists(code_dir):
            os.mkdir(code_dir)

        env_dir = os.path.join(agent_state_dir, "env")
        dir_map["env"] = env_dir
        if not os.path.exists(env_dir):
            os.mkdir(env_dir)

        return dir_map

    @protocol.handle(methods.AgentRestore.do_restore)
    @gen.coroutine
    def do_restore(self, tid, agent, restore_id, snapshot_id, resources):
        """
            Restore a snapshot
        """
        if agent not in self._instances:
            return 200

        return (yield self._instances[agent].do_restore(restore_id, snapshot_id, resources))

    @protocol.handle(methods.AgentSnapshot.do_snapshot)
    @gen.coroutine
    def do_snapshot(self, tid, agent, snapshot_id, resources):
        """
            Create a snapshot of stateful resources managed by this agent
        """
        if agent not in self._instances:
            return 200

        return (yield self._instances[agent].do_snapshot(snapshot_id, resources))

    @protocol.handle(methods.AgentParameterMethod.get_parameter)
    @gen.coroutine
    def get_facts(self, tid, agent, resource):
        if agent not in self._instances:
            return 200

        return (yield self._instances[agent].get_facts(resource))

    @protocol.handle(methods.AgentReporting.get_status)
    @gen.coroutine
    def get_status(self):
        return 200, collect_report(self)
