"""
    Copyright 2017 Inmanta

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

import logging

from tornado.concurrent import Future
from tornado import gen, locks

from inmanta.agent.handler import Commander
import uuid
from inmanta.resources import Resource


LOGGER = logging.getLogger(__name__)


class ResourceActionResult(object):

    def __init__(self, success: bool, reload: bool, cancel: bool) -> None:
        self.success = success
        self.reload = reload
        self.cancel = cancel

    def __add__(self, other: "ResourceActionResult") -> "ResourceActionResult":
        return ResourceActionResult(self.success and other.success,
                                    self.reload or other.reload,
                                    self.cancel or other.cancel)

    def __str__(self) -> str:
        return "%r %r %r" % (self.success, self.reload, self.cancel)


class ResourceAction(object):

    def __init__(self, scheduler: "ResourceScheduler", resource: Resource, gid: uuid.UUID) -> None:
        self.scheduler = scheduler
        self.resource = resource
        self.future = Future() # type: Future[ResourceActionResult]
        self.running = False
        self.gid = gid

    def is_running(self) -> bool:
        return self.running

    def is_done(self) -> bool:
        return self.future.done()

    def cancel(self) -> None:
        if not self.is_running() and not self.is_done():
            LOGGER.info("Cancelled deploy of %s %s", self.gid, self.resource)
            self.future.set_result(ResourceActionResult(False, False, True))

    @gen.coroutine
    def __complete(self, success, reload, changes={}, status="", log_msg=""):
        action = "deploy"
        if status == "skipped" or status == "dry" or status == "deployed":
            level = "INFO"
        else:
            level = "ERROR"

        yield self.scheduler.get_client().resource_updated(tid=self.scheduler._env_id,
                                                           id=str(self.resource.id),
                                                           level=level,
                                                           action=action,
                                                           status=status,
                                                           message="%s: %s" % (status, log_msg),
                                                           extra_data=changes)

        self.future.set_result(ResourceActionResult(success, reload, False))
        LOGGER.info("end run %s" % self.resource)
        self.running = False

    @gen.coroutine
    def execute(self, dummy, generation, cache):
        LOGGER.log(3, "Entering %s %s", self.gid, self.resource)
        cache.open_version(self.resource.id.version)

        self.dependencies = [generation[x.resource_str()] for x in self.resource.requires]
        waiters = [x.future for x in self.dependencies]
        waiters.append(dummy.future)
        results = yield waiters

        with (yield self.scheduler.ratelimiter.acquire()):
            LOGGER.info("run %s %s" % (self.gid, self.resource))
            self.running = True
            if self.is_done():
                # Action is cancelled
                LOGGER.log(3, "%s %s is no longer active" % (self.gid, self.resource))
                self.running = False
                return

            result = sum(results, ResourceActionResult(True, False, False))

            if result.cancel:
                return

            if not result.success:
                yield self.__complete(False, False, changes={}, status="skipped")
            else:
                resource = self.resource

                LOGGER.debug("Start deploy of resource %s %s" % (self.gid, resource))
                provider = None

                try:
                    provider = Commander.get_provider(cache, self.scheduler.agent, resource)
                    provider.set_cache(cache)
                except Exception:
                    if provider is not None:
                        provider.close()

                    cache.close_version(self.resource.id.version)
                    LOGGER.exception("Unable to find a handler for %s" % resource.id)
                    return (yield self.__complete(False, False, changes={}, status="unavailable"))

                results = yield self.scheduler.agent.thread_pool.submit(provider.execute, resource)

                status = results["status"]
                if status == "failed" or status == "skipped":
                    provider.close()
                    cache.close_version(self.resource.id.version)
                    return (yield self.__complete(False, False,
                                                  changes=results["changes"],
                                                  status=results["status"],
                                                  log_msg=results["log_msg"]))

                if result.reload and provider.can_reload():
                    LOGGER.warning("Reloading %s because of updated dependencies" % resource.id)
                    yield self.scheduler.agent.thread_pool.submit(provider.do_reload, resource)

                provider.close()
                cache.close_version(self.resource.id.version)

                reload = results["changed"] and hasattr(resource, "reload") and resource.reload
                return (yield self.__complete(True, reload=reload, changes=results["changes"],
                                              status=results["status"], log_msg=results["log_msg"]))

                LOGGER.debug("Finished %s %s" % (self.gid, resource))

    def __str__(self, *args, **kwargs):
        if self.resource is None:
            return "DUMMY"

        status = ""
        if self.is_done():
            status = "Done"
        elif self.is_running():
            status = "Running"

        return self.resource.id.resource_str() + status

    def long_string(self):
        return "%s awaits %s" % (self.resource.id.resource_str(), " ".join([str(aw) for aw in self.dependencies]))


class RemoteResourceAction(ResourceAction):

    def __init__(self, scheduler, resource_id, gid):
        super(RemoteResourceAction, self).__init__(scheduler, None, gid)
        self.resource_id = resource_id

    @gen.coroutine
    def execute(self, dummy, generation, cache):
        yield dummy.future
        try:
            result = yield self.scheduler.get_client().get_resource(self.scheduler.agent._env_id,
                                                                    str(self.resource_id), status=True)
            status = result.result['status']
            if status == '' or self.future.done():
                # wait for event
                pass
            elif status == "deployed":
                # TODO: remote reload propagation
                self.future.set_result(ResourceActionResult(True, False, False))
            else:
                self.future.set_result(ResourceActionResult(False, False, False))
            self.running = False
        except Exception:
            LOGGER.exception("could not get status for remote resource")

    def notify(self):
        if not self.future.done():
            self.future.set_result(ResourceActionResult(True, False, False))


class ResourceScheduler(object):

    def __init__(self, agent, env_id, name, cache, ratelimiter):
        self.generation = {}
        self.cad = {}
        self._env_id = env_id
        self.agent = agent
        self.cache = cache
        self.name = name
        self.ratelimiter = ratelimiter
        self.version = 0

    def reload(self, resources):
        version = resources[0].id.get_version

        self.version = version

        for ra in self.generation.values():
            ra.cancel()

        gid = uuid.uuid4()
        self.generation = {r.id.resource_str(): ResourceAction(self, r, gid) for r in resources}

        cross_agent_dependencies = [q for r in resources for q in r.requires if q.get_agent_name() != self.name]
        for cad in cross_agent_dependencies:
            ra = RemoteResourceAction(self, cad, gid)
            self.cad[str(cad)] = ra
            self.generation[cad.resource_str()] = ra

        dummy = ResourceAction(self, None, gid)
        for r in self.generation.values():
            r.execute(dummy, self.generation, self.cache)
        dummy.future.set_result(ResourceActionResult(True, False, False))

    def notify_ready(self, resourceid):
        if resourceid not in self.cad:
            LOGGER.warning("received CAD notification that was not required, %s", resourceid)
            return
        self.cad[resourceid].notify()

    def dump(self):
        print("Waiting:")
        for r in self.generation.values():
            print(r.long_string())
        print("Ready to run:")
        for r in self.queue:
            print(r.long_string())

    def get_client(self):
        return self.agent.get_client()
