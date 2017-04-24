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

import datetime
import logging
import uuid

from tornado import gen
from tornado.concurrent import Future

from inmanta import const
from inmanta.agent import handler, grouping
from inmanta.agent.cache import AgentCache
from inmanta.agent.grouping import Node


LOGGER = logging.getLogger(__name__)


class ResourceActionResult(object):

    def __init__(self, success, receive_events, cancel):
        self.success = success
        self.receive_events = receive_events
        self.cancel = cancel

    def __add__(self, other):
        return ResourceActionResult(self.success and other.success,
                                    self.receive_events or other.receive_events,
                                    self.cancel or other.cancel)

    def __str__(self, *args, **kwargs):
        return "%r %r %r" % (self.success, self.receive_events, self.cancel)


class ResourceAction(object):

    def __init__(self, scheduler, resource, gid, requires):
        """
            :param gid A unique identifier to identify a deploy. This is local to this agent.
            :param resource The resource to handle
            :param requires A list of identifiers for the requirements
        """
        self.scheduler = scheduler
        self.resource = resource
        if self.resource is not None:
            self.resource_id = resource.id
        self.future = Future()
        self.running = False
        self.gid = gid
        self.status = None
        self.change = None
        self.changes = None
        self.requires = requires

    def is_running(self):
        return self.running

    def is_done(self):
        return self.future.done()

    def cancel(self):
        if not self.is_running() and not self.is_done():
            LOGGER.info("Cancelled deploy of %s %s", self.gid, self.resource)
            self.future.set_result(ResourceActionResult(False, False, True))

    @gen.coroutine
    def _execute(self, ctx: handler.HandlerContext, events: dict, cache: AgentCache) -> (bool, bool):
        """
            :param ctx The context to use during execution of this deploy
            :param events Possible events that are available for this resource
            :param cache The cache instance to use
            :return (success, send_event) Return whether the execution was successful and whether a change event should be sent
                                          to provides of this resource.
        """
        ctx.debug("Start deploy %(deploy_id)s of resource %(resource_id)s",
                  deploy_id=self.gid, resource_id=self.resource_id)
        provider = None

        try:
            provider = handler.Commander.get_provider(cache, self.scheduler.agent, self.resource)
            provider.set_cache(cache)
        except Exception:
            if provider is not None:
                provider.close()

            cache.close_version(self.resource.id.version)
            ctx.set_status(const.ResourceState.unavailable)
            ctx.exception("Unable to find a handler for %(resource_id)s", resource_id=str(self.resource.id))
            return False, False

        yield self.scheduler.agent.thread_pool.submit(provider.execute, ctx, self.resource)

        if ctx.status is not const.ResourceState.deployed:
            provider.close()
            cache.close_version(self.resource.id.version)
            return False, False

        if len(events) > 0 and provider.can_process_events():
            ctx.info("Sending events to %(resource_id)s because of modified dependencies", resource_id=str(self.resource.id))
            yield self.scheduler.agent.thread_pool.submit(provider.process_events, ctx, self.resource, events)

        provider.close()
        cache.close_version(self.resource_id.version)

        send_event = (ctx.changed and hasattr(self.resource, "send_event") and self.resource.send_event)
        return True, send_event

    @gen.coroutine
    def execute(self, dummy, generation, cache):
        LOGGER.log(const.LogLevel.TRACE.value, "Entering %s %s", self.gid, self.resource)
        cache.open_version(self.resource.id.version)

        self.dependencies = [generation[x] for x in self.requires]
        waiters = [x.future for x in self.dependencies]
        waiters.append(dummy.future)
        results = yield waiters

        with (yield self.scheduler.ratelimiter.acquire()):
            start = datetime.datetime.now()
            ctx = handler.HandlerContext(self.resource)

            LOGGER.info("start run %s %s", self.gid, self.resource.id)
            ctx.debug("start run for resource with id %(deploy_id)s", deploy_id=self.gid)
            self.running = True
            if self.is_done():
                # Action is cancelled
                LOGGER.log(const.LogLevel.TRACE.value, "%s %s is no longer active" % (self.gid, self.resource))
                self.running = False
                ctx.set_status(const.ResourceState.cancelled)
                return

            result = sum(results, ResourceActionResult(True, False, False))

            if result.cancel:
                # self.running will be set to false when self.cancel is called
                # Only happens when global cancel has not cancelled us but our predecessors have already been cancelled
                ctx.set_status(const.ResourceState.cancelled)
                return

            if not result.success:
                ctx.set_status(const.ResourceState.skipped)
                success = False
                send_event = False
            else:
                if result.receive_events:
                    received_events = {x.resource_id: dict(status=x.status, change=x.change, changes=x.changes)
                                       for x in self.dependencies}
                else:
                    received_events = {}
                success, send_event = yield self._execute(ctx=ctx, events=received_events, cache=cache)

            LOGGER.info("end run %s", self.resource)

            end = datetime.datetime.now()
            result = yield self.scheduler.get_client().resource_action_update(tid=self.scheduler._env_id,
                                                                              resource_ids=[str(self.resource.id)],
                                                                              action_id=ctx.action_id,
                                                                              action=const.ResourceAction.deploy,
                                                                              started=start, finished=end, status=ctx.status,
                                                                              changes={str(self.resource.id): ctx.changes},
                                                                              messages=ctx.logs, change=ctx.change,
                                                                              send_events=send_event)
            if result.code != 200:
                LOGGER.error("Resource status update failed %s", result.result)

            self.status = ctx.status
            self.change = ctx.change
            self.changes = ctx.changes
            self.future.set_result(ResourceActionResult(success, send_event, False))
            self.running = False

    def __str__(self):
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


class GroupedResourceAction(ResourceAction):

    def __init__(self, scheduler, node: Node, gid):
        super(GroupedResourceAction, self).__init__(scheduler, node.resources[0], gid, node.get_requires_ids())
        self.resources = node.resources

    @gen.coroutine
    def _execute(self, ctx: handler.HandlerContext, events: dict, cache: AgentCache) -> (bool, bool):
        """
            :param ctx The context to use during execution of this deploy
            :param events Possible events that are available for this resource
            :param cache The cache instance to use
            :return (success, send_event) Return whether the execution was successful and whether a change event should be sent
                                          to provides of this resource.
        """
        ctx.debug("Start deploy %(deploy_id)s of resource %(resource_id)s",
                  deploy_id=self.gid, resource_id=self.resource_id)
        provider = None

        try:
            provider = handler.Commander.get_provider(cache, self.scheduler.agent, self.resource)
            provider.set_cache(cache)
        except Exception:
            if provider is not None:
                provider.close()

            cache.close_version(self.resource.id.version)
            ctx.set_status(const.ResourceState.unavailable)
            ctx.exception("Unable to find a handler for %(resource_id)s", resource_id=str(self.resource.id))
            return False, False

        yield self.scheduler.agent.thread_pool.submit(provider.execute_group, ctx, self.resources)

        if ctx.status is not const.ResourceState.deployed:
            provider.close()
            cache.close_version(self.resource.id.version)
            return False, False

        if len(events) > 0 and provider.can_process_events():
            ctx.info("Sending events to %(resource_id)s because of modified dependencies", resource_id=str(self.resource.id))
            yield self.scheduler.agent.thread_pool.submit(provider.process_events, ctx, self.resource, events)

        provider.close()
        cache.close_version(self.resource_id.version)

        send_event = (ctx.changed and hasattr(self.resource, "send_event") and self.resource.send_event)
        return True, send_event


class RemoteResourceAction(ResourceAction):

    def __init__(self, scheduler, resource_id, gid):
        super(RemoteResourceAction, self).__init__(scheduler, None, gid, [])
        self.resource_id = resource_id

    @gen.coroutine
    def execute(self, dummy, generation, cache):
        yield dummy.future
        try:
            result = yield self.scheduler.get_client().get_resource(self.scheduler.agent._env_id, str(self.resource_id),
                                                                    logs=True, log_action=const.ResourceAction.deploy,
                                                                    log_limit=1)
            status = const.ResourceState[result.result["resource"]["status"]]
            if status == const.ResourceState.available or self.future.done():
                # wait for event
                pass
            else:
                if status == const.ResourceState.deployed:
                    success = True
                else:
                    success = False

                send_event = False
                if "logs" in result.result and len(result.result["logs"]) > 0:
                    log = result.result["logs"][0]
                    self.change = const.Change[log["change"]]
                    if str(self.resource_id) in log["changes"]:
                        self.changes = log["changes"]
                    else:
                        self.changes = {}
                    self.status = status
                    send_event = log["send_event"]

                self.future.set_result(ResourceActionResult(success, send_event, False))

            self.running = False
        except Exception:
            LOGGER.exception("could not get status for remote resource")

    def notify(self, send_events, status, change, changes):
        if not self.future.done():
            self.status = status
            self.change = change
            self.changes = changes
            self.future.set_result(ResourceActionResult(True, send_events, False))


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

        grouped = grouping.group(resources, self.name)

        def get_resource_action(node):
            if node.is_CAD():
                # cross agent dependency
                myid = node.id
                ra = RemoteResourceAction(self, myid, gid)
                self.cad[str(myid)] = ra
                return (myid.resource_str(), ra)
            elif len(node.resources) == 1:
                # normal node
                resource = node.resources[0]
                return (resource.id.resource_str(), ResourceAction(self, resource, gid, node.get_requires_ids()))
            else:
                # grouped execution
                return (node.resource_str(), GroupedResourceAction(self, node, gid))

        self.generation = {k: v for k, v in [get_resource_action(r) for r in grouped]}

        dummy = ResourceAction(self, None, gid, [])
        for r in self.generation.values():
            r.execute(dummy, self.generation, self.cache)
        dummy.future.set_result(ResourceActionResult(True, False, False))

    def notify_ready(self, resourceid, send_events, state, change, changes):
        if resourceid not in self.cad:
            LOGGER.warning("received CAD notification that was not required, %s", resourceid)
            return
        self.cad[resourceid].notify(send_events, state, change, changes)

    def dump(self):
        print("Waiting:")
        for r in self.generation.values():
            print(r.long_string())
        print("Ready to run:")
        for r in self.queue:
            print(r.long_string())

    def get_client(self):
        return self.agent.get_client()
