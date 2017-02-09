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

from _collections import defaultdict

from inmanta import resources


class Node(object):
    """Convenient graph representation."""

    def __init__(self, resources, mytype):
        self.resources = resources
        self.type = mytype
        self._depth = {}

    def index(self, index):
        for resc in self.resources:
            index[resc.id.resource_str()] = self

    def link(self, index):
        self.requires = set()
        for resc in self.resources:
            for req in resc.requires:
                self.requires.add(index[req.resource_str()])

    def depth_for(self, mytype):
        if mytype in self._depth:
            return self._depth[mytype]

        base = max([child.depth_for(mytype) for child in self.requires], default=0)

        if self.type == mytype:
            base += 1

        self._depth[mytype] = base
        return base

    def __repr__(self):
        return "N:" + "|".join([str(x) for x in self.resources])

    def short_id(self):
        names = [r.id.get_attribute_value() for r in self.resources]
        return "".join(sorted(names))


def group(inr):
    types = set([res.id.get_entity_type() for res in inr])
    types = [t for t in types if resources.resource.get_grouping_gain(t) > 0]
    types = sorted(types, key=lambda t: -resources.resource.get_grouping_gain(t))

    wrapped = [Node([n], n.id.get_entity_type()) for n in inr]

    for mytype in types:
        wrapped = group_for_type(wrapped, mytype)

    return rebuild_graph(wrapped)


def rebuild_graph(graph):

    # rebuild
    index = {}
    for node in graph:
        node.index(index)

    for node in graph:
        node.link(index)

    return graph


def group_for_type(graph, mytype):
    """
        Optimal (smallest number of groups) grouping for this type of nodes

        1-determined depth,
          where depth is the largest number of nodes of this type on any path
          from any leaf node up to this node (including the node itself)
        2-group by depth
        3-merge all nodes on the same depth

        explanation

        1- nodes can only be grouped if this doesn't create a dependency cycle (rendering the graph impossible to execute)
        2- the minimal (optimal) number of groups for any type is the depth of the deepest node
          proof: if there were less groups,
           two nodes on the path to the deepest node would be in the same group,
           which would introduce a cycle
        3- merging nodes at the same depth can not introduce a cycle
          proof: if two nodes are at the same depth,
            there is no directed path between then, and thus they can not form a cycle
            if node A would be on a directed path to B,
            the depth of B would be at least one higher than that of A (by definition of depth)
    """

    # rebuild
    rebuild_graph(graph)

    nodes = [n for n in graph if n.type == mytype]
    others = [n for n in graph if n.type != mytype]

    groups = defaultdict(lambda: [])

    for node in nodes:
        groups[node.depth_for(mytype)].append(node)

    for k, nodes in groups.items():
        others.append(Node([r for node in nodes for r in node.resources], mytype))

    return others
