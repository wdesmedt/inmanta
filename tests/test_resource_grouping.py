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
import re

from inmanta.resources import resource, Resource
from inmanta.agent import grouping


@resource("test::Alpha", agent="agent", id_attribute="name", grouping_gain=120)
class AlphaResource(Resource):
    fields = ("agent", "name")


@resource("test::Beta", agent="agent", id_attribute="name", grouping_gain=10)
class BetaResource(Resource):
    fields = ("agent", "name")


@resource("test::Gamma", agent="agent", id_attribute="name")
class GammaResource(Resource):
    fields = ("agent", "name")


def expandToGraph(inp):
    """expect graph input in the form
            A1: B1 B2
    """
    lines = inp.split("\n")

    all_nodes = set()
    parts = {}
    for line in lines:
        k, v = line.split(": ")
        v = v.split(" ")
        k = k.strip()
        if k in parts:
            raise Exception("Bad test case")
        parts[k] = v
        all_nodes.add(k)
        all_nodes.update(set(v))

    terminals = all_nodes.difference(parts.keys())

    for t in terminals:
        parts[t] = []

    out = []

    types = {"A": "test::Alpha", "B": "test::Beta", "C": "test::Gamma"}

    def id_for(k):
        mytype = types[k[0]]
        return '%s[agent1,name=%s],v=5' % (mytype, k)

    for k, vs in parts.items():

        out.append({
            'name': k,
            'agent': 'agent1',
            'id': id_for(k),
            'requires': [id_for(val) for val in vs],
        })

    return out

escape = r'[[,\]:]'
cut = r'.*name=(\w*)].*'


def dot_escape(name):
    return re.match(cut, name).groups()[0]


def dot_out(graph):
    out = ""

    for res in graph:
        for req in res["requires"]:
            out = out + "%s -> %s\n" % (dot_escape(res["id"]), dot_escape(req))

    return "digraph test {\n %s }" % out


def deserialize(resources):
    return [Resource.deserialize(data) for data in resources]


def dot_out_res(graph):
    out = ""

    for res in graph:
        for req in res.requires:
            out = out + "%s -> %s\n" % (res.name, req.get_attribute_value())

    return "digraph test {\n %s }" % out


def dot_out_mg(graph):
    out = ""

    for res in graph:
        for req in res.requires:
            out = out + "%s -> %s\n" % (res.short_id(), req.short_id())

    return "digraph test {\n %s }" % out


def assertGraph(graph, expected):
    lines = ["%s: %s" % (f.short_id(), t.short_id()) for f in graph for t in f.requires]
    lines = sorted(lines)

    elines = [x.strip() for x in expected.split("\n")]
    elines = sorted(elines)

    assert elines == lines


def test_A_B_Grouping():
    ing = expandToGraph(""" A1: B1
                            B2: A2
                            A3: B3
                            B4: A4""")
    grouped = grouping.group(deserialize(ing))
    assertGraph(grouped, """ A1A2A3A4: B1B3
                                 B2B4: A1A2A3A4""")


def test_larger():
    ing = deserialize(expandToGraph("""C2: B6 A8
    C1: B5
    B5: A4
    A4: B2
    B2: A1
    B6: C3 A6
    C3: A5
    A5: B3
    B3: A2
    A2: B1
    A6: B4
    B4: B3
    A8: A7
    A7: A3
    A3: A2"""))
    grouped = grouping.group(ing)
    assertGraph(grouped, """C1: B5B6
    C2: B5B6
    C2: A8
    B5B6: A3A4A5A6
    B5B6: C3
    C3: A3A4A5A6
    A8: A7
    A7: A3A4A5A6
    A3A4A5A6: B4
    A3A4A5A6: B2B3
    A3A4A5A6: A1A2
    B4: B2B3
    B2B3: A1A2
    A1A2: B1""")
