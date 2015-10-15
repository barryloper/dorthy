import pytest
from dorthy.json import jsonify
from sqlalchemy import Column, String, Integer, ForeignKey, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSON
from dorthy.enum import DeclarativeEnum
from datetime import datetime

import pytz

Base = declarative_base()

# todo: more test cases
# todo: check what happens to memo when dumps() raises an externally-handled exception


@pytest.fixture
def gen_dict():
    test_dict = {
        "stringArg": "hello",
        "intArg": 1,
        "floatArg": 1.2,
        "listArg": ["hello", "world"],
        "dictArg": {"hello": "first", "world": "second"}
    }
    test_dict["recurseArg"] = test_dict
    test_dict['listArg'].append(test_dict)
    test_dict["dictArg"]["recurseArg"] = test_dict
    test_dict["dictArg"]["floatArg"] = test_dict["floatArg"] # this should not trigger recursion probs
    return test_dict


@pytest.fixture
def gen_sqlalchemy():
    class Enum(DeclarativeEnum):
        Option1 = "option1"
        Option2 = "option2"

    siblings = Table('siblings', Base.metadata,
        Column("left_sibling_id", Integer, ForeignKey("child.id"), primary_key=True),
        Column("right_sibling_id", Integer, ForeignKey("child.id"), primary_key=True)
    )

    class ChildEntity(Base):
        __tablename__ = "child"
        id = Column(Integer, primary_key=True)
        name = Column(String)
        parent_id = Column(Integer, ForeignKey("parent.id"))
        siblings = relationship("ChildEntity",
                    secondary="siblings",
                    primaryjoin="ChildEntity.id==siblings.c.left_sibling_id",
                    secondaryjoin="siblings.c.right_sibling_id==ChildEntity.id")

    class ParentEntity(Base):
        __tablename__ = "parent"
        id = Column(Integer, primary_key=True)
        name = Column(String)
        attrs = Column(JSON)
        option = Column(Enum.db_type())
        children = relationship(ChildEntity, backref="parent")
        __hidden_attr = "peekaboo"

        @hybrid_property
        def hidden_attr(self):
            return self.__hidden_attr

    dad = ParentEntity(name="Dad", attrs={"att1": "val1", "attr2": 2}, option=Enum.Option1)
    bobby = ChildEntity(name="Bobby", parent=dad)
    judy = ChildEntity(name="Judy", parent=dad)
    jane = ChildEntity(name="Jane", parent=dad)
    bobby.siblings = [judy, jane]

    return {'dad': dad, 'bobby': bobby, 'judy': judy}


@pytest.fixture
def gen_obj():
    class ParentObject(object):
        def __init__(self, name, children=None):
            self.name = name
            self.children = children or []

    class ChildObject(object):
        def __init__(self, name, parent=None, siblings=None, cousins=None):
            self.name = name
            self.parent = parent
            self.siblings = siblings
            self.cousins = cousins

    class ChildTransient(ChildObject):
        _transients = ("parent", "siblings", "cousins")

    dad = ParentObject(name="Dad")
    bobby = ChildObject(name="Bobby", parent=dad)
    judy = ChildObject(name="Judy", parent=dad)
    rebel = ChildObject(name="Rebel")
    transient = ChildTransient(name="Trudy", parent=dad)
    jimbob = ChildObject(name="JimBob", siblings=[bobby, ], cousins=(rebel, ))
    dad.children = [bobby, judy, rebel, transient, jimbob]

    return {'dad': dad, 'bobby': bobby, 'judy': judy, 'trudy': transient, 'jimbob': jimbob}


@pytest.fixture
def gen_recur_list():
    the_list = [1, "two"]
    list_one = ["1a", "1b"]
    list_two = ["2a", "2b", list_one] # make sure ancestor checking isn't checking siblings
    the_list.append(list_one)
    the_list.append(list_two)
    the_list.append(the_list) # test recursion handling
    return the_list


def test_jsonify_basics():
    assert jsonify(["hello world's fair", 45, 4.5, None]) == '["hello world\'s fair", 45, 4.5, null]'
    assert jsonify({"num": 99.44, "str": 'string', "none": None}) == '{"none": null, "num": 99.44, "str": "string"}'
    assert jsonify(None) is 'null'
    assert jsonify([datetime(1978, 10, 4, 14, 50, tzinfo=pytz.timezone("US/Mountain"))]) == '["1978-10-04T14:50:00-07:00"]'
    assert jsonify([datetime(1978, 10, 4, 14, 50, tzinfo=pytz.utc)]) == '["1978-10-04T14:50:00+00:00"]'


def test_jsonify_list(gen_recur_list):
    with pytest.raises(ValueError) as excinfo:
       bad_out = jsonify(gen_recur_list, basename="bad_list")
    assert "Circular reference detected: bad_list contains parent bad_list" in str(excinfo.value)

def test_jsonify_generic_obj(gen_obj):
    with pytest.raises(ValueError) as excinfo:
        bad_out = jsonify(gen_obj["dad"], basename="bad_dad")
    assert 'Circular reference detected: bad_dad.children.parent is parent bad_dad' in str(excinfo.value)

    ignore_list_attribute = jsonify(gen_obj["dad"], ignore_attributes=("children",))
    assert ignore_list_attribute == '{"name": "Dad"}'

    respects_transients = jsonify(gen_obj["trudy"])
    assert respects_transients == '{"name": "Trudy"}'


    # two copies of same item, but not an infinite loop
    ignore_list_items = jsonify(gen_obj["dad"], ignore_attributes=("children.parent", "children.siblings.parent", "children.cousins", "children.siblings.cousins"))
    assert ignore_list_items == '{"children": [{"name": "Bobby", "siblings": null}, ' + \
                                '{"name": "Judy", "siblings": null}, {"name": "Rebel", "siblings": null}, ' + \
                                '{"name": "Trudy"}, {"name": "JimBob", "siblings": ' + \
                                '[{"name": "Bobby", "siblings": null}]}], "name": "Dad"}'


def test_sqlalchemy(gen_sqlalchemy):
    with pytest.raises(ValueError) as excinfo:
        bad_out = jsonify(gen_sqlalchemy['dad'], basename="bad_dad")
    assert str(excinfo.value) == "Circular reference detected: bad_dad.children.parent is parent bad_dad"

    children_ignored = jsonify(gen_sqlalchemy['dad'], ignore_attributes=("children", ))
    for attr in ("attrs", "name", "Dad", "description"):
        assert attr in children_ignored
    assert '"children": ' not in children_ignored

    # enums count as collections and must be whitelisted if a whitelist is provided. fixme?
    explicit_whitelist = jsonify(gen_sqlalchemy['dad'], include_relationships=("children",))
    # parent.children should be included
    assert '"children": [{"id":' in explicit_whitelist
    # parent.children.siblings should not be included
    assert '"siblings"' not in explicit_whitelist


    blacklist_attrs_of_children = jsonify(gen_sqlalchemy['dad'], ignore_attributes=("children.parent", "children.siblings.parent" ))
    assert '"parent": ' not in blacklist_attrs_of_children
    assert '"siblings": [{"' in blacklist_attrs_of_children

    whitelist_and_blacklist_together = jsonify(gen_sqlalchemy['dad'], include_relationships=("children",),
        ignore_attributes=("attrs", ))
    #sub-relationships are not loaded unless explicitly included when include_relationships is specified
    assert '"children": [{"id":' in whitelist_and_blacklist_together
    assert '"attrs": {' not in whitelist_and_blacklist_together

    sub_collections = jsonify(gen_sqlalchemy['dad'], include_relationships=("children", "children.siblings"))
    assert '"parent": ' not in sub_collections
    assert '"siblings": [' in sub_collections
    assert '"children": [' in sub_collections


def test_json_attribute():

    class InvalidJSON(object):
        _json = [1,2,3]

    ij = InvalidJSON()
    with pytest.raises(ValueError) as excinfo:
        bad_out = jsonify(ij)
    assert "Invalid _json" in str(excinfo.value)

    class ValidJSONStr(object):
        _json = "a string"

    assert '"a string"' == jsonify(ValidJSONStr())

    class ValidJSONFun(object):
        _objData = "a json function"

        def _json(self):
            return self._objData

    assert '"a json function"' == jsonify(ValidJSONFun())

