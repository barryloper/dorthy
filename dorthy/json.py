# Based on http://www.awebcoder.com/post/91001/extended-jsonify-function-for-appengine-s-db-model
import collections
import datetime
import inspect
import json
import logging
import sqlalchemy
from collections import OrderedDict
from functools import wraps

from dorthy.utils import camel_encode, native_str

PRIMITIVE_TYPES = (bool, int, float, str)

logger = logging.getLogger(__name__)


def memoize(f):
    memo = OrderedDict()
    @wraps(f)
    def _memoize(obj, basename, *args, **kw):
        # fixme? will memo get cleared if an exception is raised and caught from dumps()
        _oid = id(obj)
        if _oid not in memo:
            if len(memo) == 0:
                memo[_oid] = basename or '$root'
            else:
                memo[_oid] = basename or type(obj)
            retval = f(obj, basename, *args, **kw)
            memo.popitem()
            return retval
        else:
            verb = 'contains' if isinstance(obj, (collections.Iterable, collections.Mapping, dict)) else 'is'
            memo_name = memo[_oid]
            memo.clear()
            raise ValueError("Circular reference detected: {} {} parent {}".format(basename or type(obj), verb, memo_name))

    return _memoize


@memoize
def dumps(obj, basename, camel_case=False, ignore_attributes=None, include_collections=None, encoding="utf-8"):
    """
    Provides basic json encoding.  Handles encoding of SQLAlchemy objects
    """

    if obj is None:
        return None
    elif isinstance(obj, PRIMITIVE_TYPES):
        return obj
    elif isinstance(obj, bytes):
        return native_str(obj, encoding)
    elif hasattr(obj, "_json"):
        json_obj = getattr(obj, "_json")
        if callable(json_obj):
            return json_obj()
        elif isinstance(json_obj, str):
            return json_obj
        else:
            raise ValueError("Invalid _json attribute found on object")
    elif hasattr(obj, "_as_dict"):
        dict_attr = getattr(obj, "_as_dict")
        if callable(dict_attr):
            return dumps(dict_attr(), basename, camel_case, ignore_attributes, include_collections, encoding)
        else:
            raise ValueError("Invalid _as_dict attribute found on object")
    elif isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    elif isinstance(obj, dict) or isinstance(obj, collections.Mapping):
        values = dict()
        for name, value in obj.items():
            name = native_str(name, encoding)
            new_basename = _append_path(basename, name)
            if camel_case:
                name = camel_encode(name)
            if not ignore_attributes or new_basename not in ignore_attributes:
                values[name] = dumps(value, new_basename, camel_case, ignore_attributes, include_collections, encoding)
        return values
    elif isinstance(obj, collections.Iterable):
        return [dumps(val, basename, camel_case, ignore_attributes, include_collections, encoding) for val in obj]

    # Object serializer

    values = {}
    transients = _get_transients(obj)

    # special handling for sqlalchemy objects
    try:
        mapper = sqlalchemy.inspect(obj).mapper
        serializable = mapper.all_orm_descriptors.keys()
        relationships = mapper.relationships.keys()
    except sqlalchemy.exc.NoInspectionAvailable:
        serializable = dir(obj)
        relationships = []
        pass

    for name in serializable:
        new_basename = _append_path(basename, name)
        if not _is_blacklisted_attribute(new_basename, ignore_attributes):
            if _is_visible_attribute(name, transients):
                if name in relationships and not _check_whitelist(new_basename, include_collections):
                    # don't handle sqlalchemy relationships not in whitelist
                    continue
                try:
                    value = obj.__getattribute__(name)  # why not use getattr(obj, name) or even obj.name?
                    if _is_visible_type(value):
                        if camel_case:
                            name = camel_encode(name)
                        values[name] = dumps(value, new_basename, camel_case, ignore_attributes, include_collections, encoding)
                except AttributeError:
                    continue
    if not values:
        return str(obj)
    else:
        return values


def _get_transients(obj):
    transients = set()
    trans_attr = getattr(obj, "_transients", None)
    if trans_attr:
        if callable(trans_attr):
            trans = trans_attr()
        else:
            trans = trans_attr

        if trans:
            if isinstance(trans, str):
                transients.add(trans)
            elif isinstance(trans, collections.Iterable):
                transients.update(trans)
    return transients


def _append_path(basename, name):
    if basename:
        return native_str(basename + '.' + name)
    else:
        return native_str(name)


def _is_visible_attribute(name, transients):
    return not(name.startswith("_") or
        name in transients)


def _check_whitelist(collection, include_collections):
    # if there is no whitelist, or collection is whitelisted, this returns true
    return include_collections is None or collection in include_collections


def _is_blacklisted_attribute(attribute, ignore_attributes):
    # if there is a blacklist and the attribute is in it, this returns true
    return ignore_attributes is not None and attribute in ignore_attributes


def _is_visible_type(attribute):
    return not(inspect.isfunction(attribute) or
               inspect.ismethod(attribute) or
               inspect.isbuiltin(attribute) or
               inspect.isroutine(attribute) or
               inspect.isclass(attribute) or
               inspect.ismodule(attribute) or
               inspect.istraceback(attribute) or
               inspect.isframe(attribute) or
               inspect.iscode(attribute) or
               inspect.isabstract(attribute) or
               inspect.ismethoddescriptor(attribute) or
               inspect.isdatadescriptor(attribute) or
               inspect.isgetsetdescriptor(attribute) or
               inspect.ismemberdescriptor(attribute))


class JSONEntityEncoder(json.JSONEncoder):

    def __init__(self, camel_case=False, ignore_attributes=None, encoding="utf-8", include_collections=None, basename=None, **kwargs):
        super().__init__(**kwargs)
        self.__camel_case = camel_case
        self.__encoding = encoding
        self.__ignore_attributes = ignore_attributes
        self.__include_collections = include_collections
        self.__basename = basename

    def encode(self, obj):
        d = dumps(obj, self.__basename, self.__camel_case, self.__ignore_attributes, self.__include_collections, self.__encoding)
        en = super().encode(d)
        return en


def jsonify(obj, root=None, camel_case=False, ignore_attributes=None, sort_keys=True,
            indent=None, encoding="utf-8", include_collections=None, **kwargs):
    """
    JSONify the object provided
    """

    json_out = json.dumps(obj,
                          camel_case=camel_case,
                          ignore_attributes=ignore_attributes,
                          skipkeys=True,
                          sort_keys=sort_keys,
                          indent=indent,
                          cls=JSONEntityEncoder,
                          encoding=encoding,
                          include_collections=include_collections,
                          **kwargs)

    if root:
        return '{{"{!s}": {!s}}}'.format(root, json_out)
    else:
        return json_out
