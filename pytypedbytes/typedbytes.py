from collections import namedtuple
from cStringIO import StringIO
from itertools import islice
from math import isnan
from struct import Struct
from types import ClassType


class StreamStruct(Struct):
    """Subclass of ``struct.Struct`` with methods to write and read
    binary data with file-like objects."""

    def pack_write(self, fp, *args):
        """Pack values into a writeable file-like object *fp* according
        to the compiled format."""
        string = self.pack(*args)
        fp.write(string)

    def unpack_read(self, fp):
        """Unpack bytes from a readable file-like object *fp* according
        to the compiled format. This method raises EOFError if not
        enough bytes can be read from *fp*."""
        string = fp.read(self.size)
        if len(string) != self.size:
            raise EOFError(
                "Not enough bytes were read from the file-like readable.")
        return self.unpack(string)


# Pre-compiled Struct instances.
unsigned_char_struct = StreamStruct('>B')
signed_char_struct = StreamStruct('>b')
int_struct = StreamStruct('>i')
long_struct = StreamStruct('>q')
float_struct = StreamStruct('>f')
double_struct = StreamStruct('>d')


# Type codes must unsigned 8-bit integers.
valid_type_codes = set(range(0x100))


# The type codes 50 to 200 are reserved for application-specific
# extensions.
application_type_codes = set(range(50, 200 + 1))


class EndOfList(object):
    """Class that abstractly represents the end of a list.

    Instances of this class are used as sentinel values for
    serialization and deserialization of lists."""
    pass


class Type(namedtuple("Type", ["code", "type", "load", "dump"])):
    """Type definition for Hadoop typed bytes."""

    def __init__(self, *args, **kwargs):
        validate_type_definition(self)


def validate_type_definition(type_definition):
    if not isinstance(type_definition.code, int):
        raise TypeError(
            "Type definition has an invalid 'code' field: %s" %
            type_definition.code)
    if type_definition.code not in valid_type_codes:
        raise ValueError(
            "Type definition has an invalid 'code' field: %s" %
            type_definition.code)
    if not isclassinfo(type_definition.type):
        raise TypeError(
            "Type definition has an invalid 'type' field: %s" %
            type_definition.type)
    if not callable(type_definition.load):
        raise TypeError(
            "Type definition has a non-callable 'load' field: %s" %
            type_definition.load)
    if not callable(type_definition.dump):
        raise TypeError(
            "Type definition has a non-callable 'dump' field: %s" %
            type_definition.dump)


def isclassinfo(classinfo):
    """Test whether an object is a valid second argument to the
    `isinstance` builtin function."""
    if isinstance(classinfo, tuple):
        return all(map(isclassinfo, classinfo))
    else:
        return isinstance(classinfo, (type, ClassType))


def load(fp, types=None):
    """Deserialize a readable file-like object *fp* to a Python
    object."""
    if types is None:
        types = basic_types
    type_code = load_type_code(fp)
    for td in types:
        if td.code == type_code:
            obj = td.load(fp)
            break
    else:
        raise ValueError("Unrecognized type code.")
    return obj


def loads(s, types=None):
    """Deserialize a sequence of bytes *s* to a Python object."""
    fp = StringIO(s)
    return load(fp, types)


def iterload(fp, types=None):
    """Generator function that deserializes Python objects from a
    readable file-like object *fp*.

    The returned iterator raises ``StopIteration`` when it encounters a
    0xff byte."""
    if types is None:
        types = basic_types
    while True:
        try:
            obj = load(fp, types)
        except EOFError:
            raise StopIteration
        if isinstance(obj, EndOfList):
            raise StopIteration
        yield obj


def dump(obj, fp, types=None):
    """Serialize *obj* to a writeable file-like object *fp*, flushing
    the output buffer after write."""
    if types is None:
        types = basic_types
    for td in types:
        if isinstance(obj, td.type):
            dump_type_code(td.code, fp)
            td.dump(obj, fp, types)
            break
    else:
        raise TypeError("Object is not serializable.")
    fp.flush()


def dumps(obj, types=None):
    """Serialize *obj* to a ``str`` instance."""
    fp = StringIO()
    dump(obj, fp, types)
    string = fp.getvalue()
    return string


def iterdump(fp, types=None):
    """Coroutine function that serializes Python objects to a writeable
    file-like object *fp*, flushing the output buffer after each object
    is written.

    This function returns the coroutine after "priming" it by calling
    its ``.next()`` method once.
    """
    if types is None:
        types = basic_types
    def _write_typed_bytes():
        while True:
            obj = (yield)
            dump(obj, fp, types)
    cr = _write_typed_bytes()
    cr.next()
    return cr


def load_type_code(fp):
    """Deserialize a type code from a readable file-like object *fp*.

    This function calls the ``read()`` method of *fp* to read 1 byte and
    interprets it as a type code, which is returned as an ``int``
    instance from 0 (inclusive) to 256 (exclusive).
    """
    return unsigned_char_struct.unpack_read(fp)[0]


def dump_type_code(obj, fp):
    """Serialize a type code *obj* to a writeable file-like object *fp*.

    This function calls the ``write()`` method of *fp* to write 1 byte
    that represents *obj* as an unsigned byte. This function raises a
    ValueError if *obj* does not have an integer value within the range
    of an unsigned byte, which is 0 (inclusive) to 256 (exclusive).
    """
    if int(obj) != obj:
        raise TypeError(
            "Object must be coercible to int without loss of information.")
    if obj not in valid_type_codes:
        raise ValueError("Integer must be a valid type code.")
    unsigned_char_struct.pack_write(fp, obj)


def load_end_of_list(fp, types=None):
    """Deserialize an end-of-list marker from a readable file-like
    object *fp*.

    This function calls the ``.read()`` method of *fp* to read 0 bytes
    and returns an instance of the EndOfList class. This function is
    provided as a useful abstraction when deserializing a list from
    typed bytes."""
    _ = fp.read(0)
    return EndOfList()


def dump_end_of_list(obj, fp, types=None):
    """Serialize the end of a list to a writeable file-like object *fp*.

    This function calls the ``write()`` method of *fp* to write 0 bytes
    (nothing at all). This function is provided as a useful
    abstract when serializing a list to typed bytes."""
    fp.write("")


def load_bytes(fp, types=None):
    """Deserialize a ``bytearray`` instance from a readable file-like
    object *fp*.

    This function calls the ``read()`` method of *fp* to read 4 or more
    bytes and interprets them as follows:
        <big-endian 32-bit signed integer>
        <as many bytes as indicated by the integer>
    """
    size = load_size(fp)
    return bytearray(fp.read(size))


def dump_bytes(obj, fp, types=None):
    """Serialize a sequence of bytes *obj* to a writeable file-like
    object *fp*.

    This function calls the ``write()`` method of *fp* to write 1 or
    more bytes that represent the sequence of bytes in *obj*.
    """
    if not isinstance(obj, (str, bytearray)):
        obj = bytearray(obj)
    size = len(obj)
    dump_integer(size, fp)
    fp.write(obj)


def load_byte(fp, types=None):
    """Deserialize a signed byte ``int`` from a readable file-like
    object *fp*.

    This function calls the ``read()`` method of *fp* to read 1 byte and
    interprets it as a signed byte, which is returned as an integer from
    -128 (inclusive) to +128 (exclusive).
    """
    return signed_char_struct.unpack_read(fp)[0]


def dump_byte(obj, fp, types=None):
    """Serialize a signed byte value *obj* to a writeable file-like
    object *fp*.

    This function calls the ``write()`` method of *fp* to write 1 byte
    that represents *obj* as a signed byte. This function raises a
    ValueError if *obj* does not have an integer value within the range
    of a signed byte, which is -128 (inclusive) to +128 (exclusive).
    """
    if int(obj) != obj:
        raise TypeError(
            "Object must be coercible to int without loss of information.")
    if not (obj >= -0x80 and obj < +0x80):
        raise ValueError("Integer must be in the range of a signed byte.")
    signed_char_struct.pack_write(fp, obj)


def load_boolean(fp, types=None):
    """Deserialize a ``bool`` instance from a readable file-like object
    *fp*.

    This function calls the ``read()`` method of *fp* to read 1 byte and
    interprets it as a boolean according to the following rules:
    - If the byte equals '\x00', then return False.
    - If the byte equals '\x01', then return True.

    This function raises a ValueError if the byte that is read from *fp*
    does not equal either recognized values.
    """
    i = load_byte(fp)
    if i == 0:
        return False
    elif i == 1:
        return True
    else:
        raise ValueError("%d is not a recognized value for boolean")


def dump_boolean(obj, fp, types=None):
    """Serialize the truth value of *obj* to a writeable file-like
    object *fp*.

    This function calls the ``write()`` method of *fp* to write 1 byte
    that represents the truth value of *obj* according to the following
    rules:
    - If *obj* is false, then serialize as '\x00'.
    - If *obj* is true, then serialize as '\x01'.
    """
    i = 1 if obj else 0
    dump_byte(i, fp)


def load_integer(fp, types=None):
    """Deserialize an ``int`` instance from a readable file-like object
    *fp*.

    This function calls the ``read()`` method of *fp* to read 4 bytes
    and interprets them as a big-endian 32-bit signed integer."""
    return int_struct.unpack_read(fp)[0]


def dump_integer(obj, fp, types=None):
    """Serialize an integer value *obj* to a writeable file-like object
    *fp*.

    This function calls the ``write()`` method of *fp* to write 4 bytes
    that represent *obj* as a big-endian 32-bit signed integer. This
    function raises a ValueError if *obj* is not within the range of a
    32-bit signed integer.
    """
    if int(obj) != obj:
        raise TypeError(
            "Object must be coercible to int without loss of information.")
    if not (obj >= -0x80000000 and obj < +0x80000000):
        raise ValueError("Integer must be in the range of a signed integer.")
    int_struct.pack_write(fp, obj)


def load_size(fp, types=None):
    """Deserialize a non-negative ``int`` instance from a readable
    file-like object *fp*.

    This function calls the ``read()`` method of *fp* to read 4 bytes
    and interprets them as a big-endian 32-bit signed integer. This
    function raises a ValueError if the deserialized integer is
    negative.
    """
    size = load_integer(fp)
    if size < 0:
        raise ValueError("%d is not a valid size" % size)
    return size


def dump_size(obj, fp, types=None):
    """Serialize the non-negative integer *obj* to a writeable file-like
    object *fp*.

    This function calls the ``write()`` method of *fp* to write 4 bytes
    that represent *obj* as a big-endian 32-bit signed integer. This
    function raises a ValueError if *obj* is not within the range of a
    32-bit integer or if *obj* is negative."""
    if obj < 0:
        raise ValueError("Size must be non-negative.")
    dump_integer(obj, fp)


def load_long(fp, types=None):
    """Deserialize a ``long`` instance from a readable file-like object
    *fp*.

    This function calls the ``read()`` method of *fp* to read 8 bytes
    and interprets them as a big-endian 64-bit signed integer."""
    return long(long_struct.unpack_read(fp)[0])


def dump_long(obj, fp, types=None):
    """Serialize the integer *obj* to a writeable file-like object *fp*.

    This function calls the ``write()`` method of *fp* to write 8 bytes
    that represent *obj* as a big-endian 64-bit signed integer. This
    function raises a ValueError if *obj* is not within the range of a
    64-bit signed integer.
    """
    if long(obj) != obj:
        raise TypeError(
            "Object must be coercible to long without loss of information.")
    if not (obj >= -0x8000000000000000 and obj < +0x8000000000000000):
        raise ValueError("Integer must be in the range of a signed integer.")
    long_struct.pack_write(fp, obj)


def load_float(read, types=None):
    """Deserialize a ``float`` instance from a readable file-like object
    *fp*.

    This function calls the ``read()`` method of *fp* to read 4 bytes
    and interprets them as a big-endian 32-bit signed IEEE floating
    point number."""
    return float_struct.unpack_read(fp)[0]


def dump_float(write, obj, types=None):
    """Serialize a 32-bit floating point number *obj* to a writeable
    file-like object *fp*.

    This function calls the ``write()`` method of *fp* to write 4 bytes
    that represent *obj* as a big-endian 32-bit signed IEEE floating
    point number. This function raises a ValueError if *obj* can not be
    exactly represented in this floating point format.

    To serialize Python's builtin float type, use the ``dump_double()``
    function instead."""
    coerced_obj = float(obj)
    if (not isnan(coerced_obj)) and (coerced_obj != obj):
        raise TypeError(
            "Object must be coercible to float without loss of information.")
    # Check for loss of precision when packing as a 32-bit signed IEEE
    # floating point number.
    string = float_struct.pack(obj)
    if float_struct.unpack(string) != (obj,):
        raise TypeError(
            "Object must be exactly representable as a 32-bit signed float.")
    fp.write(string)


def load_double(fp, types=None):
    """Deserialize a ``float`` instance from a readable file-like object
    *fp*.

    This function calls the ``read()`` method of *fp* to read 8 bytes
    and interprets them as a big-endian 64-bit signed IEEE floating
    point number."""
    return double_struct.unpack_read(fp)[0]


def dump_double(obj, fp, types=None):
    """Serialize a 64-bit floating-point number *obj* to a writeable
    file-like object *fp*.

    This function calls the ``write()`` method of *fp* to write 8 bytes
    that represent *obj* as a big-endian 64-bit signed IEEE floating
    point number."""
    coerced_obj = float(obj)
    if (not isnan(coerced_obj)) and (coerced_obj != obj):
        raise TypeError(
            "Object must be coercible to float without loss of information.")
    double_struct.pack_write(fp, obj)


def load_string(fp, types=None):
    """Deserialize ``unicode`` instance from a readable file-like object
    *fp*.

    This function calls the ``read()`` method of *fp* to read 4 or more
    bytes and interprets them as follows:
        <big-endian 32-bit signed integer>
        <as many UTF-8 bytes as indicated by the integer>
    """
    raw = load_bytes(fp)
    return raw.decode('utf_8')


def dump_string(obj, fp, types=None):
    """Serialize a string-like object *obj* to a writeable file-like
    object *fp*.

    This function calls the ``write()`` method of *fp* to write 1 or
    more bytes that encode *obj* as UTF-8.
    """
    obj = unicode(obj)
    raw = obj.encode('utf_8')
    size = len(raw)
    dump_size(size, fp)
    fp.write(raw)


def load_vector(fp, types=None):
    """Deserialize a ``tuple`` instance ("vector" in typed bytes) from a
    readable file-like object *fp*.

    This function calls the ``read()`` method of *fp* to read 4 or more
    bytes and interprets them as follows:
        <big-endian 32-bit signed integer>
        <as many typed bytes sequences as indicated by the integer>
    The elements of the tuple are recursively deserialized.
    """
    size = load_size(fp)
    deserializer = iterload(fp, types)
    return tuple(islice(deserializer, size))


def dump_vector(obj, fp, types=None):
    """Serialize a tuple-like object *obj* to writeable file-like object
    *fp*.

    This function calls the ``write()`` method of *fp* to write 4 or
    more bytes that represent *obj* as a typed bytes vector. The
    elements of *obj* are recursively serialized.
    """
    size = len(obj)
    dump_size(size, fp)
    serializer = iterdump(fp, types)
    for element in obj:
        serializer.send(element)


def load_list(fp, types=None):
    """Deserialize a ``list`` instance from a readable file-like object
    *fp*.

    This function calls the ``read()`` method of *fp* to read 1 or more
    bytes and interprets them as follows:
        <variable number of typed bytes sequences>
        <255 written as an unsigned byte, marking the end of the list>
    The elements of the list are recursively deserialized.
    """
    return list(iterload(fp, types))


def dump_list(obj, fp, types=None):
    """Serialize a list-like object *obj* to a writeable file-like
    object *fp*.

    This function calls the ``write()`` method of *fp* to write 1 or
    more bytes that represent *obj* as a typed bytes list. The
    elements of *obj* are recursively serialized.
    """
    serializer = iterdump(fp, types)
    for element in obj:
        serializer.send(element)
    serializer.send(EndOfList())


def load_map(fp, types=None):
    """Deserialize a ``dict`` instance ("map" in typed bytes) from a
    readable file-like object *fp*.

    This function calls the ``read()`` method of *fp* to read 4 or more
    bytes and interprets them as follows:
        <32-bit signed integer>
        <as many (key-value) pairs of typed bytes sequences as indicated
         by the integer>
    The items of the dict are recursively deserialized.
    """
    size = load_size(fp)
    deserializer = iterload(fp, types)
    def key_value_pair():
        key = deserializer.next()
        value = deserializer.next()
        return (key, value)
    key_value_pairs = (key_value_pair() for _ in xrange(size))
    return dict(key_value_pairs)


def dump_map(obj, fp, types=None):
    """Serialize a dict-like object *obj* to a writeable file-like
    object *fp*.

    This function calls the ``write()`` method of *fp* to write 4 or
    more bytes that represent *obj* as a typed bytes map. The
    items of *obj* are recursively serialized.
    """
    size = len(obj)
    dump_size(size, fp)
    serializer = iterdump(fp, types)
    for (k, v) in obj.iteritems():
        serializer.send(k)
        serializer.send(v)


basic_types = (
    Type(255, EndOfList, load_end_of_list, dump_end_of_list),
    Type(0, bytearray, load_bytes, dump_bytes),
    # Type code 1 (byte) has no corresponding Python type.
    Type(1, (), load_byte, dump_byte),
    Type(2, bool, load_boolean, dump_boolean),
    Type(3, int, load_integer, dump_integer),
    Type(4, long, load_long, dump_long),
    # Type code 5 (float) has no corresponding Python type. The builtin
    # Python float is a double.
    Type(5, (), load_float, dump_float),
    Type(6, float, load_double, dump_double),
    Type(7, basestring, load_string, dump_string),
    # Type code 8 (vector) corresponds to a Python tuple.
    Type(8, tuple, load_vector, dump_vector),
    Type(9, list, load_list, dump_list),
    Type(10, dict, load_map, dump_map),
    )
