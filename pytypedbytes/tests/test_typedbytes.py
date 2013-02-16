# coding=utf-8

import unittest
import struct
from collections import namedtuple
from math import isnan
from StringIO import StringIO

from pytypedbytes import typedbytes


class TypedBytesTestCase(unittest.TestCase):

    def test_default_types_round_trip(self):
        """Test successful round-trip serialization/deserialization of a
        sequence of objects whose types have default serializations."""
        expected = [
            bytearray("\x0a\x0b\x0c"), # sequence of bytes
            True, # boolean
            -1, # integer
            1125899906842624L, # long
            struct.unpack('>f', "abcd")[0], # float
            struct.unpack('>d', "abcdefgh")[0], # double
            float('inf'), # double
            u" śpăm\n ", # string
            (-0.1, False, 27), # tuple ("vector" in typed bytes)
            [-0.1, False, 27], # list
            {"ab": -0.1, "cd": False, True: 27}, # dict ("map" in typed bytes)
            ]
        fp = StringIO()
        serializer = typedbytes.iterdump(fp)
        for obj in expected:
            serializer.send(obj)
        serializer.close()
        fp.seek(0)
        deserializer = typedbytes.iterload(fp)
        computed = [deserializer.next() for _ in xrange(len(expected))]
        self.assertRaises(StopIteration, deserializer.next)
        self.assertEqual(expected, computed)

    def test_NaN_round_trip(self):
        obj = float('nan')
        self.assertTrue(isnan(typedbytes.loads(typedbytes.dumps(obj))))

    def test_serialization_respects_type_inheritance(self):
        """Test that the serialization type of an object is inferred by
        inheritance, like the behavior of the ``isinstance`` built-in
        function, not by checking strict type equality."""
        TupleLike = namedtuple("TupleLike", ["spam", "ham", "eggs"])
        tuple_like = TupleLike(spam=1, ham=2, eggs=3)
        computed = typedbytes.dumps(tuple_like)
        expected = typedbytes.dumps(tuple(tuple_like))
        # tuple_like is an instance of tuple, so we expect it to be
        # serialized exactly like a "normal" tuple.
        self.assertEqual(expected, computed)

    def test_serialization_of_unrecognized_type(self):
        """Test that a TypeError is raised when attempting to serialize
        an object whose type does not have a default serialization."""
        exotic_objects = [
            set([1, 2]),
            slice(None),
            lambda x: x,
            ]
        for obj in exotic_objects:
            self.assertRaises(TypeError, typedbytes.dumps, obj)

    def test_extensibility(self):
        """Test the ability to define serializations for additional
        types."""
        # Define a serialization for Python's ``set`` type.
        def load_set(fp, types=None):
            return set(typedbytes.load_list(fp, types))

        def dump_set(obj, fp, types=None):
            typedbytes.dump_list(obj, fp, types)

        set_type = typedbytes.Type(111, set, load_set, dump_set)

        # Define a serialization for Python's ``NoneType``
        def load_null(fp, types=None):
            return None

        def dump_null(obj, fp, types=None):
            typedbytes.dump_end_of_list(obj, fp, types)

        null_type = typedbytes.Type(47, type(None), load_null, dump_null)

        custom_types = typedbytes.default_types + (set_type, null_type)
        expected = [
            bytearray("\x0a\x0b\x0c"),
            set(), # set is not a default type
            [set([-0.1, False, 27])], # set is not a default type
            None, # NoneType is not a default type
            {"ab": -0.1, "cd": False, True: 27},
            set([None]), # NoneType and set are not default types
            ]
        fp = StringIO()
        serializer = typedbytes.iterdump(fp, custom_types)
        for obj in expected:
            serializer.send(obj)
        serializer.close()
        fp.seek(0)
        deserializer = typedbytes.iterload(fp, custom_types)
        computed = [deserializer.next() for _ in xrange(len(expected))]
        self.assertRaises(StopIteration, deserializer.next)
        self.assertEqual(expected, computed)

    def test_deserialization_of_unrecognized_type_code(self):
        """Test that a ValueError is raised when attempting to
        deserialize a byte stream that starts with an unrecognized type
        code."""
        # Define a serialization for Python's ``set`` type.
        def load_set(fp, types=None):
            return set(typedbytes.load_list(fp, types))

        def dump_set(obj, fp, types=None):
            typedbytes.dump_list(obj, fp, types)

        new_type = typedbytes.Type(111, set, load_set, dump_set)
        custom_types = typedbytes.default_types + (new_type,)
        expected = [
            bytearray("\x0a\x0b\x0c"),
            set([-0.1, False, 27]), # set is not a default type
            {"ab": -0.1, "cd": False, True: 27},
            ]
        fp = StringIO()
        # Serialize with this custom type definition.
        serializer = typedbytes.iterdump(fp, custom_types)
        for obj in expected:
            serializer.send(obj)
        serializer.close()
        fp.seek(0)
        # Deserialize without this custom type definition.
        deserializer = typedbytes.iterload(fp, typedbytes.default_types)
        self.assertEqual(expected[0], deserializer.next())
        self.assertRaises(ValueError, deserializer.next)


if __name__ == "__main__":
    unittest.main()
