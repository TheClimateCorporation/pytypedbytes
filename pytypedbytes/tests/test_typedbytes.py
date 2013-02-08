# coding=utf-8

import unittest
import struct
from collections import namedtuple
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


if __name__ == "__main__":
    unittest.main()
