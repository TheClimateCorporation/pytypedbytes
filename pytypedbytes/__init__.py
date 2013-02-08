"""
===========
Typed Bytes
===========

Typed bytes are sequences of bytes in which the first byte is a type
code.

Type codes
----------

Each typed bytes sequence starts with an unsigned byte that contains the
type code. Possible values are:

==== ====================
Code Type
==== ====================
0    A sequence of bytes.
1    A byte.
2    A boolean.
3    An integer.
4    A long.
5    A float.
6    A double.
7    A string.
8    A vector.
9    A list.
10   A map.
==== ====================

The type codes 50 to 200 are treated as aliases for 0, and can thus be
used for application-specific serialization.

Subsequent Bytes
----------------

These are the subsequent bytes for the different type codes (everything
is big-endian and unpadded):

==== ===========================================
Code Subsequent Bytes
==== ===========================================
0    <32-bit signed integer>
     <as many bytes as indicated by the integer>
1    <signed byte>
2    <signed byte (0 = false and 1 = true)>
3    <32-bit signed integer>
4    <64-bit signed integer>
5    <32-bit IEEE floating point number>
6    <64-bit IEEE floating point number>
7    <32-bit signed integer>
     <as many UTF-8 bytes as indicated by the
     integer>
8    <32-bit signed integer>
     <as many typed bytes sequences as indicated
     by the integer>
9    <variable number of typed bytes sequences>
     <255 written as an unsigned byte>
10   <32-bit signed integer>
     <as many (key-value) pairs of typed bytes
     sequences as indicated by the integer>
==== ===========================================

A reference implementation of typed bytes in Java is provided by the
org.apache.hadoop.typedbytes package.
"""

from version import __version__
