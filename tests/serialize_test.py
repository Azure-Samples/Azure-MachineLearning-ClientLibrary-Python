#-------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation
# All rights reserved.
#
# MIT License:
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#--------------------------------------------------------------------------

import unittest
import azureml
import sys
from azureml.services import _serialize_func, _deserialize_func, _encode, _decode

def mutually_ref_f():
    mutually_ref_g
    return 42, mutually_ref_g

def mutually_ref_g():
    return 100, mutually_ref_f

abc = 200
def reads_global():
    return abc


class MyClass():
    pass

class BaseClass: pass

class DerivedClass(BaseClass):
    pass

def reads_class():
    return MyClass()

def reads_derived_class():
    return DerivedClass()

def aliased_function():
    return 42

alias = aliased_function
def calls_aliased_function():
    return alias()

def reads_module():
    return sys.version

class Test_serialize_test(unittest.TestCase):
    def make_globals(self):
        return {'__builtins__' : __builtins__}

    def test_module(self):
        serialized = _serialize_func(reads_module)
        glbs = self.make_globals()
        f = _deserialize_func(serialized, glbs)
        self.assertEqual(f(), sys.version)

    def test_aliasing(self):
        serialized = _serialize_func(calls_aliased_function)
        glbs = self.make_globals()
        f = _deserialize_func(serialized, glbs)
        self.assertEqual(f(), 42)

    def test_mutually_ref(self):
        global mutually_ref_f, mutually_ref_g

        glbs = self.make_globals()
        serialized = _serialize_func(mutually_ref_f)
        del mutually_ref_f, mutually_ref_g

        f = _deserialize_func(serialized, glbs)
        self.assertEqual(f()[0], 42)

        self.assertEqual(f()[1]()[0], 100)

    def test_reads_global(self):
        global abc, reads_global

        glbs = self.make_globals()
        s = _serialize_func(reads_global)
        del abc, reads_global
        f = _deserialize_func(s, glbs)

        self.assertEqual(f(), 200)
        pass

    def test_core_types(self):
        values = [42, 'abc', b'abc', 100.0, True, False, 3j, None, [1,2,3], (1,2,3), {2:3}]

        for value in values:
            self.assertEqual(_decode(_encode(value)), value)

    def test_other_types(self):
        try:
            import numpy
            self.assertTrue(_decode(_encode(numpy.ndarray(42))).all())
        except:
            return

    def test_reads_class(self):
        global reads_class, MyClass

        s = _serialize_func(reads_class)
        del reads_class, MyClass

        glbs = self.make_globals()
        f = _deserialize_func(s, glbs)

        self.assertTrue(repr(f()).startswith('<__main__.MyClass instance at'))

    #def test_reads_derived_class(self):
    #    global reads_derived_class, BaseClass, DerivedClass

    #    s = _serialize_func(reads_derived_class)
    #    del reads_derived_class, BaseClass, DerivedClass

    #    glbs = self.make_globals()
    #    f = _deserialize_func(s, glbs)

    #    print(glbs)
    #    print(repr(f()))

if __name__ == '__main__':
    unittest.main()
