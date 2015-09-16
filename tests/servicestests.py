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

from azureml import services
import time
import tests
import traceback
import unittest
import lib
import uuid

def invoke(published_func, *args, **kwargs):
    '''helper to repeatedly invoke the function until it becomes available...'''
    for i in xrange(100):
        time.sleep(5)
        try:
            return published_func(*args, **kwargs)
            break
        except Exception as e:
            traceback.print_exc()
            print(e)
    
def invoke_map(published_func, *args):
    '''helper to repeatedly invoke the function until it becomes available...'''
    for i in xrange(100):
        time.sleep(5)
        try:
            return published_func.map(*args)
            break
        except Exception as e:
            traceback.print_exc()
            print(e)

class Test_services(unittest.TestCase):
    def test_service_id(self):
        service_id = uuid.UUID(lib.str_typed.service.service_id)
        self.assertNotEqual(service_id, uuid.UUID('00000000000000000000000000000000'))

    def test_str_typed(self):
        self.assertEqual(invoke(lib.str_typed.service, 'abc', 'def'), 'abcdef')

    def test_attached(self): 
        self.assertEqual(invoke(lib.attached.service, 'test '), 'test hello world!')

    def test_bool_typed(self):
        self.assertEqual(invoke(lib.bool_typed.service, True, False), False)

    def test_float_typed(self):
        self.assertEqual(invoke(lib.float_typed.service, 3.0, 5.0), .6)

    def test_multivalue_return(self):
        self.assertEqual(invoke(lib.multivalue_return.service, 1, 2), (3, -1))

    def test_map(self):
        # invoking via map
        self.assertEqual(invoke_map(lib.typed.service, [1, 1], [2, 4]), [3, 5])

    def test_varargs(self):
        # style 1, var args
        self.assertEqual(invoke(lib.mysum.service, 1, 2, 3), 6)

    def test_kwargs(self):
        self.assertEqual(invoke(lib.kwargs.service, x = 1, y = 2), {'y': 2, 'x': 1})

    def test_simple_decorator(self):
        # style 1, define a function and publish it with a decorator
        self.assertEqual(invoke(lib.myfunc.service, 1, 2), [4, -3, 2, 0])

    def test_publish_explicitly(self):
        # style 2, define a function and call the publish API explicitly.
        self.assertEqual(invoke(lib.published, 1, 2), [3, -1, 2, 0])

    def test_strongly_typed(self):
        # a strongly typed version...
        self.assertEqual(invoke(lib.typed.service, 1, 2), 3)

    def test_data_frame_input(self):
        # style 2, define a function and call the publish API explicitly.
        self.assertEqual(invoke(lib.dataframe_int, 1, 2), 3.0)


    #def test_complex_typed(self):
    #    print(invoke(lib.complex_typed, 3j, 5j))

    def test_consume_published(self):
        # style 3, consume an already published service
        url, api_key, help_url = lib.published

        @services.service(url, api_key)
        def published_func(a, b):
            pass

        self.assertEqual(invoke(published_func, 1, 2), [3, -1, 2, 0])

if __name__ == '__main__':
    unittest.main()
