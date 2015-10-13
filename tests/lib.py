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
import pandas
from os import path
import os
try:
    import tests
    from tests.settings import load_test_settings
    settings = load_test_settings()
    TEST_WS = settings.workspace.id
    TEST_KEY = settings.workspace.token
    ENDPOINT = settings.workspace.management_endpoint
except:
    TEST_WS = ''
    TEST_KEY = ''
    ENDPOINT = ''


#@services.publish(TEST_WS, TEST_KEY)
#def noparams():
#    return 'hello world!'

@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
@services.types(a = unicode, b = unicode)
@services.returns(unicode)
def str_typed(a, b):
    return a + b

@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
def untyped_identity(a):
    return a

@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
@services.attach((path.join(path.dirname(__file__), 'foo.txt'), 'foo.txt'))
@services.types(a = unicode)
@services.returns(unicode)
def attached(a):
    return a + ''.join(file('Script Bundle\\foo.txt', 'rU').readlines())

@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
@services.types(a = float, b = float)
@services.returns(float)
def float_typed(a, b):
    return a / b


@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
@services.types(a = int, b = int)
@services.returns((int, int))
def multivalue_return(a, b):
    return a + b, a - b


# style 1, var args
@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
def mysum(*args):
    return sum(args)

@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
@services.types(a = int, b = int)
@services.returns(int)
def typed(a, b):
    return a + b


@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
@services.types(a = bool, b = bool)
@services.returns(bool)
def bool_typed(a, b):
    return a and b

##@services.publish(TEST_WS, TEST_KEY)
##@services.types(a = complex, b = complex)
##@services.returns(complex)
##def complex_typed(a, b):
##    return a * b


@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
@services.dataframe_service(a = int, b = int)
@services.returns(int)
def dataframe(df):
    return pandas.DataFrame([df['a'][i] + df['b'][i] for i in range(df.shape[0])])


if hasattr(dataframe, 'service'):
    @services.service(dataframe.service.url, dataframe.service.api_key)
    @services.types(a = int, b = int)
    @services.returns(int)
    def dataframe_int(a, b):
        pass

## style 1, define a function and call the publish API explicitly.

# style 1, define a function and publish it with a decorator
@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
def myfunc(a, b):
    return [a + b + a, a - b * b, a * b * a, a / b]


# style 2, define a function and call the publish API explicitly.
def myfunc2(a, b):
    return [a + b, a - b, a * b, a / b]

published = services.publish(myfunc2, TEST_WS, TEST_KEY, endpoint=ENDPOINT)




# style 1, kw args
@services.publish(TEST_WS, TEST_KEY, endpoint=ENDPOINT)
def kwargs(**args):
    return args




