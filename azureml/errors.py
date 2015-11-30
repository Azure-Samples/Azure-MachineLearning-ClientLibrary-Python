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


class _ErrorMessages(object):
    unsupported_type = 'Dataset type "{0}" is not supported'
    not_none = '"{0}" should not be None.'
    not_none_or_empty = '"{0}" should not be None or empty.'


class AzureMLError(Exception):
    '''AzureML Exception base class.'''
    def __init__(self, message):
        super(AzureMLError, self).__init__(message)


class AzureMLHttpError(AzureMLError):
    '''Error from Azure ML REST API.'''
    def __init__(self, message, status_code):
        super(AzureMLHttpError, self).__init__(message)
        self.status_code = status_code

    def __new__(cls, message, status_code, *args, **kwargs):
        if status_code == 409:
            cls = AzureMLConflictHttpError
        elif status_code == 401:
            cls = AzureMLUnauthorizedError
        return AzureMLError.__new__(cls, message, status_code, *args, **kwargs)


class AzureMLUnauthorizedError(AzureMLHttpError):
    '''Unauthorized error from Azure ML REST API.'''
    def __init__(self, message, status_code):
        message = 'Unauthorized, please check your workspace ID and authorization token ({})'.format(message)
        super(AzureMLUnauthorizedError, self).__init__(message, status_code)


class AzureMLConflictHttpError(AzureMLHttpError):
    '''Conflict error from Azure ML REST API.'''
    def __init__(self, message, status_code):
        super(AzureMLConflictHttpError, self).__init__(message, status_code)

class UnsupportedDatasetTypeError(AzureMLError):
    '''Dataset type is not supported.'''
    def __init__(self, data_type_id):
        super(UnsupportedDatasetTypeError, self).__init__(
            _ErrorMessages.unsupported_type.format(data_type_id))


def _not_none(param_name, param):
    if param is None:
        raise TypeError(_ErrorMessages.not_none.format(param_name))


def _not_none_or_empty(param_name, param):
    if not param:
        raise TypeError(_ErrorMessages.not_none_or_empty.format(param_name))
