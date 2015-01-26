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

from os import path
import json
import numpy as np
import random
import string


class TestSettings(object):
    class Workspace(object):
        def __init__(self, settings):
            self.settings = settings

        @property
        def id(self):
            return self.settings['id']

        @property
        def token(self):
            return self.settings['token']

        @property
        def endpoint(self):
            return self.settings['endpoint']

    class Storage(object):
        def __init__(self, settings):
            self.settings = settings

        @property
        def account_name(self):
            return self.settings['accountName']

        @property
        def account_key(self):
            return self.settings['accountKey']

        @property
        def container(self):
            return self.settings['container']

        @property
        def medium_size_blob(self):
            return self.settings['mediumSizeBlob']

        @property
        def unicode_bom_blob(self):
            return self.settings['unicodeBomBlob']

        @property
        def blobs(self):
            return self.settings['blobs']

    class IntermediateDataset(object):
        def __init__(self, settings):
            self.settings = settings

        @property
        def experiment_id(self):
            return self.settings['experimentId']

        @property
        def node_id(self):
            return self.settings['nodeId']

        @property
        def port_name(self):
            return self.settings['portName']

        @property
        def data_type_id(self):
            return self.settings['dataTypeId']

    class Diagnostics(object):
        def __init__(self, settings):
            self.settings = settings

        @property
        def write_blob_contents(self):
            return self.settings['writeBlobContents']

        @property
        def write_serialized_frame(self):
            return self.settings['writeSerializedFrame']

    def __init__(self, settings):
        self.workspace = TestSettings.Workspace(settings['workspace'])
        self.storage = TestSettings.Storage(settings['storage'])
        self.intermediateDataset = TestSettings.IntermediateDataset(settings['intermediateDataset'])
        self.diagnostics = TestSettings.Diagnostics(settings['diagnostics'])


def load_test_settings():
    name = "azuremltestsettings.json"
    full_path = path.join(path.abspath(path.dirname(__file__)), name)
    if not path.exists(full_path):
        raise RuntimeError("Cannot run AzureML tests when the expected settings file , '{0}', does not exist!".format(full_path))
    with open(full_path, "r") as f:
        settings = json.load(f)
        return TestSettings(settings)

def id_generator(size=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))
