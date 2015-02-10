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
import pandas as pd
from datetime import datetime
from pandas.util.testing import assert_frame_equal

from azure.storage import BlobService
from azureml import (
    BytesIO,
    Workspace,
    DataTypeIds,
    serialize_dataframe,
)
from tests import (
    load_test_settings,
)


settings = load_test_settings()


class PerformanceTests(unittest.TestCase):
    def setUp(self):
        self.workspace = Workspace(
            settings.workspace.id,
            settings.workspace.token,
            settings.workspace.endpoint
        )
        self.blob = BlobService(
            settings.storage.account_name,
            settings.storage.account_key
        )

    def _write_blob_contents(self, filename, data):
        if settings.diagnostics.write_blob_contents:
            with open('original-blob-' + filename, 'wb') as data_file:
                data_file.write(data)

    def _write_serialized_frame(self, filename, data):
        if settings.diagnostics.write_serialized_frame:
            with open('serialized-frame-' + filename, 'wb') as data_file:
                data_file.write(data)

    def test_serialize_40mb_dataframe(self):
        # Arrange
        blob_name = settings.storage.medium_size_blob
        original_data = self.blob.get_blob_to_bytes(settings.storage.container, blob_name)
        original_dataframe = pd.read_csv(BytesIO(original_data), header=0, sep=",", encoding='utf-8-sig')

        self._write_blob_contents(blob_name, original_data)

        # Act
        start_time = datetime.now()
        writer = BytesIO()
        serialize_dataframe(writer, DataTypeIds.GenericCSV, original_dataframe)
        elapsed_time = datetime.now() - start_time
        result_data = writer.getvalue()

        self._write_serialized_frame(blob_name, result_data)

        # Assert
        result_dataframe = pd.read_csv(BytesIO(result_data), header=0, sep=",", encoding='utf-8-sig')
        assert_frame_equal(original_dataframe, result_dataframe)
        self.assertLess(elapsed_time.total_seconds(), 10)


if __name__ == '__main__':
    unittest.main()
