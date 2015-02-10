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
from pandas.util.testing import assert_frame_equal

from azure.storage import BlobService
from azureml import (
    BytesIO,
    Workspace,
    DataTypeIds,
)
from tests import (
    id_generator,
    load_test_settings,
)


settings = load_test_settings()


class RoundTripTests(unittest.TestCase):
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

    def test_download_blob_then_upload_as_dataframe_then_read_dataset(self):
        def datatypeid_from_header_and_format(header, format):
            if format == 'csv':
                if header == 'wh':
                    return DataTypeIds.GenericCSV
                else:
                    return DataTypeIds.GenericCSVNoHeader
            elif format == 'tsv':
                if header == 'wh':
                    return DataTypeIds.GenericTSV
                else:
                    return DataTypeIds.GenericTSVNoHeader
            elif format == 'txt':
                return DataTypeIds.PlainText
            else:
                self.assertTrue(False, 'Unexpected format')

        def split_blob_name(blob_name):
            # blob naming convention:
            # name_<header>.<format>
            # <header>: WH: with header
            #           NH: no header
            # <format>: CSV: comma separated
            #           TSV: tab separated
            #           TXT: newline separated
            name, format = blob_name.lower().split('.')
            if format != 'txt':
                name, header = name.split('_')
            else:
                header = 'nh'

            return name, format, header

        for blob_name in settings.storage.blobs:
            print(blob_name)

            name, format, header = split_blob_name(blob_name)

            # Read the data from blob storage
            original_data = self.blob.get_blob_to_bytes(settings.storage.container, blob_name)
            self._write_blob_contents(blob_name, original_data)

            # Parse the data to a dataframe using Pandas
            original_dataframe = pd.read_csv(
                BytesIO(original_data),
                header=0 if header == 'wh' else None,
                sep=',' if format == 'csv' else '\t' if format == 'tsv' else '\n',
                encoding='utf-8-sig'
            )

            # Upload the dataframe as a new dataset
            dataset_name = 'unittest' + name + id_generator()
            description = 'safe to be deleted - ' + dataset_name
            data_type_id = datatypeid_from_header_and_format(header, format)
            self.workspace.datasets.add_from_dataframe(
                original_dataframe,
                data_type_id,
                dataset_name,
                description,
            )

            # Get the new dataset
            dataset = self.workspace.datasets[dataset_name]
            self.assertIsNotNone(dataset)

            # Read the dataset as a dataframe
            result_data = dataset.read_as_binary()
            self._write_serialized_frame(blob_name, result_data)
            result_dataframe = dataset.to_dataframe()

            # Verify that the dataframes are equal
            assert_frame_equal(original_dataframe, result_dataframe)

    def test_azureml_example_datasets(self):
        max_size = 10 * 1024 * 1024
        skip = [
            'Restaurant feature data',
            'IMDB Movie Titles',
            'Book Reviews from Amazon',
        ]

        for dataset in self.workspace.example_datasets:
            if not hasattr(dataset, 'to_dataframe'):
                print('skipped (unsupported format): {0}'.format(dataset.name))
                continue

            if dataset.size > max_size:
                print('skipped (max size): {0}'.format(dataset.name))
                continue

            if dataset.name in skip:
                print('skipped: {0}'.format(dataset.name))
                continue

            print('downloading: ' + dataset.name)
            frame = dataset.to_dataframe()

            print('uploading: ' + dataset.name)
            dataset_name = 'unittest' + dataset.name + id_generator()
            description = 'safe to be deleted - ' + dataset_name
            self.workspace.datasets.add_from_dataframe(frame, dataset.data_type_id, dataset_name, description)


if __name__ == '__main__':
    unittest.main()
