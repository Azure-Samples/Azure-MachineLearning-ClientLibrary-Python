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

import math
import sys
import unittest
import os
import pandas as pd
import numpy as np
from os import path
from pandas.util.testing import assert_frame_equal

from azure.storage import BlobService
from azureml import (
    BytesIO,
    Workspace,
    DataTypeIds,
    AzureMLConflictHttpError,
    AzureMLHttpError,
    UnsupportedDatasetTypeError,
    serialize_dataframe,
    deserialize_dataframe,
)
from tests import (
    id_generator,
    load_test_settings,
)


EXAMPLE_EXPERIMENT_ID = '506153734175476c4f62416c57734963.f-id.1f022fd4578847dc867d662a51f0a105'
EXAMPLE_EXPERIMENT_DESC = 'Binary Classification: Breast cancer detection'

EXAMPLE_DATASET_NAME = 'Airport Codes Dataset'
EXAMPLE_UNSUPPORTED_DATASET_NAME = 'Breast cancer data'

settings = load_test_settings()


class WorkspaceTests(unittest.TestCase):
    def test_create(self):
        # Arrange

        # Act
        workspace = Workspace(
            workspace_id=settings.workspace.id,
            authorization_token=settings.workspace.token,
            endpoint=settings.workspace.endpoint
        )

        # Assert

    def test_create_ini(self):
        # Arrange
        try:
            with open(path.expanduser('~/.azureml/settings.ini'), 'w') as config:
                config.write('''
[workspace]
id=test_id
authorization_token=test_token
api_endpoint=api_endpoint
management_endpoint=management_endpoint
''')

            workspace = Workspace()
            # Assert
            self.assertEqual(workspace.workspace_id, 'test_id')
            self.assertEqual(workspace.authorization_token, 'test_token')
            self.assertEqual(workspace.api_endpoint, 'api_endpoint')
            self.assertEqual(workspace.management_endpoint, 'management_endpoint')
        finally:
            if path.exists(path.expanduser('~/.azureml/settings.ini')):
                os.unlink(path.expanduser('~/.azureml/settings.ini'))

    def test_create_json(self):
        # Arrange

        # Act

        try:
            with open(path.expanduser('~/.azureml/settings.json'), 'w') as config:
                config.write('''
{"workspace":{
  "id":"test_id",
  "authorization_token": "test_token",
  "api_endpoint":"api_endpoint",
  "management_endpoint":"management_endpoint"
}}''')

            workspace = Workspace()
            # Assert
            self.assertEqual(workspace.workspace_id, 'test_id')
            self.assertEqual(workspace.authorization_token, 'test_token')
            self.assertEqual(workspace.api_endpoint, 'api_endpoint')
            self.assertEqual(workspace.management_endpoint, 'management_endpoint')
        finally:
            if path.exists(path.expanduser('~/.azureml/settings.json')):
                os.unlink(path.expanduser('~/.azureml/settings.json'))


    def test_create_no_workspace_id(self):
        # Arrange

        # Act
        with self.assertRaises(TypeError):
            workspace = Workspace(
                workspace_id='',
                authorization_token=settings.workspace.token,
            )

        # Assert

    def test_create_no_workspace_token(self):
        # Arrange

        # Act
        with self.assertRaises(TypeError):
            workspace = Workspace(
                workspace_id=settings.workspace.id,
                authorization_token='',
            )

        # Assert

    def test_create_no_endpoint(self):
        # Arrange

        # Act
        with self.assertRaises(TypeError):
            workspace = Workspace(
                workspace_id=settings.workspace.id,
                authorization_token=settings.workspace.token,
                endpoint=None
            )

        # Assert


class ExperimentsTests(unittest.TestCase):
    def setUp(self):
        self.workspace = Workspace(
            settings.workspace.id,
            settings.workspace.token,
            settings.workspace.endpoint
        )

    def test_iter(self):
        # Arrange

        # Act
        all = []
        for experiment in self.workspace.experiments:
            all.append(experiment)
            print(experiment.experiment_id)
            print(experiment.description.encode('ascii', 'ignore'))
            print('')

        # Assert
        self.assertGreater(len(all), 0)

    def test_iter_example_experiments(self):
        # Arrange

        # Act
        all = []
        for experiment in self.workspace.example_experiments:
            all.append(experiment)
            print(experiment.experiment_id)
            print(experiment.description.encode('ascii', 'ignore'))
            print('')
            self.assertTrue(experiment.is_example)

        # Assert
        self.assertGreater(len(all), 0)
        self.assertEqual(1, len([e for e in all if e.description == EXAMPLE_EXPERIMENT_DESC]))

    def test_iter_user_experiments(self):
        # Arrange

        # Act
        all = []
        for experiment in self.workspace.user_experiments:
            all.append(experiment)
            print(experiment.experiment_id)
            print(experiment.description.encode('ascii', 'ignore'))
            print('')
            self.assertFalse(experiment.is_example)

        # Assert
        self.assertGreater(len(all), 0)
        self.assertEqual(0, len([e for e in all if e.description == EXAMPLE_EXPERIMENT_DESC]))

    def test_len(self):
        # Arrange

        # Act
        result = len(self.workspace.experiments)

        # Assert
        self.assertGreater(result, 0)

    def test_getitem_by_index(self):
        # Arrange

        # Act
        result = self.workspace.experiments[0]

        # Assert
        self.assertIsNotNone(result)

    def test_getitem_by_index_long(self):
        if sys.version_info >= (3,):
            return

        # Arrange

        # Act
        index = long(0) # can't use 0L as that breaks 3.x parsing
        result = self.workspace.experiments[index]

        # Assert
        self.assertIsNotNone(result)

    def test_getitem_by_index_out_of_range(self):
        # Arrange

        # Act
        with self.assertRaises(IndexError):
            result = self.workspace.experiments[32700]

        # Assert

    def test_getitem_by_id(self):
        # Arrange

        # Act
        id = settings.intermediateDataset.experiment_id
        result = self.workspace.experiments[id]

        # Assert
        self.assertIsNotNone(result)
        self.assertEqual(result.experiment_id, id)

    def test_getitem_by_id_does_not_exist(self):
        # Arrange

        # Act
        with self.assertRaises(IndexError):
            result = self.workspace.experiments['Does Not Exist']

        # Assert

    def test_repr(self):
        # Arrange

        # Act
        result = repr(self.workspace.example_experiments)

        # Assert
        self.assertIn(EXAMPLE_EXPERIMENT_DESC, result)


class ExperimentTests(unittest.TestCase):
    def setUp(self):
        self.workspace = Workspace(
            settings.workspace.id,
            settings.workspace.token,
            settings.workspace.endpoint
        )

    def assertArrayEqual(self, a, b):
        if sys.version_info < (3,):
            self.assertItemsEqual(a, b)
        else:
            self.assertCountEqual(a, b)

    def test_metadata(self):
        # Arrange
        experiment = self.workspace.experiments[
            settings.intermediateDataset.experiment_id]

        # Act
        print('status.status_code: {0}'.format(experiment.status.status_code))
        print('status.status_detail: {0}'.format(experiment.status.status_detail))
        print('status.creation_time: {0}'.format(experiment.status.creation_time))
        print('description: {0}'.format(experiment.description.encode('ascii','ignore')))
        print('creator: {0}'.format(experiment.creator))
        print('experiment_id: {0}'.format(experiment.experiment_id))
        print('job_id: {0}'.format(experiment.job_id))
        print('version_id: {0}'.format(experiment.version_id))
        print('etag: {0}'.format(experiment.etag))
        print('run_id: {0}'.format(experiment.run_id))
        print('is_archived: {0}'.format(experiment.is_archived))
        print('is_example: {0}'.format(experiment.is_example))

        # Assert

    def test_repr(self):
        # Arrange
        experiment = self.workspace.experiments[
            settings.intermediateDataset.experiment_id]

        # Act
        result = repr(experiment)

        # Assert
        expected = u'{0}\t{1}'.format(experiment.experiment_id, experiment.description)
        if sys.version_info < (3,):
            self.assertEqual(type(result), bytes)
            self.assertEqual(result, expected.encode('ascii', 'ignore'))
        else:
            self.assertEqual(type(result), str)
            self.assertEqual(result, expected)

    def test_get_intermediate_dataset(self):
        # Arrange
        experiment = self.workspace.experiments[
            settings.intermediateDataset.experiment_id]

        # Act
        result = experiment.get_intermediate_dataset(
            settings.intermediateDataset.node_id,
            settings.intermediateDataset.port_name,
            settings.intermediateDataset.data_type_id
        )

        # Assert
        self.assertIsNotNone(result)
        self.assertEqual(result.workspace, self.workspace)
        self.assertEqual(result.experiment, experiment)
        self.assertEqual(result.node_id, settings.intermediateDataset.node_id)
        self.assertEqual(result.port_name, settings.intermediateDataset.port_name)
        self.assertEqual(result.data_type_id, settings.intermediateDataset.data_type_id)


class IntermediateDatasetTests(unittest.TestCase):
    def setUp(self):
        self.workspace = Workspace(
            settings.workspace.id,
            settings.workspace.token,
            settings.workspace.endpoint
        )

        self.experiment = self.workspace.experiments[
            settings.intermediateDataset.experiment_id]

        self.dataset = self.experiment.get_intermediate_dataset(
            settings.intermediateDataset.node_id,
            settings.intermediateDataset.port_name,
            settings.intermediateDataset.data_type_id
        )

    def test_to_dataframe(self):
        # Arrange

        # Act
        result = self.dataset.to_dataframe()

        # Assert
        self.assertGreater(len(result.columns), 0)
        self.assertGreater(len(result.values[0]), 0)

    def test_to_dataframe_unsupported_data_type_id(self):
        # Arrange
        dataset = self.experiment.get_intermediate_dataset(
            settings.intermediateDataset.node_id,
            settings.intermediateDataset.port_name,
            'Unsupported'
        )

        # Act
        result = hasattr(dataset, 'to_dataframe')

        # Assert
        self.assertFalse(result)

    def test_open(self):
        # Arrange

        # Act
        result = self.dataset.open()

        # Assert
        self.assertIsNotNone(result)
        raw_data = result.read()
        self.assertGreater(len(raw_data), 0)

    def test_read_as_binary(self):
        # Arrange

        # Act
        result = self.dataset.read_as_binary()

        # Assert
        self.assertGreater(len(result), 0)

    def test_read_as_text(self):
        # Arrange

        # Act
        result = self.dataset.read_as_text()

        # Assert
        self.assertGreater(len(result), 0)


class DatasetsTests(unittest.TestCase):
    def setUp(self):
        self.workspace = Workspace(
            settings.workspace.id,
            settings.workspace.token,
            settings.workspace.endpoint
        )

    def test_len(self):
        # Arrange

        # Act
        result = len(self.workspace.datasets)

        # Assert
        self.assertGreater(result, 0)

    def test_getitem_by_index(self):
        # Arrange

        # Act
        result = self.workspace.datasets[0]

        # Assert
        self.assertIsNotNone(result)

    def test_getitem_by_index_long(self):
        if sys.version_info >= (3,):
            return

        # Arrange

        # Act
        index = long(0) # can't use 0L as that breaks 3.x parsing
        result = self.workspace.datasets[index]

        # Assert
        self.assertIsNotNone(result)

    def test_getitem_by_index_out_of_range(self):
        # Arrange

        # Act
        with self.assertRaises(IndexError):
            result = self.workspace.datasets[32700]

        # Assert

    def test_getitem_by_name(self):
        # Arrange

        # Act
        result = self.workspace.datasets[EXAMPLE_DATASET_NAME]

        # Assert
        self.assertIsNotNone(result)
        self.assertEqual(result.name, EXAMPLE_DATASET_NAME)

    def test_getitem_by_name_wrong_case(self):
        # Arrange

        # Act
        with self.assertRaises(IndexError):
            result = self.workspace.datasets[EXAMPLE_DATASET_NAME.upper()]

        # Assert

    def test_getitem_by_name_does_not_exist(self):
        # Arrange

        # Act
        with self.assertRaises(IndexError):
            result = self.workspace.datasets['Does Not Exist']

        # Assert

    def test_iter(self):
        # Arrange

        # Act
        all = []
        for dataset in self.workspace.datasets:
            all.append(dataset)
            print(dataset.name)

        # Assert
        self.assertGreater(len(all), 0)

    def test_iter_example_datasets(self):
        # Arrange

        # Act
        all = []
        for dataset in self.workspace.example_datasets:
            all.append(dataset)
            print(dataset.dataset_id)
            print(dataset.name)
            print(dataset.data_type_id)
            print('')
            self.assertTrue(dataset.is_example)

        # Assert
        self.assertGreater(len(all), 0)
        self.assertEqual(1, len([a for a in all if a.name ==EXAMPLE_DATASET_NAME]))

    def test_iter_user_datasets(self):
        # Arrange

        # Act
        all = []
        for dataset in self.workspace.user_datasets:
            all.append(dataset)
            print(dataset.dataset_id)
            print(dataset.name)
            print(dataset.data_type_id)
            print('')
            self.assertFalse(dataset.is_example)

        # Assert
        self.assertGreater(len(all), 0)
        self.assertEqual(0, len([a for a in all if a.name ==EXAMPLE_DATASET_NAME]))

    def test_repr(self):
        # Arrange

        # Act
        result = repr(self.workspace.example_datasets)

        # Assert
        self.assertIn('{0}\n'.format(EXAMPLE_DATASET_NAME), result)


class UploadTests(unittest.TestCase):
    def setUp(self):
        self.workspace = Workspace(
            settings.workspace.id,
            settings.workspace.token,
            settings.workspace.endpoint
        )

        self.original_data = [{'a': 1, 'b': 2}, {'a': 5, 'b': 10, 'c': 20}]
        self.original_dataframe = pd.DataFrame(self.original_data)
        self.original_name = 'unittestcsvwh' + id_generator()
        self.original_description = 'safe to be deleted - ' + self.original_name

        self.updated_data = [{'a': 101, 'b': 102}, {'a': 105, 'b': 110, 'c': 120}]
        self.updated_dataframe = pd.DataFrame(self.updated_data)
        self.updated_name = 'unittestcsvwhupdate' + id_generator()
        self.updated_description = 'updated'


    def test_add_from_dataframe(self):
        # Arrange

        # Act
        result = self.workspace.datasets.add_from_dataframe(
            self.original_dataframe,
            DataTypeIds.GenericCSV,
            self.original_name,
            self.original_description,
        )

        # Assert
        self.assertIsNotNone(result)
        self.assertEqual(result.name, self.original_name)
        self.assertEqual(result.description, self.original_description)
        self.assertEqual(result.data_type_id, DataTypeIds.GenericCSV)
        self.assertEqual(result.owner, 'Python SDK')
        self.assertIsNotNone(self.workspace.datasets[self.original_name])

    def test_add_from_dataframe_conflict(self):
        # Arrange
        self.workspace.datasets.add_from_dataframe(
            self.original_dataframe,
            DataTypeIds.GenericCSV,
            self.original_name,
            self.original_description,
        )

        # Act
        with self.assertRaises(AzureMLConflictHttpError):
            result = self.workspace.datasets.add_from_dataframe(
                self.original_dataframe,
                DataTypeIds.GenericCSV,
                self.original_name,
                self.original_description,
            )

        # Assert

    def test_update_from_dataframe(self):
        # Arrange
        dataset = self.workspace.datasets.add_from_dataframe(
            self.original_dataframe,
            DataTypeIds.GenericCSV,
            self.original_name,
            self.original_description,
        )

        # Act
        result = dataset.update_from_dataframe(self.updated_dataframe)

        # Assert
        self.assertIsNone(result)
        actual_dataframe = dataset.to_dataframe()
        self.assertEqual(dataset.name, self.original_name)
        self.assertEqual(dataset.description, self.original_description)
        self.assertEqual(dataset.data_type_id, DataTypeIds.GenericCSV)
        assert_frame_equal(actual_dataframe, self.updated_dataframe)

    def test_update_from_dataframe_with_type_id_name_description(self):
        # Arrange
        dataset = self.workspace.datasets.add_from_dataframe(
            self.original_dataframe,
            DataTypeIds.GenericCSV,
            self.original_name,
            self.original_description,
        )

        # Act
        result = dataset.update_from_dataframe(
            self.updated_dataframe,
            DataTypeIds.GenericTSV,
            self.updated_name,
            self.updated_description)

        # Assert
        self.assertIsNone(result)
        actual_dataframe = dataset.to_dataframe()
        self.assertEqual(dataset.name, self.updated_name)
        self.assertEqual(dataset.description, self.updated_description)
        self.assertEqual(dataset.data_type_id, DataTypeIds.GenericTSV)
        assert_frame_equal(actual_dataframe, self.updated_dataframe)

    def test_add_from_dataframe_invalid_name(self):
        # Arrange
        invalid_name = 'unittestcsvwh:' + id_generator()

        # Act
        try:
            result = self.workspace.datasets.add_from_dataframe(
                self.original_dataframe,
                DataTypeIds.GenericCSV,
                invalid_name,
                self.original_description,
            )
            self.assertTrue(False, 'Failed to raise AzureMLHttpError.')
        except AzureMLHttpError as error:
            self.assertIn('forbidden characters', str(error))
            self.assertEqual(error.status_code, 400)

        # Assert

    def test_add_from_raw_data(self):
        # Arrange
        original_raw_data = _frame_to_raw_data(self.original_dataframe, ',', True)

        # Act
        result = self.workspace.datasets.add_from_raw_data(
            original_raw_data,
            DataTypeIds.GenericCSV,
            self.original_name,
            self.original_description,
        )

        # Assert
        self.assertIsNotNone(result)
        self.assertIsNotNone(self.workspace.datasets[self.original_name])
        self.assertEqual(result.name, self.original_name)
        self.assertEqual(result.description, self.original_description)

    def test_update_from_raw_data(self):
        # Arrange
        dataset = self.workspace.datasets.add_from_dataframe(
            self.original_dataframe,
            DataTypeIds.GenericCSV,
            self.original_name,
            self.original_description,
        )

        updated_raw_data = _frame_to_raw_data(self.updated_dataframe, ',', True)

        # Act
        result = dataset.update_from_raw_data(updated_raw_data)

        # Assert
        self.assertIsNone(result)
        actual_dataframe = dataset.to_dataframe()
        self.assertEqual(dataset.name, self.original_name)
        self.assertEqual(dataset.description, self.original_description)
        self.assertEqual(dataset.data_type_id, DataTypeIds.GenericCSV)
        assert_frame_equal(actual_dataframe, self.updated_dataframe)

    def test_update_from_raw_data_with_data_type_id_name_description(self):
        # Arrange
        dataset = self.workspace.datasets.add_from_dataframe(
            self.original_dataframe,
            DataTypeIds.GenericCSV,
            self.original_name,
            self.original_description,
        )

        updated_raw_data = _frame_to_raw_data(self.updated_dataframe, '\t', True)

        # Act
        result = dataset.update_from_raw_data(
            updated_raw_data,
            DataTypeIds.GenericTSV,
            self.updated_name,
            self.updated_description,
        )

        # Assert
        self.assertIsNone(result)
        actual_dataframe = dataset.to_dataframe()
        self.assertEqual(dataset.name, self.updated_name)
        self.assertEqual(dataset.description, self.updated_description)
        self.assertEqual(dataset.data_type_id, DataTypeIds.GenericTSV)
        assert_frame_equal(actual_dataframe, self.updated_dataframe)

    def test_update_from_dataframe_example_dataset(self):
        # Arrange
        dataset = self.workspace.example_datasets[0]

        # Act
        result = hasattr(dataset, 'update_from_dataframe')

        # Assert
        self.assertFalse(result)

    def test_update_from_raw_data_example_dataset(self):
        # Arrange
        dataset = self.workspace.example_datasets[0]

        # Act
        result = hasattr(dataset, 'update_from_raw_data')

        # Assert
        self.assertFalse(result)


class DatasetTests(unittest.TestCase):
    def setUp(self):
        self.workspace = Workspace(
            settings.workspace.id,
            settings.workspace.token,
            settings.workspace.endpoint
        )

    def assertArrayEqual(self, a, b):
        if sys.version_info < (3,):
            self.assertItemsEqual(a, b)
        else:
            self.assertCountEqual(a, b)

    def test_metadata(self):
        # Arrange
        dataset = self.workspace.datasets[EXAMPLE_DATASET_NAME]

        # Act
        print('visualize_end_point.base_uri: {0}'.format(dataset.visualize_end_point.base_uri))
        print('visualize_end_point.size: {0}'.format(dataset.visualize_end_point.size))
        print('visualize_end_point.endpoint_type: {0}'.format(dataset.visualize_end_point.endpoint_type))
        print('visualize_end_point.credential_container: {0}'.format(dataset.visualize_end_point.credential_container))
        print('visualize_end_point.access_credential: {0}'.format(dataset.visualize_end_point.access_credential))
        print('visualize_end_point.location: {0}'.format(dataset.visualize_end_point.location))
        print('visualize_end_point.file_type: {0}'.format(dataset.visualize_end_point.file_type))
        print('visualize_end_point.is_auxiliary: {0}'.format(dataset.visualize_end_point.is_auxiliary))
        print('visualize_end_point.name: {0}'.format(dataset.visualize_end_point.name))
        print('schema_end_point.base_uri: {0}'.format(dataset.schema_end_point.base_uri))
        print('schema_end_point.size: {0}'.format(dataset.schema_end_point.size))
        print('schema_end_point.endpoint_type: {0}'.format(dataset.schema_end_point.endpoint_type))
        print('schema_end_point.credential_container: {0}'.format(dataset.schema_end_point.credential_container))
        print('schema_end_point.access_credential: {0}'.format(dataset.schema_end_point.access_credential))
        print('schema_end_point.location: {0}'.format(dataset.schema_end_point.location))
        print('schema_end_point.file_type: {0}'.format(dataset.schema_end_point.file_type))
        print('schema_end_point.is_auxiliary: {0}'.format(dataset.schema_end_point.is_auxiliary))
        print('schema_end_point.name: {0}'.format(dataset.schema_end_point.name))
        print('schema_status: {0}'.format(dataset.schema_status))
        print('dataset_id: {0}'.format(dataset.dataset_id))
        print('data_type_id: {0}'.format(dataset.data_type_id))
        print('name: {0}'.format(dataset.name))
        print('description: {0}'.format(dataset.description))
        print('family_id: {0}'.format(dataset.family_id))
        print('resource_upload_id: {0}'.format(dataset.resource_upload_id))
        print('source_origin: {0}'.format(dataset.source_origin))
        print('size: {0}'.format(dataset.size))
        print('created_date: {0}'.format(dataset.created_date))
        print('owner: {0}'.format(dataset.owner))
        print('experiment_id: {0}'.format(dataset.experiment_id))
        print('client_version: {0}'.format(dataset.client_version))
        print('promoted_from: {0}'.format(dataset.promoted_from))
        print('uploaded_from_filename: {0}'.format(dataset.uploaded_from_filename))
        print('service_version: {0}'.format(dataset.service_version))
        print('is_latest: {0}'.format(dataset.is_latest))
        print('category: {0}'.format(dataset.category))
        print('download_location.base_uri: {0}'.format(dataset.download_location.base_uri))
        print('download_location.size: {0}'.format(dataset.download_location.size))
        print('download_location.endpoint_type: {0}'.format(dataset.download_location.endpoint_type))
        print('download_location.credential_container: {0}'.format(dataset.download_location.credential_container))
        print('download_location.access_credential: {0}'.format(dataset.download_location.access_credential))
        print('download_location.location: {0}'.format(dataset.download_location.location))
        print('download_location.file_type: {0}'.format(dataset.download_location.file_type))
        print('download_location.is_auxiliary: {0}'.format(dataset.download_location.is_auxiliary))
        print('download_location.name: {0}'.format(dataset.download_location.name))
        print('is_deprecated: {0}'.format(dataset.is_deprecated))
        print('culture: {0}'.format(dataset.culture))
        print('batch: {0}'.format(dataset.batch))
        print('created_date_ticks: {0}'.format(dataset.created_date_ticks))

        # Assert

    def test_repr(self):
        # Arrange
        dataset = self.workspace.datasets[EXAMPLE_DATASET_NAME]

        # Act
        result = repr(dataset)

        # Assert
        self.assertEqual(dataset.name, result)

    def test_to_dataframe(self):
        # Arrange
        dataset = self.workspace.datasets[EXAMPLE_DATASET_NAME]

        # Act
        result = dataset.to_dataframe()

        # Assert
        self.assertArrayEqual(
            result.columns,
            [u'airport_id', u'city', u'state', u'name'])
        self.assertArrayEqual(
            result.values[0],
            [10165, 'Adak Island', 'AK', 'Adak'])
        self.assertArrayEqual(
            result.values[-1],
            [14543, 'Rock Springs', 'WY', 'Rock Springs Sweetwater County'])

    def test_to_dataframe_unsupported_data_type_id(self):
        # Arrange
        dataset = self.workspace.datasets[EXAMPLE_UNSUPPORTED_DATASET_NAME]

        # Act
        result = hasattr(dataset, 'to_dataframe')

        # Assert
        self.assertFalse(result)

    def test_open(self):
        # Arrange
        dataset = self.workspace.datasets[EXAMPLE_DATASET_NAME]

        # Act
        result = dataset.open()

        # Assert
        self.assertIsNotNone(result)
        raw_data = result.read()
        expected = b'airport_id,city,state,name\r\n10165,Adak Island, AK, Adak'
        self.assertEqual(raw_data[:len(expected)], expected)

    def test_read_as_binary(self):
        # Arrange
        dataset = self.workspace.datasets[EXAMPLE_DATASET_NAME]

        # Act
        result = dataset.read_as_binary()

        # Assert
        expected = b'airport_id,city,state,name\r\n10165,Adak Island, AK, Adak'
        self.assertEqual(result[:len(expected)], expected)

    def test_read_as_text(self):
        # Arrange
        dataset = self.workspace.datasets[EXAMPLE_DATASET_NAME]

        # Act
        result = dataset.read_as_text()

        # Assert
        lines = result.splitlines()
        self.assertEqual(lines[0], 'airport_id,city,state,name')
        self.assertEqual(lines[1], '10165,Adak Island, AK, Adak')
        self.assertEqual(lines[-1], '14543,Rock Springs, WY, Rock Springs Sweetwater County')


class SerializationTests(unittest.TestCase):
    def assertArrayEqual(self, a, b):
        if sys.version_info < (3,):
            self.assertItemsEqual(a, b)
        else:
            self.assertCountEqual(a, b)

    def test_serialize_to_csv(self):
        # Arrange
        data = [{'a': 1.0, 'b': 2.0}, {'a': 5.1, 'b': 10.1, 'c': 20.1}]
        dataframe = pd.DataFrame(data)

        # Act
        writer = BytesIO()
        serialize_dataframe(writer, DataTypeIds.GenericCSV, dataframe)
        result = writer.getvalue()

        # Assert
        self.assertGreater(len(result), 0)
        self.assertEqual(result, b'a,b,c\n1.0,2.0,\n5.1,10.1,20.1\n')

    def test_serialize_to_csv_no_header(self):
        # Arrange
        data = [{'a': 1.0, 'b': 2.0}, {'a': 5.1, 'b': 10.1, 'c': 20.1}]
        dataframe = pd.DataFrame(data)

        # Act
        writer = BytesIO()
        serialize_dataframe(writer, DataTypeIds.GenericCSVNoHeader, dataframe)
        result = writer.getvalue()

        # Assert
        self.assertGreater(len(result), 0)
        self.assertEqual(result, b'1.0,2.0,\n5.1,10.1,20.1\n')

    def test_serialize_to_tsv(self):
        # Arrange
        data = [{'a': 1.0, 'b': 2.0}, {'a': 5.1, 'b': 10.1, 'c': 20.1}]
        dataframe = pd.DataFrame(data)

        # Act
        writer = BytesIO()
        serialize_dataframe(writer, DataTypeIds.GenericTSV, dataframe)
        result = writer.getvalue()

        # Assert
        self.assertGreater(len(result), 0)
        self.assertEqual(result, b'a\tb\tc\n1.0\t2.0\t\n5.1\t10.1\t20.1\n')

    def test_serialize_to_tsv_no_header(self):
        # Arrange
        data = [{'a': 1.0, 'b': 2.0}, {'a': 5.1, 'b': 10.1, 'c': 20.1}]
        dataframe = pd.DataFrame(data)

        # Act
        writer = BytesIO()
        serialize_dataframe(writer, DataTypeIds.GenericTSVNoHeader, dataframe)
        result = writer.getvalue()

        # Assert
        self.assertGreater(len(result), 0)
        self.assertEqual(result, b'1.0\t2.0\t\n5.1\t10.1\t20.1\n')

    def test_serialize_to_plain_text(self):
        # Arrange
        data = ['This is the first', 'This is second line']
        dataframe = pd.DataFrame(data)

        # Act
        writer = BytesIO()
        serialize_dataframe(writer, DataTypeIds.PlainText, dataframe)
        result = writer.getvalue()

        # Assert
        self.assertGreater(len(result), 0)
        self.assertEqual(result, b'This is the first\nThis is second line\n')

    def test_deserialize_from_plain_text_bom(self):
        # Arrange
        data = b'\xef\xbb\xbfJohn enjoyed his vacation in California. His personal favorite on the trip was Los Angeles.\r\nMicrosoft announced upgrades to their line of products for information workers. The announcement was made at a partner conference at Boston.'

        # Act
        reader = BytesIO(data)
        result = deserialize_dataframe(reader, DataTypeIds.PlainText)

        # Assert
        self.assertIsNotNone(result)
        expected = [
            {0: 'John enjoyed his vacation in California. His personal favorite on the trip was Los Angeles.'},
            {0: 'Microsoft announced upgrades to their line of products for information workers. The announcement was made at a partner conference at Boston.'},
        ]
        assert_frame_equal(pd.DataFrame(expected), result)

    def test_deserialize_from_csv(self):
        # Arrange
        data = b'a,b,c\n1.0,2.0,nan\n5.1,10.1,20.1\n50.2,,50.3\n'

        # Act
        reader = BytesIO(data)
        result = deserialize_dataframe(reader, DataTypeIds.GenericCSV)

        # Assert
        self.assertIsNotNone(result)
        expected = [
            {'a': 1.0, 'b': 2.0},
            {'a': 5.1, 'b': 10.1, 'c': 20.1},
            {'a': 50.2, 'c': 50.3},
        ]
        assert_frame_equal(pd.DataFrame(expected), result)

    def test_deserialize_from_csv_bom(self):
        # Arrange
        data = b'\xef\xbb\xbfa,b,c\n1.0,2.0,nan\n5.1,10.1,20.1\n50.2,,50.3\n'

        # Act
        reader = BytesIO(data)
        result = deserialize_dataframe(reader, DataTypeIds.GenericCSV)

        # Assert
        self.assertIsNotNone(result)
        expected = [
            {'a': 1.0, 'b': 2.0},
            {'a': 5.1, 'b': 10.1, 'c': 20.1},
            {'a': 50.2, 'c': 50.3},
        ]
        assert_frame_equal(pd.DataFrame(expected), result)

    def test_deserialize_from_csv_spaces(self):
        # Arrange
        data = b'a, b, c\n1.0, two, nan\n5.1, "ten point one", 20.1\n50.2, , 50.3\n'

        # Act
        reader = BytesIO(data)
        result = deserialize_dataframe(reader, DataTypeIds.GenericCSV)

        # Assert
        self.assertIsNotNone(result)
        expected = [
            {'a': 1.0, 'b': 'two'},
            {'a': 5.1, 'b': 'ten point one', 'c': 20.1},
            {'a': 50.2, 'c': 50.3},
        ]
        assert_frame_equal(pd.DataFrame(expected), result)

    def test_deserialize_from_csv_no_header(self):
        # Arrange
        data = b'1.0,2.0,nan\n5.1,10.1,20.1\n50.2,,50.3\n'

        # Act
        reader = BytesIO(data)
        result = deserialize_dataframe(reader, DataTypeIds.GenericCSVNoHeader)

        # Assert
        self.assertIsNotNone(result)
        expected = [
            {0: 1.0, 1: 2.0},
            {0: 5.1, 1: 10.1, 2: 20.1},
            {0: 50.2, 2: 50.3},
        ]
        assert_frame_equal(pd.DataFrame(expected), result)

    @unittest.skip('ARFF is not supported yet.')
    def test_deserialize_from_arff(self):
        # Arrange
        data = b"""@RELATION	Unnamed

@ATTRIBUTE	Class	NUMERIC
@ATTRIBUTE	age	NUMERIC
@ATTRIBUTE	menopause	NUMERIC
@ATTRIBUTE	tumor-size	NUMERIC

@DATA
0,5,1,1
0,5,4,4
1,4,8,8

"""

        # Act
        reader = BytesIO(data)
        result = deserialize_dataframe(reader, DataTypeIds.ARFF)
        print(result)

        # Assert
        self.assertIsNotNone(result)
        expected = [
            {'Class': 0., 'age': 5., 'menopause': 1., 'tumor-size':1.},
            {'Class': 0., 'age': 5., 'menopause': 4., 'tumor-size':4.},
            {'Class': 1., 'age': 4., 'menopause': 8., 'tumor-size':8.},
        ]
        assert_frame_equal(pd.DataFrame(expected), result)

    def test_deserialize_from_unsupported_data_type_id(self):
        # Arrange
        data = b'1.0,2.0,nan\n5.1,10.1,20.1\n50.2,,50.3\n'

        # Act
        reader = BytesIO(data)
        with self.assertRaises(UnsupportedDatasetTypeError):
            result = deserialize_dataframe(reader, 'Unsupported')

        # Assert


def _frame_to_raw_data(dataframe, sep, header):
    return dataframe.to_csv(sep=sep, header=header, index=False, encoding='utf-8')


if __name__ == '__main__':
    unittest.main()
