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

from datetime import datetime

import numbers
import re
import sys
import json
try:
        import ConfigParser
except ImportError:
        import configparser as ConfigParser

from os import path

try:
    from cStringIO import BytesIO
except ImportError:
    from io import BytesIO

from azureml.errors import (
    AzureMLConflictHttpError,
    AzureMLError,
    AzureMLHttpError,
    UnsupportedDatasetTypeError,
    _not_none,
    _not_none_or_empty,
)
from azureml.http import (
    _RestClient,
    __author__,
    __version__,
)
from azureml.serialization import (
    DataTypeIds,
    deserialize_dataframe,
    serialize_dataframe,
    is_supported,
)


_GLOBAL_WORKSPACE_ID = '506153734175476c4f62416c57734963'


class Endpoints(object):
    """Constants for the known REST API endpoints."""
    default = 'https://studio.azureml.net'
    management_default = 'https://management.azureml.net'


class Dataset(object):
    """Abstract base class for Azure ML datasets."""
    pass


class SourceDataset(Dataset):
    """Metadata for a dataset and methods to read its contents."""

    def __init__(self, workspace=None, metadata=None):
        """
        INTERNAL USE ONLY. Initialize a dataset.

        Parameters
        ----------
        workspace : Workspace
            Parent workspace of the dataset.
        metadata : dict
            Dictionary of dataset metadata as returned by the REST API.
        """
        _not_none('metadata', metadata)
        _not_none('workspace', workspace)

        self.workspace = workspace
        self._metadata = metadata

        if is_supported(self.data_type_id):
            self.to_dataframe = self._to_dataframe

        if not self.is_example:
            self.update_from_raw_data = self._update_from_raw_data
            self.update_from_dataframe = self._update_from_dataframe

    @staticmethod
    def _metadata_repr(metadata):
        val = metadata['Name']
        if sys.version_info < (3,):
            return val.encode('ascii','ignore')
        else:
            return val

    def __repr__(self):
        return SourceDataset._metadata_repr(self._metadata)

    def open(self):
        '''Open and return a stream for the dataset contents.'''
        return self.workspace._rest.open_dataset_contents(self.contents_url)

    def read_as_binary(self):
        '''Read and return the dataset contents as binary.'''
        return self.workspace._rest.read_dataset_contents_binary(self.contents_url)

    def read_as_text(self):
        '''Read and return the dataset contents as text.'''
        return self.workspace._rest.read_dataset_contents_text(self.contents_url)

    def _to_dataframe(self):
        """Read and return the dataset contents as a pandas DataFrame."""
        with self.open() as reader:
            return deserialize_dataframe(reader, self.data_type_id)

    def _update_from_dataframe(self, dataframe, data_type_id=None, name=None,
                              description=None):
        """
        Serialize the specified DataFrame and replace the existing dataset.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data to serialize.
        data_type_id : str, optional
            Format to serialize to.
            If None, the existing format is preserved.
            Supported formats are:
                'PlainText'
                'GenericCSV'
                'GenericTSV'
                'GenericCSVNoHeader'
                'GenericTSVNoHeader'
            See the azureml.DataTypeIds class for constants.
        name : str, optional
            Name for the dataset.
            If None, the name of the existing dataset is used.
        description : str, optional
            Description for the dataset.
            If None, the name of the existing dataset is used.
        """
        _not_none('dataframe', dataframe)

        if data_type_id is None:
            data_type_id = self.data_type_id
        if name is None:
            name = self.name
        if description is None:
            description = self.description

        try:
            output = BytesIO()
            serialize_dataframe(output, data_type_id, dataframe)
            raw_data = output.getvalue()
        finally:
            output.close()

        self._upload_and_refresh(raw_data, data_type_id, name, description)

    def _update_from_raw_data(self, raw_data, data_type_id=None, name=None,
                             description=None):
        """
        Upload already serialized raw data and replace the existing dataset.

        Parameters
        ----------
        raw_data: bytes
            Dataset contents to upload.
        data_type_id : str
            Serialization format of the raw data.
            If None, the format of the existing dataset is used.
            Supported formats are:
                'PlainText'
                'GenericCSV'
                'GenericTSV'
                'GenericCSVNoHeader'
                'GenericTSVNoHeader'
                'ARFF'
            See the azureml.DataTypeIds class for constants.
        name : str, optional
            Name for the dataset.
            If None, the name of the existing dataset is used.
        description : str, optional
            Description for the dataset.
            If None, the name of the existing dataset is used.
        """
        _not_none('raw_data', raw_data)

        if data_type_id is None:
            data_type_id = self.data_type_id
        if name is None:
            name = self.name
        if description is None:
            description = self.description

        self._upload_and_refresh(raw_data, data_type_id, name, description)

    def _upload_and_refresh(self, raw_data, data_type_id, name, description):
        dataset_id = self.workspace._rest.upload_dataset(
            self.workspace.workspace_id,
            name,
            description,
            data_type_id,
            raw_data,
            self.family_id
        )

        self._metadata = self.workspace._rest.get_dataset(
            self.workspace.workspace_id,
            dataset_id
        )

    class Location(object):
        def __init__(self, metadata):
            self._metadata = metadata

        @property
        def base_uri(self):
            """TODO."""
            return self._metadata['BaseUri']

        @property
        def size(self):
            """TODO."""
            return self._metadata['Size']

        @property
        def endpoint_type(self):
            """TODO."""
            return self._metadata['EndpointType']

        @property
        def credential_container(self):
            """TODO."""
            return self._metadata['CredentialContainer']

        @property
        def access_credential(self):
            """TODO."""
            return self._metadata['AccessCredential']

        @property
        def location(self):
            """TODO."""
            return self._metadata['Location']

        @property
        def file_type(self):
            """TODO."""
            return self._metadata['FileType']

        @property
        def is_auxiliary(self):
            """TODO."""
            return self._metadata['IsAuxiliary']

        @property
        def name(self):
            """TODO."""
            return self._metadata['Name']

    @property
    def visualize_end_point(self):
        """TODO."""
        return SourceDataset.Location(self._metadata['VisualizeEndPoint'])

    @property
    def schema_end_point(self):
        """TODO."""
        return SourceDataset.Location(self._metadata['SchemaEndPoint'])

    @property
    def schema_status(self):
        """TODO."""
        return self._metadata['SchemaStatus']

    @property
    def dataset_id(self):
        """Unique identifier for the dataset."""
        return self._metadata['Id']

    @property
    def name(self):
        """Unique name for the dataset."""
        return self._metadata['Name']

    @property
    def data_type_id(self):
        """
        Serialization format for the dataset.
        See the azureml.DataTypeIds class for constants.
        """
        return self._metadata['DataTypeId']

    @property
    def description(self):
        """Description for the dataset."""
        return self._metadata['Description']

    @property
    def resource_upload_id(self):
        """TODO."""
        return self._metadata['ResourceUploadId']

    @property
    def family_id(self):
        """TODO."""
        return self._metadata['FamilyId']

    @property
    def size(self):
        """Size in bytes of the serialized dataset contents."""
        return self._metadata['Size']

    @property
    def source_origin(self):
        """TODO."""
        return self._metadata['SourceOrigin']

    @property
    def created_date(self):
        # Example format of date to parse:
        # /Date(1418444668177)/
        match = re.search(r"/Date\((\d+)\)/", self._metadata['CreatedDate'])
        return datetime.fromtimestamp(int(match.group(1)) / 1000.0)

    @property
    def owner(self):
        """TODO."""
        return self._metadata['Owner']

    @property
    def experiment_id(self):
        """TODO."""
        return self._metadata['ExperimentId']

    @property
    def client_version(self):
        """TODO."""
        return self._metadata['ClientVersion']

    @property
    def promoted_from(self):
        """TODO."""
        return self._metadata['PromotedFrom']

    @property
    def uploaded_from_filename(self):
        """TODO."""
        return self._metadata['UploadedFromFilename']

    @property
    def service_version(self):
        """TODO."""
        return self._metadata['ServiceVersion']

    @property
    def is_latest(self):
        """TODO."""
        return self._metadata['IsLatest']

    @property
    def category(self):
        """TODO."""
        return self._metadata['Category']

    @property
    def download_location(self):
        """TODO."""
        return SourceDataset.Location(self._metadata['DownloadLocation'])

    @property
    def is_deprecated(self):
        """TODO."""
        return self._metadata['IsDeprecated']

    @property
    def culture(self):
        """TODO."""
        return self._metadata['Culture']

    @property
    def batch(self):
        """TODO."""
        return self._metadata['Batch']

    @property
    def created_date_ticks(self):
        """TODO."""
        return self._metadata['CreatedDateTicks']

    @property
    def contents_url(self):
        """Full URL to the dataset contents."""
        loc = self.download_location
        return loc.base_uri + loc.location + loc.access_credential

    @property
    def is_example(self):
        """True for an example dataset, False for user created."""
        return self.dataset_id.startswith(_GLOBAL_WORKSPACE_ID)


class Datasets(object):
    def __init__(self, workspace, example_filter=None):
        """
        INTERNAL USE ONLY. Initialize a dataset collection.

        Parameters
        ----------
        workspace : Workspace
            Parent workspace of the datasets.
        example_filter : bool
            True to include only examples.
            False to include only user-created.
            None to include all.
        """
        _not_none('workspace', workspace)

        self.workspace = workspace
        self._example_filter = example_filter

    def __repr__(self):
        return '\n'.join((SourceDataset._metadata_repr(dataset) for dataset in self._get_datasets()))

    def __iter__(self):
        for dataset in self._get_datasets():
            yield self._create_dataset(dataset)

    def __len__(self):
        return sum(1 for _ in self._get_datasets())

    def __getitem__(self, index):
        '''Retrieve a dataset by index or by name (case-sensitive).'''
        _not_none('index', index)

        datasets = self._get_datasets()
        if isinstance(index, numbers.Integral):
            return self._create_dataset(list(datasets)[index])
        else:
            for dataset in datasets:
                if dataset['Name'] == index:
                    return self._create_dataset(dataset)

        raise IndexError('A data set named "{}" does not exist'.format(index))

    def add_from_dataframe(self, dataframe, data_type_id, name, description):
        """
        Serialize the specified DataFrame and upload it as a new dataset.

        Parameters
        ----------
        dataframe : pandas.DataFrame
            Data to serialize.
        data_type_id : str
            Format to serialize to.
            Supported formats are:
                'PlainText'
                'GenericCSV'
                'GenericTSV'
                'GenericCSVNoHeader'
                'GenericTSVNoHeader'
            See the azureml.DataTypeIds class for constants.
        name : str
            Name for the new dataset.
        description : str
            Description for the new dataset.

        Returns
        -------
        SourceDataset
            Dataset that was just created.
            Use open(), read_as_binary(), read_as_text() or to_dataframe() on
            the dataset object to get its contents as a stream, bytes, str or
            pandas DataFrame.
        """
        _not_none('dataframe', dataframe)
        _not_none_or_empty('data_type_id', data_type_id)
        _not_none_or_empty('name', name)
        _not_none_or_empty('description', description)

        try:
            output = BytesIO()
            serialize_dataframe(output, data_type_id, dataframe)
            raw_data = output.getvalue()
        finally:
            output.close()

        return self._upload(raw_data, data_type_id, name, description)

    def add_from_raw_data(self, raw_data, data_type_id, name, description):
        """
        Upload already serialized raw data as a new dataset.

        Parameters
        ----------
        raw_data: bytes
            Dataset contents to upload.
        data_type_id : str
            Serialization format of the raw data.
            Supported formats are:
                'PlainText'
                'GenericCSV'
                'GenericTSV'
                'GenericCSVNoHeader'
                'GenericTSVNoHeader'
                'ARFF'
            See the azureml.DataTypeIds class for constants.
        name : str
            Name for the new dataset.
        description : str
            Description for the new dataset.

        Returns
        -------
        SourceDataset
            Dataset that was just created.
            Use open(), read_as_binary(), read_as_text() or to_dataframe() on
            the dataset object to get its contents as a stream, bytes, str or
            pandas DataFrame.
        """
        _not_none('raw_data', raw_data)
        _not_none_or_empty('data_type_id', data_type_id)
        _not_none_or_empty('name', name)
        _not_none_or_empty('description', description)

        return self._upload(raw_data, data_type_id, name, description)

    def _upload(self, raw_data, data_type_id, name, description):
        dataset_id = self.workspace._rest.upload_dataset(
            self.workspace.workspace_id, name, description, data_type_id,
            raw_data, None)

        metadata = self.workspace._rest.get_dataset(
            self.workspace.workspace_id, dataset_id)

        return self._create_dataset(metadata)

    def _get_datasets(self):
        datasets = self.workspace._rest.get_datasets(self.workspace.workspace_id)
        return datasets if self._example_filter is None else \
            (d for d in datasets if d['Id'].startswith(
                _GLOBAL_WORKSPACE_ID) == self._example_filter)

    def _create_dataset(self, metadata):
        return SourceDataset(self.workspace, metadata)


class IntermediateDataset(Dataset):
    """Represents an intermediate dataset and methods to read its contents."""

    def __init__(self, workspace, experiment, node_id, port_name, data_type_id):
        """
        INTERNAL USE ONLY. Initialize an intermediate dataset.

        Parameters
        ----------
        workspace : Workspace
            Parent workspace of the dataset.
        experiment : Experiment
            Parent experiment of the dataset.
        node_id : str
            Module node id from the experiment graph.
        port_name : str
            Output port of the module.
        data_type_id : str
            Serialization format of the raw data.
            See the azureml.DataTypeIds class for constants.
        """
        _not_none('workspace', workspace)
        _not_none('experiment', experiment)
        _not_none_or_empty('node_id', node_id)
        _not_none_or_empty('port_name', port_name)
        _not_none_or_empty('data_type_id', data_type_id)

        self.workspace = workspace
        self.experiment = experiment
        self.node_id = node_id
        self.port_name = port_name
        self.data_type_id = data_type_id

        if is_supported(self.data_type_id):
            self.to_dataframe = self._to_dataframe

    def open(self):
        '''Open and return a stream for the dataset contents.'''
        return self.workspace._rest.open_intermediate_dataset_contents(
            self.workspace.workspace_id,
            self.experiment.experiment_id,
            self.node_id,
            self.port_name
        )

    def read_as_binary(self):
        '''Read and return the dataset contents as binary.'''
        return self.workspace._rest.read_intermediate_dataset_contents_binary(
            self.workspace.workspace_id,
            self.experiment.experiment_id,
            self.node_id,
            self.port_name
        )

    def read_as_text(self):
        '''Read and return the dataset contents as text.'''
        return self.workspace._rest.read_intermediate_dataset_contents_text(
            self.workspace.workspace_id,
            self.experiment.experiment_id,
            self.node_id,
            self.port_name
        )

    def _to_dataframe(self):
        """Read and return the dataset contents as a pandas DataFrame."""
        #TODO: figure out why passing in the opened stream directly gives invalid data
        data = self.read_as_binary()
        reader = BytesIO(data)
        return deserialize_dataframe(reader, self.data_type_id)


class Experiment(object):

    def __init__(self, workspace, metadata):
        """
        INTERNAL USE ONLY. Initialize an experiment.

        Parameters
        ----------
        workspace : Workspace
            Parent workspace of the experiment.
        metadata : dict
            Dictionary of experiment metadata as returned by the REST API.
        """
        _not_none('workspace', workspace)
        _not_none('metadata', metadata)

        self.workspace = workspace
        self._metadata = metadata

    @staticmethod
    def _metadata_repr(metadata):
        val = u'{0}\t{1}'.format(metadata['ExperimentId'], metadata['Description'])
        if sys.version_info < (3,):
            return val.encode('ascii','ignore')
        else:
            return val

    def __repr__(self):
        return Experiment._metadata_repr(self._metadata)

    class Status(object):
        def __init__(self, metadata):
            self._metadata = metadata

        @property
        def status_code(self):
            """TODO."""
            return self._metadata['StatusCode']

        @property
        def status_detail(self):
            """TODO."""
            return self._metadata['StatusDetail']

        @property
        def creation_time(self):
            """TODO."""
            # Example format of date to parse:
            # /Date(1418444668177)/
            match = re.search(r"/Date\((\d+)\)/", self._metadata['CreationTime'])
            return datetime.fromtimestamp(int(match.group(1)) / 1000.0)

    @property
    def status(self):
        """TODO."""
        return Experiment.Status(self._metadata['Status'])

    @property
    def description(self):
        """TODO."""
        return self._metadata['Description']

    @property
    def creator(self):
        """TODO."""
        return self._metadata['Creator']

    @property
    def experiment_id(self):
        """TODO."""
        return self._metadata['ExperimentId']

    @property
    def job_id(self):
        """TODO."""
        return self._metadata['JobId']

    @property
    def version_id(self):
        """TODO."""
        return self._metadata['VersionId']

    @property
    def etag(self):
        """TODO."""
        return self._metadata['Etag']

    @property
    def run_id(self):
        """TODO."""
        return self._metadata['RunId']

    @property
    def is_archived(self):
        """TODO."""
        return self._metadata['IsArchived']

    @property
    def is_example(self):
        """True for an example experiment, False for user created."""
        return self.experiment_id.startswith(_GLOBAL_WORKSPACE_ID)

    def get_intermediate_dataset(self, node_id, port_name, data_type_id):
        """
        Get an intermediate dataset.

        Parameters
        ----------
        node_id : str
            Module node id from the experiment graph.
        port_name : str
            Output port of the module.
        data_type_id : str
            Serialization format of the raw data.
            See the azureml.DataTypeIds class for constants.

        Returns
        -------
        IntermediateDataset
            Dataset object.
            Use open(), read_as_binary(), read_as_text() or to_dataframe() on
            the dataset object to get its contents as a stream, bytes, str or
            pandas DataFrame.
        """
        return IntermediateDataset(self.workspace, self, node_id, port_name, data_type_id)


class Experiments(object):
    def __init__(self, workspace, example_filter=None):
        """
        INTERNAL USE ONLY. Initialize an experiment collection.

        Parameters
        ----------
        workspace : Workspace
            Parent workspace of the experiments.
        example_filter : bool
            True to include only examples.
            False to include only user-created.
            None to include all.
        """
        _not_none('workspace', workspace)

        self.workspace = workspace
        self._example_filter = example_filter

    def __repr__(self):
        return '\n'.join((Experiment._metadata_repr(experiment) for experiment in self._get_experiments()))

    def __iter__(self):
        for experiment in self._get_experiments():
            yield self._create_experiment(experiment)

    def __len__(self):
        return sum(1 for _ in self._get_experiments())

    def __getitem__(self, index):
        '''Retrieve an experiment by index or by id.'''
        _not_none('index', index)

        experiments = self._get_experiments()
        if isinstance(index, numbers.Integral):
            return self._create_experiment(list(experiments)[index])
        else:
            for experiment in experiments:
                if experiment['ExperimentId'] == index:
                    return self._create_experiment(experiment)

        raise IndexError('An experiment with the id "{}" does not exist'.format(index))

    def _get_experiments(self):
        experiments = self.workspace._rest.get_experiments(self.workspace.workspace_id)
        return experiments if self._example_filter is None else \
            (e for e in experiments if e['ExperimentId'].startswith(_GLOBAL_WORKSPACE_ID) == self._example_filter)

    def _create_experiment(self, metadata):
        return Experiment(self.workspace, metadata)


_CONFIG_WORKSPACE_SECTION = 'workspace'
_CONFIG_WORKSPACE_ID = 'id'
_CONFIG_AUTHORIZATION_TOKEN = 'authorization_token'
_CONFIG_API_ENDPOINT = 'api_endpoint'
_CONFIG_MANAGEMENT_ENDPOINT = 'management_endpoint'

def _get_workspace_info(workspace_id, authorization_token, endpoint, management_endpoint):
    if workspace_id is None or authorization_token is None or endpoint is None or management_endpoint is None:
        # read the settings from config
        jsonConfig = path.expanduser('~/.azureml/settings.json')
        if path.exists(jsonConfig):
            with open(jsonConfig) as cfgFile:
                config = json.load(cfgFile)
                if _CONFIG_WORKSPACE_SECTION in config:
                    ws = config[_CONFIG_WORKSPACE_SECTION]
                    workspace_id = ws.get(_CONFIG_WORKSPACE_ID, workspace_id)
                    authorization_token = ws.get(_CONFIG_AUTHORIZATION_TOKEN, authorization_token)
                    endpoint = ws.get(_CONFIG_API_ENDPOINT, endpoint)
                    management_endpoint = ws.get(_CONFIG_MANAGEMENT_ENDPOINT, management_endpoint)
        else:
            config = ConfigParser.ConfigParser()
            config.read(path.expanduser('~/.azureml/settings.ini'))
            
            if config.has_section(_CONFIG_WORKSPACE_SECTION):
                if workspace_id is None and config.has_option(_CONFIG_WORKSPACE_SECTION, _CONFIG_WORKSPACE_ID):
                    workspace_id = config.get(_CONFIG_WORKSPACE_SECTION, _CONFIG_WORKSPACE_ID)
                if authorization_token is None and config.has_option(_CONFIG_WORKSPACE_SECTION, _CONFIG_AUTHORIZATION_TOKEN):
                    authorization_token = config.get(_CONFIG_WORKSPACE_SECTION, _CONFIG_AUTHORIZATION_TOKEN)
                if endpoint is None and config.has_option(_CONFIG_WORKSPACE_SECTION, _CONFIG_API_ENDPOINT):
                    endpoint = config.get(_CONFIG_WORKSPACE_SECTION, _CONFIG_API_ENDPOINT)
                if management_endpoint is None and config.has_option(_CONFIG_WORKSPACE_SECTION, _CONFIG_MANAGEMENT_ENDPOINT):
                    management_endpoint = config.get(_CONFIG_WORKSPACE_SECTION, _CONFIG_MANAGEMENT_ENDPOINT)
        
        if workspace_id is None:
            raise ValueError('workspace_id not provided and not available via config')
        if authorization_token is None:
            raise ValueError('authorization_token not provided and not available via config')
        if endpoint is None:
            endpoint = Endpoints.default
        if management_endpoint is None:
            management_endpoint = Endpoints.management_default

    return workspace_id, authorization_token, endpoint, management_endpoint

class Workspace(object):

    def __init__(self, workspace_id = None, authorization_token = None, endpoint=None):
        """
        Initialize a workspace.

        Parameters
        ----------
        workspace_id : str
            Unique identifier for the existing workspace. Can be obtained from
            the URL in ML Studio when editing a workspace.
        authorization_token: str
            Access token for the workspace. Can be the primary or secondary
            token managed in ML Studio.
        endpoint: str
            URL of the endpoint to connect to. Specify this only if you host
            ML Studio on your own server(s).

        Parameters that are omitted will be read from ~/.azureml/settings.ini:
        [workspace]
        id = abcd1234
        authorization_token = abcd1234
        endpoint = https://studio.azureml.net
        """
        workspace_id, authorization_token, endpoint, management_endpoint = _get_workspace_info(workspace_id, authorization_token, endpoint, None)

        _not_none_or_empty('workspace_id', workspace_id)
        _not_none_or_empty('authorization_token', authorization_token)
        _not_none_or_empty('endpoint', endpoint)

        self.workspace_id = workspace_id
        self.authorization_token = authorization_token
        self.api_endpoint = endpoint
        self.management_endpoint = management_endpoint
        self._rest = _RestClient(endpoint, authorization_token)
        self.datasets = Datasets(workspace=self)
        self.user_datasets = Datasets(workspace=self, example_filter=False)
        self.example_datasets = Datasets(workspace=self, example_filter=True)
        self.experiments = Experiments(workspace=self)
        self.user_experiments = Experiments(workspace=self, example_filter=False)
        self.example_experiments = Experiments(workspace=self, example_filter=True)


_manglingPattern = re.compile(r'[\W_]+')

def _mangled(name):
    result = _manglingPattern.sub('_', name)
    return result.lower()
