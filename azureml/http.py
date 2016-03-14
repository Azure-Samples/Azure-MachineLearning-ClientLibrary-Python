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

import json
import requests
from azureml.errors import AzureMLConflictHttpError

try:
    from urlparse import urljoin
except ImportError:
    from urllib.parse import urljoin

from azureml.errors import (
    AzureMLHttpError,
)

__author__ = 'Microsoft Corp. <ptvshelp@microsoft.com>'
__version__ = '0.2.7'


class _RestClient(object):
    SERVICE_ROOT = 'api/'
    INTERMEDIATE_DATASET_URI_FMT = SERVICE_ROOT + 'workspaces/{0}/experiments/{1}/outputdata/{2}/{3}'
    EXPERIMENTS_URI_FMT = SERVICE_ROOT + 'workspaces/{0}/experiments'
    DATASOURCES_URI_FMT = SERVICE_ROOT + 'workspaces/{0}/datasources'
    DATASOURCE_URI_FMT = SERVICE_ROOT + 'workspaces/{0}/datasources/{1}'
    UPLOAD_URI_FMI = SERVICE_ROOT + 'resourceuploads/workspaces/{0}/?userStorage=true&dataTypeId={1}'
    UPLOAD_CHUNK_URI_FMT = SERVICE_ROOT + 'blobuploads/workspaces/{0}/?numberOfBlocks={1}&blockId={2}&uploadId={3}&dataTypeId={4}'
    SESSION_ID_HEADER_NAME = 'x-ms-client-session-id'
    SESSION_ID_HEADER_VALUE = 'DefaultSession'
    ACCESS_TOKEN_HEADER_NAME = 'x-ms-metaanalytics-authorizationtoken'
    CONTENT_TYPE_HEADER_NAME = 'Content-Type'
    CONTENT_TYPE_HEADER_VALUE_JSON = 'application/json;charset=UTF8'
    CHUNK_SIZE = 0x200000 
    DEFAULT_OWNER = 'Python SDK'
    USER_AGENT_HEADER_NAME = 'User-Agent'
    USER_AGENT_HEADER_VALUE = 'pyazureml/' + __version__

    def __init__(self, service_endpoint, access_token):
        self._service_endpoint = service_endpoint
        self._access_token = access_token

    def get_experiments(self, workspace_id):
        """Runs HTTP GET request to retrieve the list of experiments."""
        api_path = self.EXPERIMENTS_URI_FMT.format(workspace_id)
        return self._send_get_req(api_path)

    def get_datasets(self, workspace_id):
        """Runs HTTP GET request to retrieve the list of datasets."""
        api_path = self.DATASOURCES_URI_FMT.format(workspace_id)
        return self._send_get_req(api_path)

    def get_dataset(self, workspace_id, dataset_id):
        """Runs HTTP GET request to retrieve a single dataset."""
        api_path = self.DATASOURCE_URI_FMT.format(workspace_id, dataset_id)
        return self._send_get_req(api_path)

    def open_intermediate_dataset_contents(self, workspace_id, experiment_id,
                                           node_id, port_name):
        return self._get_intermediate_dataset_contents(
            workspace_id,
            experiment_id,
            node_id,
            port_name,
            stream=True).raw

    def read_intermediate_dataset_contents_binary(self, workspace_id,
                                                  experiment_id, node_id,
                                                  port_name):
        return self._get_intermediate_dataset_contents(
            workspace_id,
            experiment_id,
            node_id,
            port_name,
            stream=False).content

    def read_intermediate_dataset_contents_text(self, workspace_id,
                                                experiment_id, node_id,
                                                port_name):
        return self._get_intermediate_dataset_contents(
            workspace_id,
            experiment_id,
            node_id,
            port_name,
            stream=False).text

    def _get_intermediate_dataset_contents(self, workspace_id, experiment_id,
                                           node_id, port_name, stream):
        api_path = self.INTERMEDIATE_DATASET_URI_FMT.format(
            workspace_id, experiment_id, node_id, port_name)
        response = requests.get(
            url=urljoin(self._service_endpoint, api_path),
            headers=self._get_headers(),
            stream=stream,
        )
        return response

    def open_dataset_contents(self, url):
        response = requests.get(url, stream=True)
        return response.raw

    def read_dataset_contents_binary(self, url):
        response = requests.get(url)
        return response.content

    def read_dataset_contents_text(self, url):
        response = requests.get(url)
        return response.text

    def upload_dataset(self, workspace_id, name, description, data_type_id,
                       raw_data, family_id):
        # uploading data is a two step process. First we upload the raw data
        api_path = self.UPLOAD_URI_FMI.format(workspace_id, data_type_id)
        upload_result = self._send_post_req(api_path, data=b'')

        # now get the id that was generated
        upload_id = upload_result["Id"]

        # Upload the data in chunks...
        total_chunks = int((len(raw_data) + (self.CHUNK_SIZE-1)) / self.CHUNK_SIZE)
        for chunk in range(total_chunks):
            chunk_url = self.UPLOAD_CHUNK_URI_FMT.format(
                workspace_id,
                total_chunks, # number of blocks
                chunk, # block id
                upload_id,
                data_type_id,
            )
            chunk_data = raw_data[chunk*self.CHUNK_SIZE:(chunk + 1)*self.CHUNK_SIZE]
            self._send_post_req(chunk_url, data=chunk_data)

        # use that to construct the DataSource metadata
        metadata = {
            "DataSource": {
                "Name": name,
                "DataTypeId":data_type_id,
                "Description":description,
                "FamilyId":family_id,
                "Owner": self.DEFAULT_OWNER,
                "SourceOrigin":"FromResourceUpload"
            },
            "UploadId": upload_id,
            "UploadedFromFileName":"",
            "ClientPoll": True
        }

        try:
            api_path = self.DATASOURCES_URI_FMT.format(workspace_id)
        except AzureMLConflictHttpError as e:
            raise AzureMLConflictHttpError(
                'A data set named "{}" already exists'.format(name), 
                e.status_code
            )

        datasource_id = self._send_post_req(
            api_path, json.dumps(metadata), self.CONTENT_TYPE_HEADER_VALUE_JSON)
        return datasource_id

    def _send_get_req(self, api_path):
        response = requests.get(
            url=urljoin(self._service_endpoint, api_path),
            headers=self._get_headers()
        )

        if response.status_code >= 400:
            raise AzureMLHttpError(response.text, response.status_code)

        return response.json()

    def _send_post_req(self, api_path, data, content_type=None):
        response = requests.post(
            url=urljoin(self._service_endpoint, api_path),
            data=data,
            headers=self._get_headers(content_type)
        )

        if response.status_code >= 400:
            raise AzureMLHttpError(response.text, response.status_code)

        return response.json()

    def _get_headers(self, content_type=None):
        headers = {
            self.USER_AGENT_HEADER_NAME: self.USER_AGENT_HEADER_VALUE,
            self.CONTENT_TYPE_HEADER_NAME: self.CONTENT_TYPE_HEADER_VALUE_JSON,
            self.SESSION_ID_HEADER_NAME: self.SESSION_ID_HEADER_VALUE,
            self.ACCESS_TOKEN_HEADER_NAME: self._access_token
        }
        if content_type:
            headers[self.CONTENT_TYPE_HEADER_NAME] = content_type
        return headers
