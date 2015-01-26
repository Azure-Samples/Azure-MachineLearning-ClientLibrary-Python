Microsoft Azure Machine Learning Data Access SDK for Python
===========================================================

The Machine Learning Data Access SDK for Python lets you access your Azure ML datasets from your local Python environment.

You can download datasets that are available in your ML workspace, or intermediate datasets from experiments that were run. You can upload new datasets and update existing datasets. The data is optionally converted to/from a Pandas DataFrame.


Installation
============

The SDK has been tested with Python 2.7, 3.3 and 3.4.

It has a dependency on the following packages:

- requests
- python-dateutil
- pandas


You can install it from [PyPI](https://pypi.python.org/pypi/azureml):

```
pip install azureml
```


Usage
=====

Note: We recommend that you use the **Generate Data Access Code** feature from [Azure Machine Learning Studio](https://studio.azureml.net) in order to get Python code snippets that give you access to your datasets. The code snippets include your workspace id, authorization token, and other necessary identifiers to get to your datasets.

Accessing your workspace
------------------------

You'll need to obtain your workspace id and token in order to get access to your workspace.

```python
from azureml import Workspace

ws = Workspace(workspace_id='4c29e1adeba2e5a7cbeb0e4f4adfb4df',
               authorization_token='f4f3ade2c6aefdb1afb043cd8bcf3daf')
```


Accessing datasets
------------------

To enumerate all datasets in a given workspace:

```python
for ds in ws.datasets:
    print(ds.name)
```

Just the user-created datasets:

```python
for ds in ws.user_datasets:
    print(ds.name)
```

Just the example datasets:

```python
for ds in ws.example_datasets:
    print(ds.name)
```

You can access a dataset by name (which is case-sensitive):

```python
ds = ws.datasets['my dataset name']
```

By index:

```python
ds = ws.datasets[0]
```


Dataset metadata
----------------

Every dataset has metadata in addition to its content.

Some metadata values are assigned by the user at creation time:

```python
print(ds.name)
print(ds.description)
print(ds.family_id)
print(ds.data_type_id)
```

Others are values assigned by Azure ML:

```python
print(ds.id)
print(ds.created_date)
print(ds.size)
```

See the `SourceDataset` class for more on the available metadata.


Reading contents
----------------

You can import the dataset contents as a pandas DataFrame object.
The `data_type_id` metadata on the dataset is used to determine how to import the contents.

```python
frame = ds.to_dataframe()
```

If a dataset is in a format that cannot be deserialized to a pandas DataFrame, the dataset object will not have a to_dataframe method.

You can still read those datasets as text or binary, then parse the data manually.

Read the contents as text:

```python
text_data = ds.read_as_text()
```

Read the contents as binary:

```python
binary_data = ds.read_as_binary()
```

You can also just open a stream to the contents:

```python
with ds.open() as file:
    binary_data_chunk = file.read(1000)
```

This gives you more control over the memory usage, as you can read and parse the data in chunks.


Accessing intermediate datasets
-------------------------------

You can access the intermediate datasets at the output ports of the nodes in your experiments.

Note that the default binary serialization format (.dataset) for intermediate datasets is not supported. Make sure to use a Convert to TSV or Convert to CSV module and read the intermediate dataset from its output port.

First, get the experiment, using the experiment id:

```python
experiment = ws.experiments['my experiment id']
```

Then get the intermediate dataset object:

```python
ds = experiment.get_intermediate_dataset(
    node_id='5c457225-68e3-4b60-9e3a-bc55f9f029a4-565',
    port_name='Results dataset',
    data_type_id=DataTypeIds.GenericCSV
)
```

To determine the values to pass to `get_intermediate_dataset`, use the **Generate Data Access Code** command on the module output port in ML Studio.

You can then read the intermediate dataset contents just like you do for a regular dataset:

```python
frame = ds.to_dataframe()
```

You can also use `open`, `read_as_text` and `read_as_binary`.

Note that intermediate datasets do not have any metadata available.


Creating a new dataset
----------------------

After you've manipulated the data, you can upload it as a new dataset on Azure ML.

This will serialize the pandas DataFrame object to the format specified in the
`data_type_id` parameter, then upload it to Azure ML.

```python
dataset = workspace.datasets.add_from_dataframe(
    dataframe=frame,
    data_type_id=DataTypeIds.GenericCSV,
    name='my new dataset',
    description='my description'
)
```

If you want to serialize the data yourself, you can upload the raw data. Note
that you still have to indicate the format of the data.

```python
raw_data = my_own_csv_serialization_function(frame)
dataset = workspace.datasets.add_from_raw_data(
    raw_data=raw_data,
    data_type_id=DataTypeIds.GenericCSV,
    name='my new dataset',
    description='my description'
)
```

After it's added, it's immediately accessible from the datasets collection.

If you attempt to create a new dataset with a name that matches an existing dataset, an AzureMLConflictHttpError will be raised.

```python
from azureml import AzureMLConflictHttpError

try:
    workspace.datasets.add_from_dataframe(
        dataframe=frame,
        data_type_id=DataTypeIds.GenericCSV,
        name='not a unique name',
        description='my description'
    )
except AzureMLConflictHttpError:
    print('Try again with a unique name!')
```

To update an existing dataset, you can use `update_from_dataframe` or `update_from_raw_data`:

```python
name = 'my existing dataset'
dataset = workspace.datasets[name]

dataset.update_from_dataframe(dataframe=frame)
```

You can optionally change the name, description or the format of the data too:

```python
name = 'my existing dataset'
dataset = workspace.datasets[name]

dataset.update_from_dataframe(
    dataframe=frame,
    data_type_id=DataTypeIds.GenericCSV,
    name='my new name',
    description='my new description'
)
```

If you attempt to create a new dataset with an invalid name, or if Azure ML rejects the dataset for any other reason, an AzureMLHttpError will be raised. AzureMLHttpError is raised when the http status code indicates a failure. A detailed error message can displayed by printing the exception, and the HTTP status code is stored in the `status_code` field.

```python
from azureml import AzureMLHttpError

try:
    workspace.datasets.add_from_dataframe(
        dataframe=frame,
        data_type_id=DataTypeIds.GenericCSV,
        name='invalid:name',
        description='my description'
    )
except AzureMLHttpError as error:
    print(error.status_code)
    print(error)
```
