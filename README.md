Microsoft Azure Machine Learning Python client library for Azure ML Studio
==========================================================================

> **NOTE** This content is no longer maintained. Visit the [Azure Machine Learning Notebook](https://github.com/Azure/MachineLearningNotebooks) project for sample Jupyter notebooks for ML and deep learning with Azure Machine Learning using the Python SDK.

The preview of Azure Machine Learning Python client library lets you access your Azure ML Studio datasets from your local Python environment.

You can download datasets that are available in your ML Studio workspace, or intermediate datasets from experiments that were run. You can upload new datasets and update existing datasets. The data is optionally converted to/from a Pandas DataFrame.

This is a technology preview. The APIs exposed by the library and the REST endpoints it connects to are subject to change.


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

If you're using AzureML in a region other than South Central US you'll also need to specify the endpoint:

```python
from azureml import Workspace

ws = Workspace(workspace_id='4c29e1adeba2e5a7cbeb0e4f4adfb4df',
               authorization_token='f4f3ade2c6aefdb1afb043cd8bcf3daf',
               endpoint='https://europewest.studio.azureml.net/')
```

Specify workspace via config
----------------------------
If you don't want to store your access tokens in code you can also put them in a configuration file.  The SDK will look for ~/.azureml/settings.ini and if available use that:

```
[workspace]
id=4c29e1adeba2e5a7cbeb0e4f4adfb4df
authorization_token=f4f3ade2c6aefdb1afb043cd8bcf3daf
api_endpoint=https://studio.azureml.net
management_endpoint=https://management.azureml.net
```

And then the workspace can be created without arguments:

```python
from azureml import Workspace

ws = Workspace()
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

Services Usage
==============
The services subpackage allows you to easily publish and consume AzureML Web Services.  Currently only Python 2.7 is supported for services because the back end only has Python 2.7 installed.

Publishing
----------

Python functions can either be published using the @publish decorator or by calling the publish method directly.  To publish a function using the decorator you can do:

```python
from azureml import services

@services.publish(workspace, workspace_token)
@services.types(a = float, b = float)
@services.returns(float)
def func(a, b):
    return a / b
```

This publishes a function which takes two floating point values and divides them.  Alternately you can publish a function by calling the publish method directly:

```python
my_func = publish(my_func, workspace, workspace_token, files_list, endpoint=None)
```

If a function has no source file associated with it (for example, you're developing inside of a REPL environment) then the functions byte code is serialized.  If the function refers to any global variables those will also be serialized using Pickle.  In this mode all of the state which you're referring to needs to be already defined (e.g. your published function should come after any other functions you are calling).

If a function is saved on disk then the entire module the function is defined in will be serialized and re-executed on the server to get the function back.  In this mode the entire contents of the file is serialized and the order of the function definitions don't matter.

After the function is published there will be a "service" property on the function.  This object has several properties of interest:

| Property      | Description   |
| ------------- |:-------------:| 
| url           | this is the end point for executing the function |
| api_key       | this is the API key which is required to invoke the function |
| help_url      | this is a human readable page which describes the parameters and results of the function.  It also includes sample code for executing it from various languages. |
| service_id    | this is a unique GUID identifying the service in your workspace.  You can re-use this ID to update the service once it's published |
 
You can specify a list of files which should be published along with the function.
The resulting files will be stored in a subdirectory called 'Script Bundle'.  The
list of files can be one of:

| Format                                     |  Description                                                    |
| ------------------------------------------ |:---------------------------------------------------------------:| 
| (('file1.txt', None), )                    | file is read from disk                                          |
| (('file1.txt', b'contents'), )             | file contents are provided                                      |
| ('file1.txt', 'file2.txt')                 | files are read from disk, written with same filename            |
| ((('file1.txt', 'destname.txt'), None), )  | file is read from disk, written with different destination name filenames.  |

 
 The various formats for each filename can be freely mixed and matched.  Files can also be attached using the @attach decoator:
 
 ```python
 @publish(...)
 @attach('file1.txt')
 def f(x):
     pass
```

 And this supports the same file formats as the list.
 
If you are using AzureML from a different geography (for example West Europe or East Asia) you'll need to specify the endpoint that you need to connect to.  The end point is your region plus "management.azureml.net", for example: https://europewest.management.azureml.net
 
Consumption
-----------

Existing services can be consumed using the service decorator.  An empty function body is supplied and the resulting function becomes invokable and calls the published service:

```python
from azureml import services

@services.service(url, api_key)
@services.types(a = float, b = float)
@services.returns(float)
def func(a, b):
    pass
```

Controlling publishing / consumption
------------------------------------

There are several decorators which are used to control how the invocation occurs.  

### types(**kwargs)
Specifies the types used for the arguments of a published or consumed service.  

The type annotations are optional and are used for providing information which allows the service to interoperate with other languages.  The type information will be seen on the help page of the published service.  If the type information is not provided a Python specific format will be used and other languages may not be able to call the sevice.

Supported types are: int, bool, float, unicode.  

When an unsupported type is specified the type will be serialized using an internal representation based upon Python's Pickle protocol.  This will prevent the web service from being used with other languages.

When working with strings you need to use the unicode data type.  This is because the string data type used for interop is actually a Unicode string and Python's "str" objects are actually byte arrays.

For 

### returns(return_type)
Specifies the return type for a published service.

Like the parameter types this is also optional, and when omitted an internal Python format will be used and interoperability with other languages may be reduced.

Supported types are: int, bool, float, unicode.  

When an unsupported type is specified the type will be serialized using an internal representation based upon Python's Pickle protocol.  This will prevent the web service from being used with other languages.

When working with strings you need to use the unicode data type.  This is because the string data type used for interop is actually a Unicode string and Python's "str" objects are actually byte arrays.

### service_id(id)
Specifies the service ID for a service.  When publishing to the same service ID the service is updated instead of having a new service created.

### name(name)
Specifies a friendly name for a service.  By default the name is the function name, but this allows names with spaces or
other characters which are not allowed in functions.

### attach(name, contents)
Attaches a file to the payload to be uploaded.

If contents is omitted the file is read from disk.
If name is a tuple it specifies the on-disk filename and the destination filename.

### dataframe_service
Indicates that the function operations on a data frame.  The function 
will receive a single input in the form of a data frame, and should return 
a data frame object.  The schema of the data frame is specified with this 
decorator.

```python
@publish(...)
@dataframe_service(a = int, b = int)
@returns(int)
def myfunc(df):
    return pandas.DataFrame([df['a'][i] + df['b'][i] for i in range(df.shape[0])])
```

This code can then be invoked either with:
```python
myfunc(1, 2)
```

or:

```python
myfunc.map([[1,2], [3,4]])
```

### input_name
Specifies the name of the input the web service expects to receive.  Defaults to 'input1'  Currently this is only
supported on consumption.

### output_name
Specifies the name of the output the web service expects to receive.  Defaults to 'output1'. Currently this is only
supported on consumption.

Those include the types decorator for specifying the format of the inputs, the returns decorator for specifying the return value, the attach decorator for attaching files to a published function, 
