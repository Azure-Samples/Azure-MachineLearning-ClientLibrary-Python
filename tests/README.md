Test settings
=============

To successfully run tests, you'll need to create an **azuremltestsettings.json** file in this folder.

This file contains credentials and lists various Azure resources to use when running the tests.


Example
-------

```
{
    "workspace": {
        "id": "11111111111111111111111111111111",
        "token": "00000000000000000000000000000000",
        "endpoint": "https://studio.azureml.net"
    },
    "storage": {
        "accountName": "mystorageaccount",
        "accountKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==",
        "container": "mydatasettestcontainer",
        "mediumSizeBlob": "MediumSizeDataset_NH.csv",
        "unicodeBomBlob": "DatasetWithUnicodeBOM.txt",
        "blobs": [
            "Dataset_NH.csv",
            "Dataset_NH.tsv",
            "Dataset_WH.csv",
            "Dataset_WH.tsv",
            "Dataset.txt"
        ]
    },
    "intermediateDataset": {
        "experimentId": "11111111111111111111111111111111.f-id.22222222222222222222222222222222",
        "nodeId": "33333333-3333-3333-3333-333333333333-333",
        "portName":  "Results dataset",
        "dataTypeId":  "GenericCSV"
    },
    "diagnostics": {
        "writeBlobContents": "True",
        "writeSerializedFrame":  "True"
    }
}
```


Workspace
---------

From the Azure portal, create a new ML workspace. Open the new workspace in Studio. From the URL, you'll find your workspace id.

In the settings page, you'll find 2 authorization tokens, you can use either one.

Set the id and token in the json:

```
    "workspace": {
        "id": "11111111111111111111111111111111",
        "token": "00000000000000000000000000000000",
        "endpoint": "https://studio.azureml.net"
    },
```


Storage account
---------------

The storage section is used for some tests that load dataset files from Azure blob storage.

You'll need to create an Azure storage account, create a container and upload dataset files to it.

The round-trip tests rely on a naming convention for the ones in the blobs array:
```
        "blobs": [
            "Dataset_NH.csv",
            "Dataset_NH.tsv",
            "Dataset_WH.csv",
            "Dataset_WH.tsv",
            "Dataset.txt"
        ]
```

NH means no header, WH means with header.


Experiment
----------

Create a new experiment.  Add the following modules and connect them:

- Airport Codes Dataset
- Split
- Convert to CSV

Play the experiment and save.

You'll need the experiment id (appears in URL), the node id (can be found in the HTML DOM), the port name (displayed as a tooltip when you hover on the output port) and the data type id.

```
    "intermediateDataset": {
        "experimentId": "11111111111111111111111111111111.f-id.22222222222222222222222222222222",
        "nodeId": "33333333-3333-3333-3333-333333333333-333",
        "portName":  "Results dataset",
        "dataTypeId":  "GenericCSV"
    },
```


Diagnostics
-----------

Some of the tests can write intermediate results to disk, which can help with debugging.

    "diagnostics": {
        "writeBlobContents": "True",
        "writeSerializedFrame":  "True"
    }
