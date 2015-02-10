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

from functools import partial
import codecs
import pandas as pd

from azureml.errors import (
    UnsupportedDatasetTypeError,
    _not_none,
    _not_none_or_empty,
)


class DataTypeIds(object):
    """Constants for the known dataset data type id strings."""
    ARFF = 'ARFF'
    PlainText = 'PlainText'
    GenericCSV = 'GenericCSV'
    GenericTSV = 'GenericTSV'
    GenericCSVNoHeader = 'GenericCSVNoHeader'
    GenericTSVNoHeader = 'GenericTSVNoHeader'


def _dataframe_to_csv(writer, dataframe, delimiter, with_header):
    """serialize the dataframe with different delimiters"""
    encoding_writer = codecs.getwriter('utf-8')(writer)
    dataframe.to_csv(
        path_or_buf=encoding_writer,
        sep=delimiter,
        header=with_header,
        index=False
    )

def _dataframe_to_txt(writer, dataframe):
    encoding_writer = codecs.getwriter('utf-8')(writer)
    for row in dataframe.iterrows():
        encoding_writer.write("".join(row[1].tolist()))
        encoding_writer.write('\n')

def _dataframe_from_csv(reader, delimiter, with_header, skipspace):
    """Returns csv data as a pandas Dataframe object"""
    sep = delimiter
    header = 0
    if not with_header:
        header = None

    return pd.read_csv(
        reader,
        header=header,
        sep=sep,
        skipinitialspace=skipspace,
        encoding='utf-8-sig'
    )

def _dataframe_from_txt(reader):
    """Returns PlainText data as a pandas Dataframe object"""
    return pd.read_csv(reader, header=None, sep="\n", encoding='utf-8-sig')


_SERIALIZERS = {
    DataTypeIds.PlainText: (
        _dataframe_to_txt,
        _dataframe_from_txt,
    ),
    DataTypeIds.GenericCSV: (
        partial(_dataframe_to_csv, delimiter=',', with_header=True),
        partial(_dataframe_from_csv, delimiter=',', with_header=True, skipspace=True),
    ),
    DataTypeIds.GenericCSVNoHeader: (
        partial(_dataframe_to_csv, delimiter=',', with_header=False),
        partial(_dataframe_from_csv, delimiter=',', with_header=False, skipspace=True),
    ),
    DataTypeIds.GenericTSV: (
        partial(_dataframe_to_csv, delimiter='\t', with_header=True),
        partial(_dataframe_from_csv, delimiter='\t', with_header=True, skipspace=False),
    ),
    DataTypeIds.GenericTSVNoHeader: (
        partial(_dataframe_to_csv, delimiter='\t', with_header=False),
        partial(_dataframe_from_csv, delimiter='\t', with_header=False, skipspace=False),
    ),
}


def serialize_dataframe(writer, data_type_id, dataframe):
    """
    Serialize a dataframe.

    Parameters
    ----------
    writer : file
        File-like object to write to. Must be opened in binary mode.
    data_type_id : dict
        Serialization format to use.
        See the azureml.DataTypeIds class for constants.
    dataframe: pandas.DataFrame
        Dataframe to serialize.
    """
    _not_none('writer', writer)
    _not_none_or_empty('data_type_id', data_type_id)
    _not_none('dataframe', dataframe)

    serializer = _SERIALIZERS.get(data_type_id)
    if serializer is None:
        raise UnsupportedDatasetTypeError(data_type_id)
    serializer[0](writer=writer, dataframe=dataframe)

def deserialize_dataframe(reader, data_type_id):
    """
    Deserialize a dataframe.

    Parameters
    ----------
    reader : file
        File-like object to read from. Must be opened in binary mode.
    data_type_id : dict
        Serialization format of the raw data.
        See the azureml.DataTypeIds class for constants.

    Returns
    -------
    pandas.DataFrame
        Dataframe object.
    """
    _not_none('reader', reader)
    _not_none_or_empty('data_type_id', data_type_id)

    serializer = _SERIALIZERS.get(data_type_id)
    if serializer is None:
        raise UnsupportedDatasetTypeError(data_type_id)
    return serializer[1](reader=reader)

def is_supported(data_type_id):
    """Return if a serializer is available for the specified format."""
    _not_none_or_empty('data_type_id', data_type_id)

    return _SERIALIZERS.get(data_type_id) is not None
