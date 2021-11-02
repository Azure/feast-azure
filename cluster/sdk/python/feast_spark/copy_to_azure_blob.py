# coding: utf-8

# -------------------------------------------------------------------------
# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License. See License.txt in the project root for
# license information.
# --------------------------------------------------------------------------

"""
FILE: blob_samples_copy_blob.py
DESCRIPTION:
    This sample demos how to copy a blob from a URL.
USAGE: python blob_samples_copy_blob.py
    Set the environment variables with your own values before running the sample.
    1) AZURE_STORAGE_CONNECTION_STRING - the connection string to your storage account
"""

from __future__ import print_function
import os
import sys
import time
from azure.storage.blob import BlobServiceClient

def main():
    try:
        CONNECTION_STRING = os.environ['AZURE_STORAGE_CONNECTION_STRING']

    except KeyError:
        print("AZURE_STORAGE_CONNECTION_STRING must be set.")
        sys.exit(1)

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    copied_blob = blob_service_client.get_blob_client("feastjar", 'feast-ingestion-spark-latest.jar')
    # hard code to the current path
    SOURCE_FILE = "./feast-ingestion-spark-latest.jar"

    with open(SOURCE_FILE, "rb") as data:
            copied_blob.upload_blob(data, blob_type="BlockBlob",overwrite=True)
    
if __name__ == "__main__":
    main()
