# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import argparse
from azureml.core import Workspace

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--feast_sql_conn", type=str)
    parser.add_argument("--feast_redis_conn", type=str)
    args = parser.parse_args()

    sql_conn = args.feast_sql_conn
    redis_conn = args.feast_redis_conn

    ws = Workspace.from_config()
    kv = ws.get_default_keyvault()

    print("setting secrets")
    kv.set_secret("FEAST-SQL-CONN", sql_conn)
    kv.set_secret("FEAST-REDIS-CONN", redis_conn)

    print("done!")
