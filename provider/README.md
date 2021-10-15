# Feast Azure Provider

This goal of this project is to create an Azure provider for [Feast](http://feast.dev), which is an open source feature store. The Feast Azure provider acts like a plugin that allows Feast users to connect to:

- Azure SQL DB and/or Synapse SQL as the _offline store_
- Azure cache for Redis as the _online store_
- Azure blob storage for the feast _registry store_

## 📐 Architecture

The _interoperable_ design of feast means that many Azure services can be used to _produce_ and/or _consume_ features (for example: Azure ML, Synapse, Azure Databricks, Azure functions, etc).

![azure provider architecture](media/arch.png)

## 🐱‍👤 Getting Started

### Pre-requisites

You will need to have:

- An Azure account with an active subscription. 
    - Don't have an Account? [You can create an Azure account with $200 free credit](https://azure.microsoft.com/free/).
- Either an Azure SQL DB or Synapse SQL
- A provisioned Azure Cache for Redis
- An Azure Storage Account

### 1. Install Feast Azure Provider
Install the provider using `pip` (we recommend using either conda or virtualenv for environment isolation):

```bash
pip install feast-azure-provider
```

> **Note**
There is a dependency on the `unixodbc` operating system package. You can install this on Debian/Ubuntu using `sudo apt-get install unixodbc-dev`
 
### 2. Create a feature repository

```bash
feast init -m my_feature_repo
cd my_feature_repo
```

Rather than store credentials in your `feature_store.yaml` file, we recommend using environment variables:

```bash
export SQL_CONN='mssql+pyodbc://<USER_NAME>:<PASSWORD>@<SERVER_NAME>.database.windows.net:1433/<DB_NAME>?driver=ODBC+Driver+17+for+SQL+Server&autocommit=True'

export REDIS_CONN='<CACHE_NAME>.redis.cache.windows.net:6380,password=<PASSWORD>,ssl=True'
```

Update the `feature_store.yaml`:

```yaml
project: production
registry: 
    registry_store_type: feast_azure_provider.registry_store.AzBlobRegistryStore
    path: https://<ACCOUNT_NAME>.blob.core.windows.net/<CONTAINER>/<PATH>/registry.db
provider: feast_azure_provider.azure_provider.AzureProvider
offline_store:
    type: feast_azure_provider.mssqlserver.MsSqlServerOfflineStore
    connection_string: ${SQL_CONN}
online_store:
    type: redis
    connection_string: ${REDIS_CONN}
```

### 3. Register your feature definitions and set up your feature store

```bash
feast apply
```

### 4. Build a training dataset

```python
from feast import FeatureStore
import pandas as pd
from datetime import datetime

entity_df = pd.DataFrame.from_dict({
    "entity_id": [], # list of entities e.g. customer_id
    "event_timestamp": [
        datetime(2021, 4, 12, 10, 59, 42),
        # list of datetimes
    ]
})

store = FeatureStore(repo_path=".")

training_df = store.get_historical_features(
    entity_df=entity_df,
    features = [
        'featureview1:feature1',
        'featureview1:feature2',
        'featureview2:feature1'
    ],
).to_df()

print(training_df.head())

# Train model
model = ml.fit(training_df)
```

### 5. Load feature values into your online store

```bash
CURRENT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S")
feast materialize-incremental $CURRENT_TIME
```

### 6. Read online features at low latency

```python
from pprint import pprint
from feast import FeatureStore

store = FeatureStore(repo_path=".")

feature_vector = store.get_online_features(
    features=[
        'featureview1:feature1',
        'featureview1:feature2',
        'featureview2:feature1'
    ],
    entity_rows=[{"entity_id": 1001}] # for example: driver_id, customer_id
).to_dict()

pprint(feature_vector)

# Make prediction
# model.predict(feature_vector)
```


## 🎓 Learn more

- [Feast website](http://feast.dev)
- [Feast on Azure tutorial](./tutorial/README.md)

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
