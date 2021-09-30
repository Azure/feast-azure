# Getting started with Feast on Azure

In this tutorial you will:

1. Create a feature store with SQL DB (serverless)
1. Register features into a central feature registry hosted on Blob Storage
1. Consume features from the feature store to train a model
1. Materialize features to an Azure cache for redis (online store) for inference
1. Create a real-time endpoint that consumes features from the online store.

## Prerequisites

For this tutorial you will require:

1. An Azure subscription
1. An Azure ML Workspace
1. Good working knowledge of Python and ML concepts.
1. Basic understanding of Azure ML - using notebooks, job submission, etc.

## Architecture

Below overlays the Azure components used in this tutorial.

![feast architecture](media/arch.png)

## Set up your environment

### Create compute instance

From the [Azure ML studio home page](https://ml.azure.com), select __New__ > __Compute instance__. Follow the on-screen instructions to create a compute instance.

You should give the compute instance a name and choose a size (Standard_DS3_v2 is a good choice). The remaining choices you can leave as the default settings.

In your compute instance, open a terminal, git clone this repo and then:

```bash
cd feast-azure-provider
pip install code/.
conda create -n feast-tutorial python=3.8
conda activate feast-tutorial
conda install ipykernel
pip install -r requirements.txt
python -m ipykernel install --user --name newenv --display-name "Python (feast-tutorial)"
```

### Login to the Azure CLI

The features will be stored in a __serverless__ SQL DB. This is to conserve cost while you learn feast. For additional scale you can swap SQL DB for a Synapse Dedicated SQL Pool (formerly SQL DW).

In your compute instance, open a terminal and login to azure using the following command

```bash
az login
```

Set your resource group name and Azure location (e.g. westeurope). These will be used in further steps below.

```bash
# Set the resource group name and location for your server
resourceGroupName=myResourceGroup
location=eastus
```

### Create a SQL DB (Offline Store)


```bash
# Set an admin login and password for your database
adminlogin=azureuser
password=Azure1234567!

# Set a server name that is unique to Azure DNS
serverName=server-$RANDOM
echo "Variable serverName is set to $serverName"

# a databasename
sqldbName=fsoffline-$RANDOM
echo "Variable sqldbName is set to $sqldbName"

# create resource group
az group create --name $resourceGroupName --location $location

az sql server create \
    --name $serverName \
    --resource-group $resourceGroupName \
    --location $location  \
    --admin-user $adminlogin \
    --admin-password $password

# Create a firewall rule that allows access from Azure services
az sql server firewall-rule create \
    -g $resourceGroupName \
    -s $serverName \
    -n myrule \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0

az sql db create \
    --resource-group $resourceGroupName \
    --server $serverName \
    --name $sqldbName \
    --edition GeneralPurpose \
    --compute-model Serverless \
    --family Gen5 \
    --capacity 2
```

### Create Azure Cache for Redis (online store)

For accessing features for real-time inference we use an Azure Cache for Redis. In this tutorial we use a basic instance to conserve cost.

```bash
# a redis name
redisName=fsonline-$RANDOM
echo "Variable redisName is set to $redisName"

az redis create \
    --location $location \
    --name $redisName \
    --resource-group $resourceGroupName \
    --sku Basic \
    --vm-size c1 \
```

### Blob Account and container to host registry.db file

You need a blob account to host the registry.db file - this stores the feature meta data. Use the following:

```bash
# a blob account name
storageAccountName=fsblob-$RANDOM
echo "Variable storageAccountName is set to $storageAccountName"

az storage account create \
    --name $storageAccountName \
    --resource-group $resourceGroupName \
    --location $location \
    --sku Standard_ZRS \
    --encryption-services blob
```

Next create the container and assign _Storage Blob Data Contributor_ role to your Azure login:

```bash
# a container name
containerName=feast-registry

subId=$(az account show --query id)

az ad signed-in-user show --query objectId -o tsv | az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee @- \
    --scope "/subscriptions/$subId/resourceGroups/$resourceGroupName/providers/Microsoft.Storage/storageAccounts/$storageAccountName"

az storage container create \
    --account-name $storageAccountName \
    --name $containerName \
    --auth-mode login
```

### Add your connection strings to the Azure ML keyvault

With all the infrastructure set up, in the same terminal session as above you can set secrets in your Azure ML keyvault using the utility python script in this repo.

```bash
FEAST_SQL_CONN="mssql+pyodbc://$adminlogin:$password@$serverName.database.windows.net:1433/$sqldbName?driver=ODBC+Driver+17+for+SQL+Server&autocommit=True"

# get redis password
redisKey=$(sed -e 's/^"//' -e 's/"$//' <<<$(az redis list-keys --name $redisName -g $resourceGroupName --query primaryKey))

FEAST_REDIS_CONN=$redisName.redis.cache.windows.net:6380,password=$redisKey,ssl=True

cd setup
python set_secrets.py --feast_sql_conn $FEAST_SQL_CONN --feast_redis_conn $FEAST_REDIS_CONN
```

### Load features into Feature Store

Feast does not handle transformations nor loading of data into your feature store - this occurs outside of feast. You can use ADF, Azure ML Pipelines, SQL, Azure Functions, etc to manage the orchestration of feature creation and loading.

For this tutorial, we will create and load features into the store using a python script. 

In your compute instance, open a terminal and run the following:

```bash
conda activate feast-tutorial
cd setup
python load_data.py
```

## Register features in Feature store

Follow the `notebooks/register_features` notebook to register features into the central feast registry.db file.

## Train and Deploy a model using the Feature Store

Follow the `notebooks/train_and_deploy_with_feast` notebook to understand how to train and deploy a model using feast.

