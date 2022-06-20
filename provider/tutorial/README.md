# Getting started with Feast on Azure

The objective of this tutorial is to build a model that predicts if a driver will complete a trip based on a number of features ingested into Feast. During this tutorial you will:

1. Deploy the infrastructure for a feature store (using an ARM template)
1. Register features into a central feature registry hosted on Blob Storage
1. Consume features from the feature store for training and inference

## Prerequisites

For this tutorial you will require:

1. An Azure subscription.
1. Working knowledge of Python and ML concepts.
1. Basic understanding of Azure Machine Learning - using notebooks, etc.

## 1. Deploy Infrastructure

We have created an ARM template that deploys and configures all the infrastructure required to run feast in Azure. This makes the set-up very simple - select the **Deploy to Azure** button below.

The only 2 required parameters during the set-up are:

- **Admin Password** for the the Dedicated SQL Pool being deployed.
- **Principal ID** this is to set the storage permissions for the feast registry store. You can find the value for this by opening **Cloud Shell** and run the following command:

```bash
az ad signed-in-user show --query id -o tsv
```

> You may want to first make sure your subscription has registered `Microsoft.Synapse`, `Microsoft.SQL` and `Microsoft.Network` providers before running the template below, as some of them may require explicit registration.

[![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FAzure%2Ffeast-azure%2Fmain%2Fprovider%2Fcloud%2Ffs_synapse_azuredeploy.json)

![feast architecture](media/arch.png)

The ARM template will not only deploy the infrastructure but it will also:

- install the feast azure provider on the compute instance
- set the Registry Blob path, Dedicated SQL Pool and Redis cache connection strings in the Azure ML default Keyvault.

> **☕ It can take up to 20 minutes for the Redis cache to be provisioned.**

## 2. Git clone this repo to your compute instance

In the [Azure Machine Learning Studio](https://ml.azure.com), navigate to the left-hand menu and select **Compute**. You should see your compute instance running, select **Terminal**

![compute instance terminal](./media/ci.png)

In the terminal you need to clone this GitHub repo:

```bash
git clone https://github.com/Azure/feast-azure
```

### 3. Load feature values into Feature Store

In the Azure ML Studio, select *Notebooks* from the left-hand menu and then open the [Loading feature values into feature store notebook](./notebooks/part1-load-data.ipynb).Work through this notebook.

> __💁Ensure the Jupyter kernel is set to Python 3.8 - AzureML__

![compute instance kernel](./media/ci-kernel.png)


## 4. Register features in Feature store

In the Azure ML Studio, select *Notebooks* from the left-hand menu and then open the [register features into your feature registry notebook](notebooks/part2-register-features.ipynb). Work through this notebook.

> __💁Ensure the Jupyter kernel is set to Python 3.8 - AzureML__

## 5.Train and Deploy a model using the Feature Store

In the Azure ML Studio, select *Notebooks* from the left-hand menu and then open the [train and deploy a model using feast notebook](notebooks/part3-train-and-deploy-with-feast.ipynb). Work through this notebook.

> __💁Ensure the Jupyter kernel is set to Python 3.8 - AzureML__
