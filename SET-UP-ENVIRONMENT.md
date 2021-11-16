# Set up Azure enviroment

This guide contains the following steps to set up your enviroment for all the demos in this repo.

- Set up Azure CLI
- Set up Databricks CLI
- Set up Databricks Secrets Scope
- Set up ADLS gen2
- Set up Event Hubs
- Set up Databricks Cluster

## Set up Azure CLI

Follow the instructions on [how to install the Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli).

### Define resource group for all Azure CLI cmds

```
az configure --defaults group=<your-resource-group>
```

## Set up Databricks CLI

Follow the instructions on [how to install the Azure Databricks CLI](https://docs.microsoft.com/en-us/azure/databricks/dev-tools/cli/).

### Authenticate against the Databricks CLI using PAT token method

Create a token in the Databricks Workspace and then run the following to authenticate.

```
databricks configure --token
```

### Check Databricls CLI configuration works

```
databricks workspace list
```

## Set up Databricks Secrets Scope

### Create secrets scope to be used for ADLS and Event Hubs access keys

Create a Databricks secrets scope called `access_creds`. If sharing a workspace the scope might already exist.  

```
databricks secrets create-scope --scope access_creds
```

## Set up ADLS gen2

### Define storage account name

*Note: Storage account name must be less than 24 characters*

```
export STORAGE_ACCOUNT=musicapp<storageaccountname>
```

### Create ADLS gen2 bucket

```
az storage account create \
  --name $STORAGE_ACCOUNT \
  --location northeurope \
  --sku Standard_RAGRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --allow-shared-key-access true
```

### Create container called data in storage account

```
az storage fs create -n data --account-name $STORAGE_ACCOUNT
```

### Add ADLS access key to Databricks secrets

```
export ADLS_SECRET_NAME=adls"${STORAGE_ACCOUNT}"AccessKey
export ADLS_PRIMARY_KEY=$(az storage account keys list --account-name $STORAGE_ACCOUNT --query '[0].value' --output tsv)

databricks secrets put --scope access_creds --key $ADLS_SECRET_NAME --string-value $ADLS_PRIMARY_KEY
```

## Set up Event Hubs

### Define namespace name and topic name

```
export EH_NAMESPACE=musicapp-<eventhubname>
export EH_KAFKA_TOPIC=musicapp-events
```

### Create Event Hubs namespace with Kafka enabled

```
az eventhubs namespace create --name $EH_NAMESPACE \
  --location northeurope \
  --sku standard \
  --enable-kafka
```

### Create Event Hubs Kafka topic (hub)

```
az eventhubs eventhub create --name $EH_KAFKA_TOPIC \
  --namespace-name $EH_NAMESPACE
```

### Create Auth rules for send and listen

Send:

```
az eventhubs eventhub authorization-rule create \
  --namespace-name $EH_NAMESPACE \
  --eventhub-name $EH_KAFKA_TOPIC \
  --name adbSendMusicAppEvents \
  --rights Send
```

Listen: 

```
az eventhubs eventhub authorization-rule create \
  --namespace-name $EH_NAMESPACE \
  --eventhub-name $EH_KAFKA_TOPIC \
  --name adbListenMusicAppEvents \
  --rights Listen
```

### Check that the keys were created (optional)

Send:

```
az eventhubs eventhub authorization-rule keys list \
  --namespace-name $EH_NAMESPACE \
  --eventhub-name $EH_KAFKA_TOPIC \
  --name adbSendMusicAppEvents
```

Listen:

```
az eventhubs eventhub authorization-rule keys list \
  --namespace-name $EH_NAMESPACE \
  --eventhub-name $EH_KAFKA_TOPIC \
  --name adbListenMusicAppEvents
```


### Add Event Hubs access key to Databricks secrets

Send key:

```
export SEND_KEY_NAME=ehSend"${EH_NAMESPACE}"AccessKey
export SEND_PRIMARY_KEY=$(az eventhubs eventhub authorization-rule keys list --namespace-name $EH_NAMESPACE --eventhub-name $EH_KAFKA_TOPIC --name adbSendMusicAppEvents --query 'primaryKey' --output tsv)

databricks secrets put --scope access_creds --key $SEND_KEY_NAME --string-value $SEND_PRIMARY_KEY
```

Listen key:

```
export LISTEN_KEY_NAME=ehListen"${EH_NAMESPACE}"AccessKey
export LISTEN_PRIMARY_KEY=$(az eventhubs eventhub authorization-rule keys list --namespace-name $EH_NAMESPACE --eventhub-name $EH_KAFKA_TOPIC --name adbListenMusicAppEvents --query 'primaryKey' --output tsv)

databricks secrets put --scope access_creds --key $LISTEN_KEY_NAME --string-value $LISTEN_PRIMARY_KEY
```

## Set up Databricks Cluster


### Create Databricks Cluster

Echo your `STORAGE_ACCOUNT` and `ADLS_SECRET_NAME` to be used in the cluster settings.

```
echo $STORAGE_ACCOUNT 
echo $ADLS_SECRET_NAME
```

Create a file called `create-cluster.json`, add the following settings, change `<username>`, change `<STORAGE_ACCOUNT>` to match your account storage name, and change `<ADLS_SECRET_NAME>` to match your adls secrate name:

```
{
  "cluster_name": "musicapp-demo-9-0-<username>",
  "spark_version": "9.0.x-scala2.12",
  "node_type_id": "Standard_D3_v2",
  "spark_conf": {
    "spark.databricks.enableWsfs": false,
    "spark.hadoop.fs.azure.account.key.<STORAGE_ACCOUNT>.dfs.core.windows.net": "{{secrets/access_creds/<ADLS_SECRET_NAME>}}"
  },
  "num_workers": 2
}
```

Run the clusters create command to create a cluster in the Databricks workspace.

```
databricks clusters create --json-file create-cluster.json
```


### Check cluster settings

Once cluster is created go advanced options > Spark config and check the following settings have been applied:

```
spark.databricks.enableWsfs false
spark.hadoop.fs.azure.account.key.<STORAGE_ACCOUNT>.dfs.core.windows.net {{secrets/access_creds/<ADLS_SECRET_NAME>}}
```



