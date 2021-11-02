#!/usr/bin/env bash

getdeploymentstatus(){
	result=$(kubectl get deployments)
	printf '%s\n' "$result" | while IFS= read -r line
	do
		status=$(echo $line | awk '{print $4}')
		deploymentName=$(echo $line | awk '{print $1}')
		deploymentReady=$(expr "${status}" == "1" "|" "${status}" == "AVAILABLE")
        if [ "${deploymentReady}" = "0" ];
        then
            echo "0"
        fi
	done
    echo "1"
}

while getopts "r:n:p:s:l:" opt
do
   case "$opt" in
      r ) resourceGroup="$OPTARG" ;;
      n ) aksName="$OPTARG" ;;
      p ) sqlPassword="$OPTARG" ;;
      s ) subscriptionId="$OPTARG" ;;
      ? ) helpFunction ;; # Print helpFunction in case parameter is non-existent
   esac
done

if [ "${sqlPassword}" = "" ];
then
	echo "ERROR: Parameter sqlPassword (with option -p) is required."
	exit 1
fi

if [ "${aksName}" = "" ];
then
	echo "ERROR: Parameter aksName (with option -n) is required."
	exit 1
fi

if [ "${subscriptionId}" = "" ];
then
	echo "ERROR: Parameter subscriptionId (with option -s) is required."
	exit 1
fi

if [ "${resourceGroup}" = "" ];
then
	echo "ERROR: Parameter resourceGroup (with option -r) is required."
	exit 1
fi

echo "INFO: Install Azure CLI"
curl -sL https://aka.ms/InstallAzureCLIDeb | bash || exit 1

echo "INFO: Install kubectl"
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl || exit 1

echo "INFO: Install helm"
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh || exit 1

echo "INFO: Login to Azure"
az login

echo "INFO: Set subscription"
az account set -s $subscriptionId || exit 1

aksList=$(az aks list -g $resourceGroup --query "[?name == '${aksName}'].name")
if [ "${aksList}" = "[]" ]; then
	echo "ERROR: The specified AKS cluster ${aksName} in resource group ${resourceGroup} doesn't exist or you don't have permission to access it."
	exit 1
fi

echo "INFO: Get AKS credentials"
az aks install-cli

az aks get-credentials --resource-group $resourceGroup --name $aksName --overwrite-existing

echo "INFO: Add feast charts"
helm repo add feast-charts https://feast-charts.storage.googleapis.com

echo "INFO: Install feast"
kubectl create secret generic feast-postgresql --from-literal=postgresql-password="${sqlPassword}"
helm install feast-release feast-charts/feast

timeout=0
while true
do
    if [ $timeout -ge 120 ];
	then
		echo "ERROR: Deployment timeout after 60 minutes. Please check the error and try again."
		exit 1
	else
		let timeout='timeout+1'
	fi
    ready="$(getdeploymentstatus)"
    if [ "${ready}" = "1" ];
	then
		echo "INFO: Deployment finished"
		break
	fi
    echo "INFO: Deployment is not finished yet. check again in 30 seconds"
    sleep 30
done

podIP=$(kubectl get pods -l app=feast-core -o jsonpath="{.items[0].status.podIP}")
echo "Feast deployment completed."
echo "You can now access the Feast service with ip ${podIP}:6565 within the same virtual network where the AKS cluster is deployed to."

