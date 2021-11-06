#! /bin/bash
echo INFO: changing directory
cd ~/wizeline-bootcamp/kubernetes
pwd
echo INFO: deleting namespace
kubectl delete namespace airflow
echo INFO: creating namespace
kubectl create namespace airflow
echo INFO: adding apache-airflow
helm repo add apache-airflow https://airflow.apache.org
echo INFO: installing apache-airflow into the airflow namespace
helm install airflow apache-airflow/airflow --namespace airflow