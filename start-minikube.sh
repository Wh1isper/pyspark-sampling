#!/usr/bin/env bash
sudo sysctl fs.protected_regular=0
minikube start --driver=none --kubernetes-version=v1.18.20

# 修复root权限问题（如果用其他用户调试程序）
cd ~ && chmod +rx . && chmod +rx .minikube -R && chmod +rx .kube -R
# 创建本地调试的namespace
kubectl create ns spark-sampling

minikube addons enable metrics-server
#minikube addons enable ingress
#minikube addons enable ingress-dns

# 在集群开启后，给集群所有用户授予超级用户权限
kubectl create clusterrolebinding serviceaccounts-cluster-admin \
    --clusterrole=cluster-admin \
    --group=system:serviceaccounts
