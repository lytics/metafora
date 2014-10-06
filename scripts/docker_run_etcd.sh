#!/bin/bash
PUBLIC_IP=`hostname --ip-address`
echo PUBLIC_IP = ${PUBLIC_IP}

echo You may need to run this once:
echo sudo docker pull coreos/etcd
echo

echo stopping existing etcd TestEtcdNode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
ETCD_DOCKERS=$(sudo docker ps --no-trunc -a | grep TestEtcdNode | awk '{print $1}')
sudo docker stop ${ETCD_DOCKERS}
echo 


echo removing existing etcd TestEtcdNode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker rm ${ETCD_DOCKERS}
echo 

echo starting new etcd TestEtcdNode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker run -d -p 8001:8001 -p 5001:5001 coreos/etcd -peer-addr ${PUBLIC_IP}:8001 -addr ${PUBLIC_IP}:5001 -name TestEtcdNode1
sudo docker run -d -p 8002:8002 -p 5002:5002 coreos/etcd -peer-addr ${PUBLIC_IP}:8002 -addr ${PUBLIC_IP}:5002 -name TestEtcdNode2 -peers ${PUBLIC_IP}:8001,${PUBLIC_IP}:8002,${PUBLIC_IP}:8003
sudo docker run -d -p 8003:8003 -p 5003:5003 coreos/etcd -peer-addr ${PUBLIC_IP}:8003 -addr ${PUBLIC_IP}:5003 -name TestEtcdNode3 -peers ${PUBLIC_IP}:8001,${PUBLIC_IP}:8002,${PUBLIC_IP}:8003
echo 

echo list of running TestEtcdNode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker ps --no-trunc | grep TestEtcdNode
