#!/bin/bash
export PUBLIC_IP=`hostname --ip-address`

echo PUBLIC_IP = ${PUBLIC_IP}

echo You may need to run this once:
echo sudo docker pull coreos/etcd
echo 



echo stopping existing etcd mTesetEtcdnode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker stop $(sudo docker ps --no-trunc | grep mTesetEtcdnode | awk '{print $1}')
echo 


echo removing existing etcd mTesetEtcdnode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker rm $(sudo docker ps --no-trunc | grep mTesetEtcdnode | awk '{print $1}')
echo 

echo starting new etcd mTesetEtcdnode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker run -d -p 8001:8001 -p 5001:5001 coreos/etcd -peer-addr ${PUBLIC_IP}:8001 -addr ${PUBLIC_IP}:5001 -name mTesetEtcdnode1
sudo docker run -d -p 8002:8002 -p 5002:5002 coreos/etcd -peer-addr ${PUBLIC_IP}:8002 -addr ${PUBLIC_IP}:5002 -name mTesetEtcdnode2 -peers ${PUBLIC_IP}:8001,${PUBLIC_IP}:8002,${PUBLIC_IP}:8003
sudo docker run -d -p 8003:8003 -p 5003:5003 coreos/etcd -peer-addr ${PUBLIC_IP}:8003 -addr ${PUBLIC_IP}:5003 -name mTesetEtcdnode3 -peers ${PUBLIC_IP}:8001,${PUBLIC_IP}:8002,${PUBLIC_IP}:8003
echo 

echo list of running mTesetEtcdnode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker ps --no-trunc | grep mTesetEtcdnode
