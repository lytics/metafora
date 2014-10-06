#!/bin/bash
export RunningEtcdDockers=$(sudo docker ps --no-trunc -a | grep metafora-etcd- | awk '{print $1}')
if [[ -n $RunningEtcdDockers ]]; then
echo stopping existing etcd mTesetEtcdnode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
    echo sudo docker stop ${RunningEtcdDockers}
    sudo docker stop ${RunningEtcdDockers}
    echo 


    echo removing existing etcd docker containers 
    echo ----------------------------------------------------------------------------------------------------------------
    sudo docker rm ${RunningEtcdDockers}
    echo 
fi

echo starting new etcd mTesetEtcdnode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker run -d --name="metafora-etcd-a" \
    --net=host \
    -p 127.0.0.1:8001:8001 -p 127.0.0.1:5001:5001 \
    coreos/etcd -peer-addr 127.0.0.1:8001 -addr 127.0.0.1:5001 -name metafora-a
sudo docker run -d --name="metafora-etcd-b" \
    --net=host \
    -p 127.0.0.1:8002:8002 -p 127.0.0.1:5002:5002 \
    coreos/etcd -peer-addr 127.0.0.1:8002 -addr 127.0.0.1:5002 -name metafora-b -peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
sudo docker run -d --name="metafora-etcd-c" \
    --net=host \
    -p 127.0.0.1:8003:8003 -p 127.0.0.1:5003:5003 \
    coreos/etcd -peer-addr 127.0.0.1:8003 -addr 127.0.0.1:5003 -name metafora-c -peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
echo 

echo list of running mTesetEtcdnode docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker ps | grep metafora-etcd-
