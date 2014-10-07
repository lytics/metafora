#!/bin/bash
export RunningEtcdDockers=$(sudo docker ps -a | grep metafora-etcd- | awk '{print $1}')
if [[ -n $RunningEtcdDockers ]]; then
echo stopping existing etcd metafora docker containers 
echo ----------------------------------------------------------------------------------------------------------------
    echo sudo docker stop ${RunningEtcdDockers}
    sudo docker stop ${RunningEtcdDockers}
    echo 


    echo removing existing etcd docker containers 
    echo ----------------------------------------------------------------------------------------------------------------
    sudo docker rm ${RunningEtcdDockers}
    echo 
fi

echo starting new etcd metafora docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker run -d --name="metafora-etcd-a" --net=host coreos/etcd \
    -peer-addr 127.0.0.1:8001 -peer-bind-addr 127.0.0.1:8001 -addr 127.0.0.1:5001 -bind-addr 127.0.0.1:5001 -name metafora-a
sudo docker run -d --name="metafora-etcd-b" --net=host coreos/etcd \
    -peer-addr 127.0.0.1:8002 -peer-bind-addr 127.0.0.1:8002 -addr 127.0.0.1:5002 -bind-addr 127.0.0.1:5002 -name metafora-b -peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
sudo docker run -d --name="metafora-etcd-c" --net=host coreos/etcd \
    -peer-addr 127.0.0.1:8003 -peer-bind-addr 127.0.0.1:8003 -addr 127.0.0.1:5003 -bind-addr 127.0.0.1:5003 -name metafora-c -peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
echo 

echo list of running metafora docker containers 
echo ----------------------------------------------------------------------------------------------------------------
sudo docker ps | grep metafora-etcd-
