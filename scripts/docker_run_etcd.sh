export PUBLIC_IP=`hostname --ip-address`

echo You may need to run this once:
echo sudo docker pull coreos/etcd
echo 
echo

sudo docker stop $(sudo docker ps --no-trunc | grep coreos/etcd | awk '{print $1}')

sudo docker run -d -p 8001:8001 -p 5001:5001 coreos/etcd -peer-addr ${PUBLIC_IP}:8001 -addr ${PUBLIC_IP}:5001 -name etcdnode1
sudo docker run -d -p 8002:8002 -p 5002:5002 coreos/etcd -peer-addr ${PUBLIC_IP}:8002 -addr ${PUBLIC_IP}:5002 -name etcdnode2 -peers ${PUBLIC_IP}:8001,${PUBLIC_IP}:8002,${PUBLIC_IP}:8003
sudo docker run -d -p 8003:8003 -p 5003:5003 coreos/etcd -peer-addr ${PUBLIC_IP}:8003 -addr ${PUBLIC_IP}:5003 -name etcdnode3 -peers ${PUBLIC_IP}:8001,${PUBLIC_IP}:8002,${PUBLIC_IP}:8003


