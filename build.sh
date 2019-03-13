#!/bin/bash  

set -x
set -e

make images
docker save kubesigs/kube-batch:local > /tmp/kube-batch-local.tar
gzip -f /tmp/kube-batch-local.tar
scp -i ~/Downloads/pengdu-huawei-aws.pem /tmp/kube-batch-local.tar.gz ubuntu@34.217.97.17:/tmp

ssh -i ~/Downloads/pengdu-huawei-aws.pem ubuntu@34.217.97.17 'gzip -d -f /tmp/kube-batch-local.tar.gz'
ssh -i ~/Downloads/pengdu-huawei-aws.pem ubuntu@34.217.97.17 'sudo docker load -i /tmp/kube-batch-local.tar'
