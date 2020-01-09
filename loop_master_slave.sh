#!/bin/bash

index=1
while true
do
    echo $index
    ./master_slave_test 172.17.42.62 9221 172.17.42.94 9221 || exit -1
    # 172.17.43.137 9221 || exit -1
    # 172.17.41.134 9221 || exit -1
    index=`expr $index + 1`
done
