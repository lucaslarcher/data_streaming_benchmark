#!/bin/bash

cd flink

./bin/start-cluster.sh

timeout 15m ./bin/flink run consumer.jar 192.168.0.181:9092 groupiflink01 16


