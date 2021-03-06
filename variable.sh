#!/usr/bin/env bash

KAFKA_STREAM_VERSION=${KAFKA_STREAM_VERSION:-"1.1.0"}
KAFKA_VERSION=${KAFKA_VERSION:-"0.11.0.2"}
REDIS_VERSION=${REDIS_VERSION:-"4.0.8"}
SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.11"}
SCALA_SUB_VERSION=${SCALA_SUB_VERSION:-"11"}
STORM_VERSION=${STORM_VERSION:-"1.2.1"}
JSTORM_VERSION=${JSTORM_VERSION:-"2.4.0"}
FLINK_VERSION=${FLINK_VERSION:-"1.6.0"}
SPARK_VERSION=${SPARK_VERSION:-"2.3.0"}
HERON_VERSION=${HERON_VERSION:-"0.17.8"}
HAZELCAST_VERSION=${HAZELCAST_VERSION:-"0.6"}

STORM_DIR="apache-storm-$STORM_VERSION"
JSTORM_DIR="jstorm-$JSTORM_VERSION"
REDIS_DIR="redis-$REDIS_VERSION"
KAFKA_DIR="kafka_$SCALA_BIN_VERSION-$KAFKA_VERSION"
KAFKA_STREAM_DIR="kafka_$SCALA_BIN_VERSION-$KAFKA_STREAM_VERSION"
FLINK_DIR="flink-$FLINK_VERSION"
HERON_DIR="heron-$HERON_VERSION"
SPARK_DIR="spark-$SPARK_VERSION-bin-hadoop2.6"
HAZELCAST_DIR="hazelcast-jet-$HAZELCAST_VERSION"


#average,max,sum,std,double_heap,red_black,skip_list,veb
ALGORITHM="skip_list"

#Get one of the closet apache mirrors
#APACHE_MIRROR=$(curl 'https://archive.apache.org/dyn/closer.cgi' |   grep -o '<strong>[^<]*</strong>' |   sed 's/<[^>]*>//g' |   head -1)
APACHE_MIRROR="http://archive.apache.org/dist"
ZK_HOST="zookeeper-node-01"
ZK_PORT="2181"
ZK_CONNECTIONS="$ZK_HOST:$ZK_PORT"
    TOPIC=${TOPIC:-"ad-events"}
#PARTITIONS=${PARTITIONS:-3}
PARTITIONS=1

#CONF_FILE=./conf/localConf.yaml
CONF_FILE=./conf/benchmarkConf.yaml

TPS=${TPS:-700000}
TEST_TIME=${TEST_TIME:-120} #seconds


SPARK_MASTER_HOST="stream-node01"
BATCH="3000"