#!/usr/bin/env bash

# params
topics=$1
consGrp=$2
instId=$3
format=$4
offset=$5
loops=10
host="http://localhost:8082/consumers/$consGrp"

# app vars
createResp=""
baseUri=""
topicsSubResp=""
recsResp=""

print_usage(){
    echo "Usage: cloud_producer.sh <topic_name> <consumer_grp> <instance_id> <format> <offset_start>"
    echo ""
}

# check args
if [ $# -lt 5 ]; then
    echo "Invalid number of arguments"
    print_usage
    exit 400
fi

cleanup_consumer(){
    echo ""
    echo "Cleaning up Consumers..."
    curl -s -X DELETE -H "Content-Type: application/vnd.kafka.v2+json" $baseUri
    echo "Consumer deleted"
    echo ""
}

create_consumer(){
    createResp=`curl -s -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
    --data "{ \"name\" : \"$instId\", \"format\": \"$format\", \"auto.offset.reset\": \"$offset\" }" \
    $host`

    echo "create response: $createResp"

    baseUri=`echo $createResp | jq -r '.base_uri'`
}

sub_to_topics(){
    curl -s -X POST -H "Content-Type: application/vnd.kafka.v2+json" \
    --data "{ \"topics\" : [ \"$topics\" ]}" \
    "$baseUri/subscription"

    echo "Topic subscription success"
}

get_topics_subbed() {
    topicsSubResp=`curl -s -X GET -H "Content-Type: application/vnd.kafka.v2+json" "$baseUri/subscription"`
    echo "Topics Subbed: `echo $topicsSubResp | jq '.topics'`"
}

get_messages(){
    echo "Retrieving records..."
    recsResp=`curl -s -X GET -H "Accept: application/vnd.kafka.json.v2+json" "$baseUri/records"`

    echo $recsResp
#    echo "`echo $recsResp | jq`"
}

### trap control-c to clean up consumer groups and istances
trap ctrl_c INT

ctrl_c() {
    cleanup_consumer
    echo "cleanup complete, exiting..."
    echo ""
    exit 0
}

########################################################################################################################
### main flow
########################################################################################################################

create_consumer
echo "baseUri: $baseUri"

if [ -z "$baseUri" ]; then
    echo "BaseUri is null"
    exit -1
fi

sub_to_topics

get_topics_subbed

for n in $(seq 1 1000)
do
    get_messages
    sleep 1s
done

### normal cleanup
cleanup_consumer

echo "exiting..."
echo ""
exit 0
