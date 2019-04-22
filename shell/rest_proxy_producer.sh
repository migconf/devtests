#!/usr/bin/env bash

print_usage(){
    echo "Usage: rest_proxy_producer.sh <topic_name> <num_messages> <optional_formatted_msg>"
    echo ""
}

# check args
if [ $# -lt 2 ]; then
    echo "Invalid number of arguments"
    print_usage
    exit 400
fi

echo "Topic: $1"
echo "Number of messages: $2"
echo "format: $3"

formattedMessage=$3

if [[ -z $3 ]]; then
    echo "no formatted msg"
    formattedMessage="this is message number:"
fi

for num in $(seq 1 $2)
do
    response=$(curl -s -X POST -H "Content-Type: application/vnd.kafka.json.v2+json" -H "Accept: application/vnd.kafka.v2+json" \
    --data "{ \"records\" :[{ \"value\":{ \"message\": \"$formattedMessage $num\" }}]}" \
    "http://localhost:8082/topics/$1")

    echo $response
    echo

    sleep 1s
done



