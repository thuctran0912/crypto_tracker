##  Start the Kafka Connector for Snowpipe streaming
$HOME/snowpipe-streaming/kafka_2.12-2.8.1/bin/connect-standalone.sh $HOME/snowpipe-streaming/scripts/connect-standalone.properties $HOME/snowpipe-streaming/scripts/snowflakeconnectorMSK.properties


## Start the producer that will ingest real-time data to the MSK cluster
python3 $HOME/snowpipe-streaming/scripts/websocket_client/websocket_to_kafka.py | $HOME/snowpipe-streaming/kafka_2.12-2.8.1/bin/kafka-console-producer.sh --broker-list $BS --producer.config $HOME/snowpipe-streaming/scripts/client.properties --topic streaming