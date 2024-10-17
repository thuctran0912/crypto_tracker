## Find the private endpoint for TLS authentication of the MSK cluster
export BS=[cluster_private_endpoint]
echo "export BS=$BS" >> ~/.bashrc

## Create Kafka Topic - each represent different asset (BTC and ETH)

$HOME/snowpipe-streaming/kafka_2.12-2.8.1/bin/kafka-topics.sh --bootstrap-server $BS --command-config $HOME/snowpipe-streaming/scripts/client.properties --create --topic btc_streaming --partitions 2 --replication-factor 2
$HOME/snowpipe-streaming/kafka_2.12-2.8.1/bin/kafka-topics.sh --bootstrap-server $BS --command-config $HOME/snowpipe-streaming/scripts/client.properties --describe --topic btc_streaming


$HOME/snowpipe-streaming/kafka_2.12-2.8.1/bin/kafka-topics.sh --bootstrap-server $BS --command-config $HOME/snowpipe-streaming/scripts/client.properties --create --topic eth_streaming --partitions 2 --replication-factor 2
$HOME/snowpipe-streaming/kafka_2.12-2.8.1/bin/kafka-topics.sh --bootstrap-server $BS --command-config $HOME/snowpipe-streaming/scripts/client.properties --describe --topic eth_streaming