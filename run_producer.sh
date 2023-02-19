. ./.pws

export LOG_LEVEL=DEBUG
export LOG_FORMAT=

export DEBUGLEVEL=1

#export TESTSIZE=6     # 2
export TESTSIZE=100     # 10
#export TESTSIZE=1000     # 1 thou
#export TESTSIZE=10000     # 10 thou
#export TESTSIZE=100000     # 100 thou
#export TESTSIZE=1000000     # 1 Mil
#export TESTSIZE=10000000    # 10 Mil
#export TESTSIZE=100000000   # 100 Mil
#export TESTSIZE=150000000   # 150 Mil
#export TESTSIZE=600000000   # 600 Mil

export SLEEP=0              # 1000 = 1 second


########################################################################
# Golang  Examples : https://developer.confluent.io/get-started/go/

export kafka_flushinterval=100

export kafka_bootstrap_port=9092
#export kafka_topic_name=SNDBX_TFM_events
export kafka_topic_name=SNDBX_TFM_engineResponse

### Confluent Cloud Cluster
#export kafka_bootstrap_servers= -> See .pws
export kafka_security_protocol=SASL_SSL
export kafka_sasl_mechanisms=PLAIN
#export kafka_sasl_username= -> See .pws
#export kafka_sasl_password= -> See .pws
export kafka_num_partitions=3
export kafka_replication_factor=3
export kafka_retension=3600
export kafka_parseduration=60s


go run -v cmd/producer.go
#./cmd/producer

# https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html
# kafkacat -b localhost:9092 -t SNDBX_AppLab
