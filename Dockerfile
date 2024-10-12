FROM confluentinc/cp-kafka-connect:7.7.1

COPY ./lib/* /kafka/connect/mongodb-connector/

ENV CLASSPATH="$CLASSPATH:/kafka/connect/mongodb-connector/*"

CMD ["/etc/confluent/docker/run"]
