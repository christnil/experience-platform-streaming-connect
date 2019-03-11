FROM confluentinc/cp-kafka-connect:latest

RUN mkdir /usr/share/java/streaming-connectors

EXPOSE 8083

COPY streaming-connect-sink/build/libs/streaming-connect-sink.jar /usr/share/java/streaming-connectors/
COPY streaming-connect-sink/build/dependencies/* /usr/share/java/streaming-connectors/
