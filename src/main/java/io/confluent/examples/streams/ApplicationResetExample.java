package io.confluent.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import static org.apache.kafka.streams.StreamsConfig.*;

import java.util.Properties;

public class ApplicationResetExample {

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "application-reset-example");
        props.put(CLIENT_ID_CONFIG, "application-reset-client");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(STATE_DIR_CONFIG, "/tmp/streams-data");
        props.put(COMMIT_INTERVAL_MS_CONFIG, 500);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> inputStream = builder.stream("input-topic");
        inputStream.selectKey((key, value) -> value.split(" ")[0])
                .groupByKey()
                .count("Count")
                .to(Serdes.String(), Serdes.Long(), "outputTopic");

        KafkaStreams streams = new KafkaStreams(builder, props);
        if (args.length > 0 && args[0].equals("--reset")) {
            streams.cleanUp();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
