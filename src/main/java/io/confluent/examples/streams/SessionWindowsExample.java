package io.confluent.examples.streams;

import io.confluent.examples.streams.pojo.PlayEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.SessionWindows;

import java.util.Properties;

public class SessionWindowsExample {

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-windows-example");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "session-windows-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SessionWindowsExampleDriver.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MySerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/home/kamal/opensource/kafka_2.11-0.11.0.0/streams-state-data");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, PlayEvent> playEvents = builder.stream(SessionWindowsExampleDriver.PLAY_EVENT_INPUT_TOPIC);

        playEvents.groupByKey()
                .count(SessionWindows.with(SessionWindowsExampleDriver.INACTIVITY_GAP_MS), "PLAY_EVENT_COUNT_STORE")
                .to(SessionWindowsExampleDriver.windowedSerde, Serdes.Long(), SessionWindowsExampleDriver.PLAY_USER_COUNT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.cleanUp();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }
}
