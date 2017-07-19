package stream.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class AnomalyDetectionLambdaExample {

    public static void main(String[] args) {

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "anomaly-detection-lambda-example");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> views = builder.stream("UserClicks");

        final KTable<Windowed<String>, Long> anamalousUsers = views.map((nullKey, username) -> KeyValue.pair(username, username))
                .groupByKey()
                .count(TimeWindows.of(60 * 1000L), "UserCountStore")
                .filter((windowedUserId, count) -> count >= 3);

        anamalousUsers.toStream().map((windowedId, count) -> KeyValue.pair(windowedId.toString(), count)).to(Serdes.String(), Serdes.Long(), "AnomalousUsers");

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
