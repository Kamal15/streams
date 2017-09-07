package my.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class SumLambdaExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "sum-lambda-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/streams-data");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<Integer, Integer> numberStream = builder.stream(SumLambdaExampleDriver.NUMBERS_TOPIC);
        numberStream.filter((key, value) -> value % 2 != 0)
                .groupBy((key, value) -> 1)
                .reduce((v1, v2) -> v1 + v2, "sum") // try with aggregation
                .to(SumLambdaExampleDriver.ODD_SUM_TOPIC);

        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
