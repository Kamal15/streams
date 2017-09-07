package my.examples.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class MapFunctionLambdaExample {

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-lambda-example");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "map-function-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<byte[], String> textLines = builder.stream("TextLinesTopic");

        textLines.mapValues(String::toUpperCase); // variant 1 with null / byte[] key

        textLines.map((key, value) -> new KeyValue<>(key, value.toUpperCase())); // variant 2
        final KStream<byte[], String> uppercasedTextLines = textLines.map(((key, value) ->
                KeyValue.pair(key, value.toUpperCase()))); // variant 3

        uppercasedTextLines.to("OriginalAndUppercasedTopic");

        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
