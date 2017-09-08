package my.examples.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by kamal on 7/6/17.
 */
public class WordCountLambdaExample {

    public static void main(String[] args) throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(WordCountLambdaExample.class);
        logger.debug("Hello Streams!");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, String> textLines = builder.stream("streams-file-input");

        final KStream<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value)
                .count("Counts")
                .toStream();

        wordCounts.to(stringSerde, longSerde, "streams-wordcount-output");

        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        Thread.sleep(7000);
        final ReadOnlyKeyValueStore<String, Long> counts = streams.store("Counts", QueryableStoreTypes.<String, Long>keyValueStore());
        System.out.println("Count of the word \"all\" is " + counts.get("all"));
        System.out.println("Count of the streams \"streams\" is " + counts.get("streams"));
    }
}
