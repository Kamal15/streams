package io.confluent.examples.streams;

import io.confluent.examples.streams.pojo.WikiFeed;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class WikipediaFeedAvroLambdaExample {

    public static void main(String[] args) {
        KafkaStreams streams = buildWikipediaStream();
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KafkaStreams buildWikipediaStream() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikipedia-feed-avro-lambda-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, WikipediaFeedAvroExampleDriver.bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MySerde.class);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/streams-data");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, WikiFeed> feeds = builder.stream(WikipediaFeedAvroExampleDriver.WIKIPEDIA_FEED);
        final KTable<String, Long> aggregatedFeeds = feeds.filter((nullKey, feed) -> feed.isNew())
                .map((nullKey, feed) -> KeyValue.pair(feed.getUser(), feed))
                .groupByKey()
                .count(WikipediaFeedAvroExampleDriver.WIKIPEDIA_STATS_STORE);

        aggregatedFeeds.to(Serdes.String(), Serdes.Long(), WikipediaFeedAvroExampleDriver.WIKIPEDIA_STATS);
        return new KafkaStreams(builder, props);
    }
}
