package my.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class UserRegionLambdaExample {

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-region-lambda-example");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "user-region-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/streams-data");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        final KTable<String, String> regionTable = builder.table("UserRegion", "UserRegionStore");

        final KTable<String, Long> regionCounts = regionTable.groupBy((user, region) -> KeyValue.pair(region, region))
                .count("CountsByRegion")
                .filter((region, count) -> count >= 2);

        regionCounts.print("Printer");

        final CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.cleanUp();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10_000);
                    System.out.println("Printing the Key Value Pairs, State = " + streams.state().name());
                    final ReadOnlyKeyValueStore<String, Long> countsByRegion = streams.store("CountsByRegion", QueryableStoreTypes.keyValueStore());
                    countsByRegion.all().forEachRemaining(keyValue -> System.out.println(keyValue.key + "=" + keyValue.value));
                    System.out.println();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }).start();

        streams.start();
    }
}
