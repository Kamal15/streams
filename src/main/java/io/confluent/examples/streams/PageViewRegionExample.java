package io.confluent.examples.streams;

import io.confluent.examples.streams.pojo.PageView;
import io.confluent.examples.streams.pojo.PageViewRegion;
import io.confluent.examples.streams.pojo.UserProfile;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PageViewRegionExample {

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "page-view-region-lambda-example");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "page-view-region-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MySerde.class);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/home/kamal/opensource/kafka_2.11-0.11.0.0/streams-state-data");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final KStreamBuilder builder = new KStreamBuilder();
        final KStream<String, PageView> pageViewStream = builder.stream(PageViewRegionExampleDriver.pageViewsTopic);
        final KStream<String, PageView> viewByUser = pageViewStream.map((nullKey, view) -> KeyValue.pair(view.getUser(), view));

        final KTable<String, UserProfile> userProfiles = builder.table(PageViewRegionExampleDriver.userProfilesTopic, "UserProfilesStore");
        final KTable<String, String> userRegions = userProfiles.mapValues(profile -> profile.getRegion());

        final KTable<Windowed<String>, Long> viewsByRegion = viewByUser.leftJoin(userRegions, (view, region) -> {
            PageViewRegion pvg = new PageViewRegion(view.getUser(), view.getPage(), region);
            return pvg;
        }).map((user, pvg) -> KeyValue.pair(pvg.getRegion(), pvg))
                .groupByKey()
                .count(TimeWindows.of(5 * 60 * 1000).advanceBy(60 * 1000), "GeoPageViewStore");

        viewsByRegion.toStream((windowedRegion, count) -> windowedRegion.toString())
                .to(Serdes.String(), Serdes.Long(), PageViewRegionExampleDriver.pageViewsByRegion);

        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.cleanUp();
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        streams.start();
    }
}
