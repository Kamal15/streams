package io.confluent.examples.streams;

import io.confluent.examples.streams.pojo.PageView;
import io.confluent.examples.streams.pojo.UserProfile;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

public class PageViewRegionExampleDriver {

    static final String bootstrapServers = "localhost:9092";
    static final String pageViewsTopic = "PageViews";
    static final String userProfilesTopic = "UserProfiles";
    static final String pageViewsByRegion = "PageViewsByRegion";

    public static void main(String[] args) {
       // produceInputs();
        consumeOutputs();
    }

    private static void produceInputs() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final String[] users = {"erica", "bob", "joe", "damian", "tania", "phil", "sam", "lauren", "joseph"};
        final String[] regions = {"europe", "usa", "asia", "africa"};
        final Random random = new Random();
        try (KafkaProducer<String, Serializable> producer = new KafkaProducer<>(props, new StringSerializer(), new MySerde<>().serializer)) {
            for (String user : users) {
                UserProfile profile = new UserProfile("some", regions[random.nextInt(regions.length)]);
                producer.send(new ProducerRecord<>(userProfilesTopic, user, profile));

                IntStream.range(0, 10).forEach(val -> {
                    PageView pageView = new PageView(user, "index.html", "eng", null);
                    producer.send(new ProducerRecord<>(pageViewsTopic, null, pageView));
                });
            }
        }
    }

    private static void consumeOutputs() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "pageview-region-lambda-example-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new LongDeserializer())) {
            consumer.subscribe(Collections.singleton(pageViewsByRegion));
            final ConsumerRecords<String, Long> records = consumer.poll(Long.MAX_VALUE);
            records.forEach(record-> System.out.println(record.key() + ":" + record.value()));
        }
    }
}
