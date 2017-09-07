package my.examples.streams;

import my.examples.streams.pojo.WikiFeed;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.stream.IntStream;

public class WikipediaFeedAvroExampleDriver {

    static final String bootstrapServers = "localhost:9092";

    static final String WIKIPEDIA_FEED = "WikipediaFeed";
    static final String WIKIPEDIA_STATS = "WikipediaStats";
    static final String WIKIPEDIA_STATS_STORE = "Counts";

    public static void main(String[] args) {
        produceInputs();
        consumerOutput();
    }

    private static void consumerOutput() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "wikipedia-feed-example-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singleton(WIKIPEDIA_STATS));
            while (true) {
                consumer.poll(Long.MAX_VALUE).forEach(record ->
                        System.out.println(record.key() + "=" + record.value()));
            }
        }
    }

    private static void produceInputs() {
        String[] users = new String[] {"alice", "bob", "christian", "damian", "eno", "ewen", "flurossis", "guozhwang"};
        final Random random = new Random();

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        try (KafkaProducer<String, WikiFeed> producer = new KafkaProducer<>(producerProps,
                new StringSerializer(), new MySerde<WikiFeed>().serializer())) {
            IntStream.range(0, 100).forEach(val -> {
                final WikiFeed wikiFeed = new WikiFeed(users[random.nextInt(users.length)], true, "content");
                producer.send(new ProducerRecord<>(WIKIPEDIA_FEED, null, wikiFeed));
            });
        }
    }

}
