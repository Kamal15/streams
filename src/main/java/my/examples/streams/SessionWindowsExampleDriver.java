package my.examples.streams;

import my.examples.streams.pojo.PlayEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SessionWindowsExampleDriver {

    static final String PLAY_EVENT_INPUT_TOPIC = "PlayEvents";
    static final String PLAY_USER_COUNT_TOPIC = "PlayUserCount";

    static final String bootstrapServers = "localhost:9092";
    static final Long INACTIVITY_GAP_MS = TimeUnit.MINUTES.toMillis(1);
    private static final int NUM_RECORDS_SENT = 13;

    static final Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(
            new WindowedSerializer<>(new StringSerializer()), new WindowedDeserializer<>(new StringDeserializer()));

    public static void main(String[] args) {
        produceInputs();
        consumeOutputs();
    }

    private static void produceInputs() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        long startTime = System.currentTimeMillis();
        try (KafkaProducer<String, Serializable> producer = new KafkaProducer<>(props, new StringSerializer(), new MySerde<>().serializer)) {
            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime, "wildcraft", new PlayEvent("wildcraft", 0))); // W1
            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime, "vip", new PlayEvent("vip", 0))); // W2
            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime, "tourister", new PlayEvent("tourister", 0))); // W3
            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime, "liviya", new PlayEvent("liviya", 0))); // W4

            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime + INACTIVITY_GAP_MS, "wildcraft", new PlayEvent("wildcraft", 0))); // belongs to W1
            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime + INACTIVITY_GAP_MS / 2, "vip", new PlayEvent("vip", 0))); // belongs to W2
            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime + INACTIVITY_GAP_MS / 3, "tourister", new PlayEvent("tourister", 0))); // belongs to W3
            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime + INACTIVITY_GAP_MS + 1, "liviya", new PlayEvent("liviya", 0))); // W5

            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime + 2 * INACTIVITY_GAP_MS + 1, "wildcraft", new PlayEvent("wildcraft", 0))); // W6
            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime + INACTIVITY_GAP_MS, "vip", new PlayEvent("vip", 0))); // belongs to W2
            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime + 2 * INACTIVITY_GAP_MS, "tourister", new PlayEvent("tourister", 0))); // W7
            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime + 2 * INACTIVITY_GAP_MS, "liviya", new PlayEvent("liviya", 0))); // belongs to W5

            producer.send(new ProducerRecord<>(PLAY_EVENT_INPUT_TOPIC, null, startTime + INACTIVITY_GAP_MS + 2, "wildcraft", new PlayEvent("wildcraft", 0))); // Merges W1 + W6
        }
    }

    private static void consumeOutputs() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "session-window-lambda-example-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<Windowed<String>, Long> consumer = new KafkaConsumer<>(props, windowedSerde.deserializer(),
                new LongDeserializer())) {
            consumer.subscribe(Collections.singleton(PLAY_USER_COUNT_TOPIC));
            int received = 0;
            while (received < NUM_RECORDS_SENT) {
                final ConsumerRecords<Windowed<String>, Long> records = consumer.poll(Long.MAX_VALUE);
                records.forEach(record -> {
                    final Windowed<String> windowed = record.key();
                    System.out.println(record.offset() + ", " + windowed.key() + ":" + record.value() + ", window : " +
                            new Date(windowed.window().start()) + " - " + new Date(windowed.window().end()));
                });
                received += records.count();
            }
        }
    }
}
