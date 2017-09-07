package my.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

public class SumLambdaExampleDriver {

    static final String NUMBERS_TOPIC = "numbers";
    static final String ODD_SUM_TOPIC = "odd_sum";

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        produceInputs(bootstrapServers);
        consumerOutputs(bootstrapServers);
    }

    private static void consumerOutputs(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sum-lambda-consumer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<Integer, Integer> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singleton(ODD_SUM_TOPIC));
            while (true) {
                final ConsumerRecords<Integer, Integer> records = consumer.poll(Long.MAX_VALUE);
                records.forEach(record -> System.out.println("Current sum of odd numbers is " + record.value()));
            }
        }
    }

    private static void produceInputs(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        try (KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(props)) {
            IntStream.range(0, 100).forEach(val ->
                    producer.send(new ProducerRecord<>(NUMBERS_TOPIC, val, val)));
        }
    }
}
