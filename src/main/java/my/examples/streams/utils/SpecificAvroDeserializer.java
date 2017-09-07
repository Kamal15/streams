package my.examples.streams.utils;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

public class SpecificAvroDeserializer<T extends SpecificRecord> implements Deserializer<T> {

    KafkaAvroDeserializer inner;

    public SpecificAvroDeserializer() {
        inner = new KafkaAvroDeserializer();
    }

    public SpecificAvroDeserializer(SchemaRegistryClient registryClient) {
        inner = new KafkaAvroDeserializer(registryClient);
    }

    public SpecificAvroDeserializer(SchemaRegistryClient registryClient, Map<String, ?> props) {
        inner = new KafkaAvroDeserializer(registryClient, props);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> effectiveConfigs = new HashMap<>(configs);
        effectiveConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        inner.configure(effectiveConfigs, isKey);
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        return (T) inner.deserialize(topic, bytes);
    }

    @Override
    public void close() {
        inner.close();
    }
}
