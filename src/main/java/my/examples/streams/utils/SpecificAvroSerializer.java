package my.examples.streams.utils;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class SpecificAvroSerializer<T extends SpecificRecord> implements Serializer<T> {

    KafkaAvroSerializer inner;

    public SpecificAvroSerializer() {
        inner = new KafkaAvroSerializer();
    }

    public SpecificAvroSerializer(SchemaRegistryClient registryClient) {
        inner = new KafkaAvroSerializer(registryClient);
    }

    public SpecificAvroSerializer(SchemaRegistryClient registryClient, Map<String, ?> props) {
        inner = new KafkaAvroSerializer(registryClient, props);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> effectiveConfigs = new HashMap<>(configs);
        effectiveConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        inner.configure(effectiveConfigs, isKey);
    }

    @Override
    public byte[] serialize(String topic, T record) {
        return inner.serialize(topic, record);
    }

    @Override
    public void close() {
        inner.close();
    }
}
