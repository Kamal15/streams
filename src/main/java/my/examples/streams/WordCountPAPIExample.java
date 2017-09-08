package my.examples.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * Created by kamal on 7/12/17.
 */
public class WordCountPAPIExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-papi-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        TopologyBuilder builder = new TopologyBuilder();
        builder.addSink("Reader", "streams-file-input");
        builder.addProcessor("WordCountProcessor", new ProcessorSupplier<String, String>() {
            @Override
            public Processor<String, String> get() {
                return new Processor<String, String>() {
                    ProcessorContext context;
                    KeyValueStore<String, Integer> kvStore;

                    @Override
                    public void init(ProcessorContext context) {
                        this.context = context;
                        this.kvStore = (KeyValueStore<String, Integer>) context.getStateStore("Counts");
                        context.schedule(1000);
                    }

                    @Override
                    public void process(String dummy, String line) {
                        final String[] words = line.split(" ");
                        for (String word : words) {
                            final Integer counter = kvStore.get(word);
                            if (counter == null) {
                                kvStore.put(word, 1);
                            } else {
                                kvStore.put(word, counter + 1);
                            }
                        }
                        context.commit();
                    }

                    @Override
                    public void punctuate(long timestamp) {
                        try (KeyValueIterator<String, Integer> iter = kvStore.all()) {
                            System.out.println("--------------- " + timestamp + " ---------------------------");
                            while (iter.hasNext()) {
                                final KeyValue<String, Integer> keyValue = iter.next();
                                System.out.println("[" + keyValue.key + ", " + keyValue.value + "]");
                                context.forward(keyValue.key, keyValue.value);
                            }
                        }
                    }

                    @Override
                    public void close() {

                    }
                };
            }
        }, "Reader");
        builder.addStateStore(Stores.create("Counts").withStringKeys().withIntegerValues().inMemory().build(), "WordCountProcessor");
        builder.addSink("Sink", "streams-wordcount-processor-output", "WordCountProcessor");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
