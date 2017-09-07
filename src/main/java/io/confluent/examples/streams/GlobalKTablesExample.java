package io.confluent.examples.streams;

import io.confluent.examples.streams.avro.Customer;
import io.confluent.examples.streams.avro.EnricherOrder;
import io.confluent.examples.streams.avro.Order;
import io.confluent.examples.streams.avro.Product;
import io.confluent.examples.streams.utils.SpecificAvroSerde;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class GlobalKTablesExample {

    static final String ORDER_TOPIC = "order";
    static final String CUSTOMER_TOPIC = "customer";
    static final String PRODUCT_TOPIC = "product";
    static final String ENRICHED_ORDER_TOPIC = "enriched-order";

    static final String PRODUCT_STORE = "product-store";
    static final String CUSTOMER_STORE = "customer-store";

    static final String schemaRegistryURL = "http://localhost:8081";
    static final Map<String, String> serdeProps = Collections.singletonMap("schema.registry.url", schemaRegistryURL);
    static final CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryURL, 100);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-tables-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/streams-data");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final SpecificAvroSerde<Order> orderSerde = createSerde(false);
        final SpecificAvroSerde<Product> productSerde = createSerde(false);
        final SpecificAvroSerde<Customer> customerSerde = createSerde(true);
        final SpecificAvroSerde<EnricherOrder> enricherOrderSerde = createSerde(false);

        KStreamBuilder builder = new KStreamBuilder();

        final GlobalKTable<Long, Product> products = builder.globalTable(Serdes.Long(), productSerde, PRODUCT_TOPIC, PRODUCT_STORE);
        final GlobalKTable<Long, Customer> customers = builder.globalTable(Serdes.Long(), customerSerde, CUSTOMER_TOPIC, CUSTOMER_STORE);

        final KStream<Long, Order> orderKStream = builder.stream(Serdes.Long(), orderSerde, ORDER_TOPIC);

        final KStream<Long, CustomerOrder> customerOrdersStream = orderKStream.join(customers,
                (orderId, order) -> order.getCustomerId(),
                (order, customer) -> new CustomerOrder(customer, order));

        final KStream<Long, EnricherOrder> enrichedOrderStream = customerOrdersStream.join(products,
                (orderId, customerOrder) -> customerOrder.order.getProductId(),
                (customerOrder, product) -> new EnricherOrder(product, customerOrder.customer, customerOrder.order));

        enrichedOrderStream.to(Serdes.Long(), enricherOrderSerde, ENRICHED_ORDER_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static class CustomerOrder {
        private final Customer customer;
        private final Order order;

        private CustomerOrder(Customer customer, Order order) {
            this.customer = customer;
            this.order = order;
        }

    }

    private static <VT extends SpecificRecord> SpecificAvroSerde<VT> createSerde(boolean isKey) {
        final SpecificAvroSerde<VT> serde = new SpecificAvroSerde<>(schemaRegistry, serdeProps);
        serde.configure(serdeProps, isKey);
        return serde;
    }
}
