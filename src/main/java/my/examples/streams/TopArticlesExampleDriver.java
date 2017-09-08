package my.examples.streams;

import my.examples.streams.pojo.PageView;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;

public class TopArticlesExampleDriver {

    static final String TOP_NEWS_PER_INDUSTRY_TOPIC = "TopNewsPerIndustry";
    static final String PAGE_VIEWS = "PageViews";
    static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        produceInputs();
        consumeOutputs();
    }

    private static void produceInputs() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        final Random random = new Random();
        final String[] industries = {"engineering", "telco", "law", "health", "finance", "fiction"};
        final String[] users = {"erica", "adam", "bob", "erica", "hossana", "kumar", "adi", "madan"};
        final String[] pages = {"index.html", "news.html", "contact.html", "about.html", "stuff.html"};

        try (final KafkaProducer<String, PageView> producer = new KafkaProducer<>(props, new StringSerializer(), new MySerde<PageView>().serializer())) {
            for (String user : users) {
                String industry = industries[random.nextInt(industries.length)];
                for (int i = 0; i < random.nextInt(10); i++) {
                    PageView pageView = new PageView();
                    pageView.setUser(user);
                    pageView.setIndustry(industry);
                    pageView.setFlags("ARTICLE");
                    pageView.setPage(pages[random.nextInt(pages.length)]);
                    producer.send(new ProducerRecord<>(PAGE_VIEWS, null, pageView));
                }
            }
        }
    }

    private static void consumeOutputs() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "topic-article-example-consumer");

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props, new StringDeserializer(),
                new StringDeserializer())) {
            consumer.subscribe(Collections.singleton(TOP_NEWS_PER_INDUSTRY_TOPIC));
            while (true) {
                final ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                records.forEach(record -> System.out.println(record.key() + ":" + record.value()));
            }
        }
    }
}
