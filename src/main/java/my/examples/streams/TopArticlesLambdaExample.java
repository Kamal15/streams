package my.examples.streams;

import my.examples.streams.pojo.PageView;
import my.examples.streams.pojo.PageViewStats;
import my.examples.streams.utils.PriorityQueueSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static my.examples.streams.TopArticlesExampleDriver.TOP_NEWS_PER_INDUSTRY_TOPIC;

public class TopArticlesLambdaExample {

    private static boolean isArticle(final PageView view) {
        return "ARTICLE".equals(view.getFlags());
    }

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "top-articles-lambda-examples");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "top-articles-lambda-example-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, TopArticlesExampleDriver.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MySerde.class);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/streams-data");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final MySerde<PageView> viewSerde = new MySerde<>();
        final MySerde<PageViewStats> statsSerde = new MySerde<>();
        final Serde<Windowed<String>> windowedStringSerde = Serdes.serdeFrom(new WindowedSerializer<>(new StringSerializer()),
                new WindowedDeserializer<>(new StringDeserializer()));

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, PageView> pageViews = builder.stream(TopArticlesExampleDriver.PAGE_VIEWS);
        final KTable<Windowed<PageView>, Long> viewCounts = pageViews.filter((nullKey, pageView) -> isArticle(pageView))
                .map((nullKey, pageView) -> {
                    // pageView.setUser("user"); // actually, not required
                    return KeyValue.pair(pageView, pageView);
                }).groupByKey(viewSerde, viewSerde)
                .count(TimeWindows.of(60 * 1000), "PageViewCountStore");

        final KGroupedTable<Windowed<String>, PageViewStats> statsKGroupedTable = viewCounts.groupBy((windowedArticle, count) -> {
            final PageView view = windowedArticle.key();
            final Windowed<String> windowedIndustry = new Windowed<>(view.getIndustry(), windowedArticle.window());
            PageViewStats stats = new PageViewStats(view.getUser(), view.getPage(), view.getIndustry(), count);
            return KeyValue.pair(windowedIndustry, stats);
        }, windowedStringSerde, statsSerde);

        final Comparator<PageViewStats> comparator = Comparator.comparingLong(PageViewStats::getCount);
        final KTable<Windowed<String>, PriorityQueue<PageViewStats>> allViewCounts = statsKGroupedTable.aggregate(
                () -> new PriorityQueue<>(comparator),
                (windowedIndustry, stats, queue) -> {
                    queue.add(stats);
                    System.out.println(windowedIndustry + " adding the stats : " + stats);
                    return queue;
                },
                (windowedIndustry, stats, queue) -> {
                    queue.remove(stats);
                    System.out.println(windowedIndustry + " removing the stats : " + stats);
                    return queue;
                },
                new PriorityQueueSerde<>(comparator, statsSerde), "AllArticles");

        final int topN = 100;
        final KTable<Windowed<String>, String> topViewCounts = allViewCounts
                .mapValues(queue -> {
                    final StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < topN; i++) {
                        final PageViewStats stats = queue.poll();
                        if (stats == null) {
                            break;
                        }
                        sb.append(stats.getPage());
                        sb.append("\n");
                    }
                    return sb.toString();
                });

        // topViewCounts.to(windowedStringSerde, Serdes.String(), TOP_NEWS_PER_INDUSTRY_TOPIC);
        topViewCounts.toStream().map((windowedIndustry, pages) -> KeyValue.pair(windowedIndustry.toString(), pages))
                .to(Serdes.String(), Serdes.String(), TOP_NEWS_PER_INDUSTRY_TOPIC);

        final CountDownLatch latch = new CountDownLatch(1);
        final KafkaStreams streams = new KafkaStreams(builder, props);
        streams.cleanUp();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

       /* Thread queryThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(10_000);
                    System.out.println("Printing the Key Value Pairs, State = " + streams.state().name());
                    final ReadOnlyKeyValueStore<Windowed<PageView>, Long> countsByRegion = streams.store("PageViewCountStore", QueryableStoreTypes.keyValueStore());
                    countsByRegion.all().forEachRemaining(record -> {
                        System.out.println("QuerYable state store. key : " + record.key + ", val : " + record.value);
                    });
                    System.out.println();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
        queryThread.start();*/
        streams.start();

    }
}
