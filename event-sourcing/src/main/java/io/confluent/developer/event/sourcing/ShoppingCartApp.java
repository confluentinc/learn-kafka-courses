package io.confluent.developer.event.sourcing;

import io.confluent.developer.event.sourcing.avro.ShoppingCartAction;
import io.confluent.developer.event.sourcing.avro.ShoppingCartState;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ShoppingCartApp {
    public static final String SHOPPING_CART_EVENT_TOPIC_NAME = "shopping-cart";
    public static final String SHOPPING_CART_STATE_TOPIC_NAME = "shopping-cart-result";

    public static Properties loadEnvProperties(String fileName) throws IOException {
        final Properties allProps = new Properties();
        try (final FileInputStream input = new FileInputStream(fileName)) {
            allProps.load(input);
        }
        return allProps;
    }
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Properties allProps) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure((Map)allProps, false);
        return specificAvroSerde;
    }

    private static ShoppingCartState aggregate(ShoppingCartAction newAction, ShoppingCartState oldState) {
        ShoppingCartState newState = new ShoppingCartState(newAction.getCustomer(), oldState.getItems());

        switch (newAction.getAction()) {
            case "add":
                newState.getItems().put(newAction.getItem(),
                  oldState.getItems().getOrDefault(newAction.getItem(), 0L) + 1L);
                break;
            case "remove":
                newState.getItems().put(newAction.getItem(),
                  oldState.getItems().getOrDefault(newAction.getItem(), 0L) - 1L);
                break;
            case "checkout":
                newState = oldState;
                break;
        }
        return newState;
    }
    public Topology buildTopology(Properties allProps) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<ShoppingCartAction> eventSerde = getSpecificAvroSerde(allProps);
        final Serde<ShoppingCartState> stateSerde  = getSpecificAvroSerde(allProps);
        final StreamsBuilder builder = new StreamsBuilder();

        builder
            .stream(SHOPPING_CART_EVENT_TOPIC_NAME, Consumed.with(stringSerde, eventSerde))
            .groupByKey()
            .aggregate(
                    () -> new ShoppingCartState("", new HashMap<String, Long>()),
                    (aggKey, newAction, cartState) -> aggregate(newAction, cartState),
                        Materialized.with(stringSerde, stateSerde))
            .toStream()
            .peek((k,v) -> System.out.println(v.toString()))
            .to(SHOPPING_CART_STATE_TOPIC_NAME, Produced.with(stringSerde, stateSerde));

        return builder.build();
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "This program takes one argument: the path to an environment configuration file.");
        }
        final Properties allProps = loadEnvProperties(args[0]);
        var app = new ShoppingCartApp();
        final Topology topology = app.buildTopology(allProps);

        final KafkaStreams streams = new KafkaStreams(topology, allProps);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
/*
public class CogroupingStreams {


    public Topology buildTopology(Properties allProps) {
        final StreamsBuilder builder = new StreamsBuilder();
        final String appOneInputTopic = allProps.getProperty("app-one.topic.name");
        final String appTwoInputTopic = allProps.getProperty("app-two.topic.name");
        final String appThreeInputTopic = allProps.getProperty("app-three.topic.name");
        final String totalResultOutputTopic = allProps.getProperty("output.topic.name");



        final KStream<String, LoginEvent> appOneStream = builder.stream(appOneInputTopic, Consumed.with(stringSerde, loginEventSerde));
        final KStream<String, LoginEvent> appTwoStream = builder.stream(appTwoInputTopic, Consumed.with(stringSerde, loginEventSerde));
        final KStream<String, LoginEvent> appThreeStream = builder.stream(appThreeInputTopic, Consumed.with(stringSerde, loginEventSerde));

        final Aggregator<String, LoginEvent, LoginRollup> loginAggregator = new LoginAggregator();

        final KGroupedStream<String, LoginEvent> appOneGrouped = appOneStream.groupByKey();
        final KGroupedStream<String, LoginEvent> appTwoGrouped = appTwoStream.groupByKey();
        final KGroupedStream<String, LoginEvent> appThreeGrouped = appThreeStream.groupByKey();

        appOneGrouped.cogroup(loginAggregator)
            .cogroup(appTwoGrouped, loginAggregator)
            .cogroup(appThreeGrouped, loginAggregator)
            .aggregate(() -> new LoginRollup(new HashMap<>()), Materialized.with(Serdes.String(), loginRollupSerde))
            .toStream().to(totalResultOutputTopic, Produced.with(stringSerde, loginRollupSerde));

        return builder.build();
    }

    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Properties allProps) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure((Map)allProps, false);
        return specificAvroSerde;
    }

    public void createTopics(final Properties allProps) {
        try (final AdminClient client = AdminClient.create(allProps)) {

            final List<NewTopic> topics = new ArrayList<>();

            topics.add(new NewTopic(
                allProps.getProperty("app-one.topic.name"),
                Integer.parseInt(allProps.getProperty("app-one.topic.partitions")),
                Short.parseShort(allProps.getProperty("app-one.topic.replication.factor"))));

            topics.add(new NewTopic(
                allProps.getProperty("app-two.topic.name"),
                Integer.parseInt(allProps.getProperty("app-two.topic.partitions")),
                Short.parseShort(allProps.getProperty("app-two.topic.replication.factor"))));

            topics.add(new NewTopic(
                allProps.getProperty("app-three.topic.name"),
                Integer.parseInt(allProps.getProperty("app-three.topic.partitions")),
                Short.parseShort(allProps.getProperty("app-three.topic.replication.factor"))));

            topics.add(new NewTopic(
                allProps.getProperty("output.topic.name"),
                Integer.parseInt(allProps.getProperty("output.topic.partitions")),
                Short.parseShort(allProps.getProperty("output.topic.replication.factor"))));

            client.createTopics(topics);
        }
    }

    public Properties loadEnvProperties(String fileName) throws IOException {
        final Properties allProps = new Properties();
        final FileInputStream input = new FileInputStream(fileName);
        allProps.load(input);
        input.close();

        return allProps;
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        final CogroupingStreams instance = new CogroupingStreams();
        final Properties allProps = instance.loadEnvProperties(args[0]);
        final Topology topology = instance.buildTopology(allProps);

        instance.createTopics(allProps);

        TutorialDataGenerator dataGenerator = new TutorialDataGenerator(allProps);
        dataGenerator.generate();

        final KafkaStreams streams = new KafkaStreams(topology, allProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    static class TutorialDataGenerator {
        final Properties properties;


        public TutorialDataGenerator(final Properties properties) {
            this.properties = properties;
        }

        public void generate() {
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

            try (Producer<String, LoginEvent> producer = new KafkaProducer<String, LoginEvent>(properties)) {
                HashMap<String, List<LoginEvent>> entryData = new HashMap<>();

                List<LoginEvent> messages1 = Arrays.asList(new LoginEvent("one", "Ted", 12456L),
                    new LoginEvent("one", "Ted", 12457L),
                    new LoginEvent("one", "Carol", 12458L),
                    new LoginEvent("one", "Carol", 12458L),
                    new LoginEvent("one", "Alice", 12458L),
                    new LoginEvent("one", "Carol", 12458L));
                final String topic1 = properties.getProperty("app-one.topic.name");
                entryData.put(topic1, messages1);

                List<LoginEvent> messages2 = Arrays.asList(new LoginEvent("two", "Bob", 12456L),
                    new LoginEvent("two", "Carol", 12457L),
                    new LoginEvent("two", "Ted", 12458L),
                    new LoginEvent("two", "Carol", 12459L));
                final String topic2 = properties.getProperty("app-two.topic.name");
                entryData.put(topic2, messages2);

                List<LoginEvent> messages3 = Arrays.asList(new LoginEvent("three", "Bob", 12456L),
                    new LoginEvent("three", "Alice", 12457L),
                    new LoginEvent("three", "Alice", 12458L),
                    new LoginEvent("three", "Carol", 12459L));
                final String topic3 = properties.getProperty("app-three.topic.name");
                entryData.put(topic3, messages3);


                entryData.forEach((topic, list) ->
                    list.forEach(message ->
                        producer.send(new ProducerRecord<String, LoginEvent>(topic, message.getAppId(), message), (metadata, exception) -> {
                            if (exception != null) {
                                exception.printStackTrace(System.out);
                            } else {
                                System.out.printf("Produced record at offset %d to topic %s %n", metadata.offset(), metadata.topic());
                            }
                        })
                    )
                );
            }
        }
    }

}

 */