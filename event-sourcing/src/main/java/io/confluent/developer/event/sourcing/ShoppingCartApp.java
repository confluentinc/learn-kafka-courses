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
        Map<String, Long> items = null;
        if (newAction.getAction().equals("checkout")) {
            items = oldState.getItems();
        } else if (oldState.getCheckedOut()) {
            items = new HashMap<>();
        } else {
            items = oldState.getItems();
        }

        ShoppingCartState newState = new ShoppingCartState(
          newAction.getCustomer(),
          newAction.getAction().equals("checkout"),
          oldState.getItems());

        switch (newAction.getAction()) {
            case "add":
                newState.getItems().put(newAction.getItem(),
                  oldState.getItems().getOrDefault(newAction.getItem(), 0L) + 1L);
                break;
            case "remove":
                newState.getItems().put(newAction.getItem(),
                  oldState.getItems().getOrDefault(newAction.getItem(), 0L) - 1L);
                break;
            //case "checkout": // No Action necessary, checkedout flag set above
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
            .peek((k,v) -> System.out.println(String.format("Action: %s", v)))
            .groupByKey()
            .aggregate(
                    () -> new ShoppingCartState("", false, new HashMap<String, Long>()),
                    (aggKey, newAction, cartState) -> aggregate(newAction, cartState),
                        Materialized.with(stringSerde, stateSerde))
            .toStream() // potentially drop anything
            .filter((k,v) -> v.getCheckedOut())
            .peek((k,v) -> System.out.println(String.format("Checked Out Cart : %s", v)))
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