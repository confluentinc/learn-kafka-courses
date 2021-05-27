package io.confluent.developer.event.sourcing;

import io.confluent.developer.event.sourcing.avro.ShoppingCartAction;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.streams.TopologyTestDriver;

import static org.junit.Assert.assertTrue;

public class ShoppingCartAppTest {
    private final static String TEST_CONFIG_FILE = "configuration/test.properties";

    @Test
    public void shoppingCartTest() throws IOException {
        final ShoppingCartApp app = new ShoppingCartApp();
        final Properties allProps = ShoppingCartApp.loadEnvProperties(TEST_CONFIG_FILE);
        final Topology topology = app.buildTopology(allProps);

        final SpecificAvroSerde<ShoppingCartAction> actionSerde = ShoppingCartApp.getSpecificAvroSerde(allProps);

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, allProps)) {
            final TestInputTopic<String, ShoppingCartAction> shoppingCartTestTopic = testDriver.createInputTopic(
                    ShoppingCartApp.SHOPPING_CART_EVENT_TOPIC_NAME,
                    Serdes.String().serializer(),
                    actionSerde.serializer());

            List.of(
                    new ShoppingCartAction("yeva", "at1", "trousers", "add"),
                    new ShoppingCartAction("yeva", "at2", "trousers", "add"),
                    new ShoppingCartAction("yeva", "aj1", "jumpers", "add"),
                    new ShoppingCartAction("yeva", "rt1", "trousers", "remove"),
                    new ShoppingCartAction("yeva", "ah1", "hat", "add"),
                    new ShoppingCartAction("yeva", "out", "", "checkout")
            ).forEach( e -> shoppingCartTestTopic.pipeInput(e.getCustomer(), e) );
        }
    }
}