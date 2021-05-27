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

/*
    @Test
    public void cogroupingTest() throws IOException {
        final CogroupingStreams instance = new CogroupingStreams();
        final Properties allProps = instance.loadEnvProperties(TEST_CONFIG_FILE);

        final Topology topology = instance.buildTopology(allProps);
        try (final TopologyTestDriver testDriver = new TopologyTestDriver(topology, allProps)) {

            final Serde<String> stringAvroSerde = Serdes.String();
            final SpecificAvroSerde<LoginEvent> loginEventSerde = CogroupingStreams.getSpecificAvroSerde(allProps);
            final SpecificAvroSerde<LoginRollup> rollupSerde = CogroupingStreams.getSpecificAvroSerde(allProps);

            final Serializer<String> keySerializer = stringAvroSerde.serializer();
            final Deserializer<String> keyDeserializer = stringAvroSerde.deserializer();
            final Serializer<LoginEvent> loginEventSerializer = loginEventSerde.serializer();
            final List<LoginEvent> appThreeEvents = new ArrayList<>();
            appThreeEvents.add(LoginEvent.newBuilder().setAppId("three").setUserId("foo").setTime(5L).build());
            appThreeEvents.add(LoginEvent.newBuilder().setAppId("three").setUserId("foo").setTime(6l).build());
            appThreeEvents.add(LoginEvent.newBuilder().setAppId("three").setUserId("bar").setTime(7L).build());
            appThreeEvents.add(LoginEvent.newBuilder().setAppId("three").setUserId("bar").setTime(9L).build());

            final Map<String, Map<String, Long>> expectedEventRollups = new TreeMap<>();
            final Map<String, Long> expectedAppOneRollup = new HashMap<>();
            final LoginRollup expectedLoginRollup = new LoginRollup(expectedEventRollups);
            expectedAppOneRollup.put("foo", 1L);
            expectedAppOneRollup.put("bar", 2L);
            expectedEventRollups.put("one", expectedAppOneRollup);

            final Map<String, Long> expectedAppTwoRollup = new HashMap<>();
            expectedAppTwoRollup.put("foo", 2L);
            expectedAppTwoRollup.put("bar", 1L);
            expectedEventRollups.put("two", expectedAppTwoRollup);

            final Map<String, Long> expectedAppThreeRollup = new HashMap<>();
            expectedAppThreeRollup.put("foo", 2L);
            expectedAppThreeRollup.put("bar", 2L);
            expectedEventRollups.put("three", expectedAppThreeRollup);

            sendEvents(appOneEvents, appOneInputTopic);
            sendEvents(appTwoEvents, appTwoInputTopic);
            sendEvents(appThreeEvents, appThreeInputTopic);

            final List<LoginRollup> actualLoginEventResults = outputTopic.readValuesToList();
            final Map<String, Map<String, Long>> actualRollupMap = new HashMap<>();
            for (LoginRollup actualLoginEventResult : actualLoginEventResults) {
                  actualRollupMap.putAll(actualLoginEventResult.getLoginByAppAndUser());
            }
            final LoginRollup actualLoginRollup = new LoginRollup(actualRollupMap);

            assertEquals(expectedLoginRollup, actualLoginRollup);
        }
    }


    private void sendEvents(List<LoginEvent> events, TestInputTopic<String, LoginEvent> testInputTopic) {
        for (LoginEvent event : events) {
             testInputTopic.pipeInput(event.getAppId(), event);
        }
    }
}

 */