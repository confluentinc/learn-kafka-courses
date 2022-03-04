package io.confluent.developer.aggregate.solution;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class StreamsAggregateTest {

    @Test
    public void shouldAggregateRecords() {

        final Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-test");
        streamsProps.put("schema.registry.url", "mock://aggregation-test");

        final String inputTopicName = "input";
        final String outputTopicName = "output";
        final Map<String, Object> configMap =
                StreamsUtils.propertiesToMap(streamsProps);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde =
                StreamsUtils.getSpecificAvroSerde(configMap);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Double> doubleSerde = Serdes.Double();

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, ElectronicOrder> electronicStream =
                builder.stream(inputTopicName, Consumed.with(Serdes.String(), electronicSerde));

        electronicStream.groupByKey().aggregate(() -> 0.0,
                (key, order, total) -> total + order.getPrice(),
                Materialized.with(stringSerde, doubleSerde))
                .toStream().to(outputTopicName, Produced.with(Serdes.String(), Serdes.Double()));

        try (final TopologyTestDriver testDriver = new TopologyTestDriver(builder.build(), streamsProps)) {
            final TestInputTopic<String, ElectronicOrder> inputTopic =
                    testDriver.createInputTopic(inputTopicName,
                            stringSerde.serializer(),
                            electronicSerde.serializer());
            final TestOutputTopic<String, Double> outputTopic =
                    testDriver.createOutputTopic(outputTopicName,
                            stringSerde.deserializer(),
                            doubleSerde.deserializer());

            final List<ElectronicOrder> orders = new ArrayList<>();
            orders.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("1").setUserId("vandeley").setTime(5L).setPrice(5.0).build());
            orders.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("2").setUserId("penny-packer").setTime(5L).setPrice(15.0).build());
            orders.add(ElectronicOrder.newBuilder().setElectronicId("one").setOrderId("3").setUserId("romanov").setTime(5L).setPrice(25.0).build());

            List<Double> expectedValues = List.of(5.0, 20.0, 45.0);
            orders.forEach(order -> inputTopic.pipeInput(order.getElectronicId(), order));
            List<Double> actualValues = outputTopic.readValuesToList();
            assertEquals(expectedValues, actualValues);
        }

    }

}
