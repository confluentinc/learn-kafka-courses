package io.confluent.developer.windows;

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;

public class StreamsWindowsAdvanced {

    public static void main(String[] args) throws IOException {

        final Properties streamsProps = new Properties();
        streamsProps.load(new FileInputStream("src/main/resources/streams.properties"));
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "advanced-windowed-streams");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("count.input.topic");
        final String outputTopic = streamsProps.getProperty("windowed.output.topic");
        final Map<String, String> configMap = propertiesToMap(streamsProps);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        final KStream<String, ElectronicOrder> electronicStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), electronicSerde));

        electronicStream.groupByKey().windowedBy(TimeWindows.of(Duration.ofHours(1)).grace(Duration.ofMinutes(5)))
                .aggregate(() -> 0.0,
                           (key, order, total) -> total + order.getPrice(),
                           Materialized.with(Serdes.String(), Serdes.Double()))
                .suppress(untilTimeLimit(Duration.ofHours(1),maxBytes(1_000_000L)))
                .toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(),value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

    }

    static Map<String,String> propertiesToMap(final Properties properties) {
        final Map<String, String> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String)key, (String)value));
        return configs;
    }

    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, String> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }
}
