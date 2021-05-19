package io.confluent.developer.time;

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamsTimestampExtractor {

    public static void main(String[] args) throws IOException {

        final Properties streamsProps = new Properties();
        streamsProps.load(new FileInputStream("src/main/resources/streams.properties"));
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowed-streams");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("count.input.topic");
        final String outputTopic = streamsProps.getProperty("windowed.output.topic");
        final Map<String, String> configMap = propertiesToMap(streamsProps);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        final KStream<String, ElectronicOrder> electronicStream = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), electronicSerde).withTimestampExtractor(new OrderTimestampExtractor()));

        electronicStream.groupByKey().windowedBy(TimeWindows.of(Duration.ofHours(1)))
                .aggregate(() -> 0.0,
                        (key, order, total) -> total + order.getPrice(),
                        Materialized.with(Serdes.String(), Serdes.Double()))
                .toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(),value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.start();

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

    static class OrderTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
             ElectronicOrder order = (ElectronicOrder)record.value();
             return order.getTime();
        }
    }
}
