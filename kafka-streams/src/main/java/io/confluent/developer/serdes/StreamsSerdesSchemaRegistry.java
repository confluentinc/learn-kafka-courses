package io.confluent.developer.serdes;

import io.confluent.developer.avro.ProcessedOrder;
import io.confluent.developer.avro.ProductOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamsSerdesSchemaRegistry  {

public static void main(String[]args) throws IOException {
    final Properties streamsProps = new Properties();
    streamsProps.load(new FileInputStream("src/main/resources/streams.properties"));
    streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "schema-registry-streams");

    StreamsBuilder builder = new StreamsBuilder();
    final String inputTopic = streamsProps.getProperty("sr.input.topic");
    final String outputTopic = streamsProps.getProperty("sr.output.topic");
    final Map<String, String> configMap = propertiesToMap(streamsProps);

    // Create the Avro Serde HINT there's a method in this class
    final SpecificAvroSerde<ProductOrder> productOrderSerde = null;
    // Create the Avro Serde HINT there's a method in this class
    final SpecificAvroSerde<ProcessedOrder> processedOrderSerde = null;

    final KStream<String, ProductOrder> orderStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), productOrderSerde));
    orderStream.mapValues(value -> ProcessedOrder.newBuilder()
            .setProduct(value.getProduct())
            .setTimeProcessed(Instant.now().toEpochMilli()).build())
    .to(outputTopic, Produced.with(Serdes.String(), processedOrderSerde));

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
}
