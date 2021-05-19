package io.confluent.developer.joins;

import io.confluent.developer.avro.ApplianceOrder;
import io.confluent.developer.avro.CombinedOrder;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.User;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamsJoin {

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = new Properties();
        streamsProps.load(new FileInputStream("src/main/resources/streams.properties"));
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "schema-registry-streams");

        StreamsBuilder builder = new StreamsBuilder();
        final String streamOneInput = streamsProps.getProperty("stream_one.input.topic");
        final String streamTwoInput = streamsProps.getProperty("stream_two.input.topic");
        final String tableInput = streamsProps.getProperty("table.input.topic");
        
        final String outputTopic = streamsProps.getProperty("joins.output.topic");
        final Map<String, String> configMap = propertiesToMap(streamsProps);

        final SpecificAvroSerde<ApplianceOrder> applianceSerde = getSpecificAvroSerde(configMap);
        final SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        final SpecificAvroSerde<CombinedOrder> combinedSerde = getSpecificAvroSerde(configMap);
        final SpecificAvroSerde<User> userSerde = getSpecificAvroSerde(configMap);

        final KStream<String, ApplianceOrder> applianceStream = builder.stream(streamOneInput, Consumed.with(Serdes.String(), applianceSerde));
        final KStream<String, ElectronicOrder> electronicStream = builder.stream(streamTwoInput, Consumed.with(Serdes.String(), electronicSerde));
        final KTable<String, User> userTable = builder.table(tableInput, Consumed.with(Serdes.String(), userSerde));

        final ValueJoiner<ApplianceOrder, ElectronicOrder, CombinedOrder> orderJoiner = (applianceOrder, electronicOrder) -> CombinedOrder.newBuilder()
                  .setApplianceId(applianceOrder.getApplianceId())
                  .setElectronicOrderId(electronicOrder.getElectronicId())
                  .setTime(Instant.now().toEpochMilli())
                  .build();

        final ValueJoiner<CombinedOrder, User, CombinedOrder> enrichmentJoiner = (combined, user) -> {
            if (user != null) {
                combined.setUserName(user.getName());
            }
            return combined;
        };

        final KStream<String, CombinedOrder> combinedStream = applianceStream.join(electronicStream,
                                                                             orderJoiner,
                                                                             JoinWindows.of(Duration.ofMinutes(30)));

        combinedStream.leftJoin(userTable,enrichmentJoiner).to(outputTopic, Produced.with(Serdes.String(), combinedSerde));

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
