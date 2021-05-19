package io.confluent.developer.ktable;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KTableExample {

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = new Properties();
        streamsProps.load(new FileInputStream("src/main/resources/streams.properties"));
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");
        // Put these in the properties file
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("ktable.input.topic");
        final String outputTopic = streamsProps.getProperty("ktable.output.topic");

        final String orderNumberStart = "orderNumber-";
        KTable<String, String> firstKTable = builder.table(inputTopic);

        firstKTable.filter((key, value) -> value.contains(orderNumberStart))
                .mapValues(value -> value.substring(value.indexOf(orderNumberStart)))
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .toStream().to(outputTopic);
    }
}
