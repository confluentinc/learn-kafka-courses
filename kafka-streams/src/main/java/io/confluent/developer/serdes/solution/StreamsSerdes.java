package io.confluent.developer.serdes.solution;

import io.confluent.developer.serdes.TopicLoader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsSerdes {

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = new Properties();
        streamsProps.load(new FileInputStream("src/main/resources/streams.properties"));
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");

        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("serdes.input.topic");
        final String outputTopic = streamsProps.getProperty("serdes.output.topic");

        final String orderNumberStart = "orderNumber-";
        KStream<String, String> firstStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        firstStream.filter((key, value) -> value.contains(orderNumberStart))
                .mapValues(value -> value.substring(value.indexOf(orderNumberStart)))
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer();
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
