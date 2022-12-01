package io.confluent.developer.errors;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.aggregate.TopicLoader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsErrorHandling {
    //This is for learning purposes only!
    static boolean throwErrorNow = true;
    
    public static class StreamsDeserializationErrorHandler implements DeserializationExceptionHandler {
        int errorCounter = 0;

        @Override
        public DeserializationHandlerResponse handle(ProcessorContext context,
                                                     ConsumerRecord<byte[], byte[]> record,
                                                     Exception exception) {
            // This return null statement is here so the code will compile
            // You need to replace it with some logic described below
            return null;
            // If the number of errors remain under 25 continue processing
            // Otherwise fail
            // Note in both cases you'll return a DeserializationHandlerResponse ENUM
            // To achieve the desired behavior

        }

        @Override
        public void configure(Map<String, ?> configs) { }
    }

    public static class StreamsRecordProducerErrorHandler implements ProductionExceptionHandler {
        @Override
        public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
                                                         Exception exception) {
            // This return null statement is here so the code will compile
            // You need to replace it with some logic described below
            return null;
            // If the exception type is RecordTooLargeException continue working
            // Otherwise fail
            // Note in both cases you'll return a ProductionExceptionHandlerResponse ENUM
        }

        @Override
        public void configure(Map<String, ?> configs) { }
    }

    public static class StreamsCustomUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
        @Override
        public StreamThreadExceptionResponse handle(Throwable exception) {
            // This return null statement is here so the code will compile
            // You need to replace it with some logic described below
            return null;
            
            // Check if the exception is a StreamsException
            // If it is - get the underlying Throwable HINT: exception.getCause()
            // Then check if the error message equals "Retryable transient error"
            // If it does, replace the thread
            // Otherwise shutdown the client
            // Note in both cases return a StreamThreadExceptionResponse ENUM
        }
    }

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = StreamsUtils.loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-error-handling");

        // Set the deserialization exception handler and the production exception handler
        // on the streamsProps object
        // HINT: look in StreamsConfig for Deserialization and Production to get the correct
        // static string configuration names

        streamsProps.put("????", null);
        streamsProps.put("???", null);
        
        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("error.input.topic");
        final String outputTopic = streamsProps.getProperty("error.output.topic");

        final String orderNumberStart = "orderNumber-";
        KStream<String, String> streamWithErrorHandling =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
                        .peek((key, value) -> System.out.println("Incoming record - key " +key +" value " + value));

        streamWithErrorHandling.filter((key, value) -> value.contains(orderNumberStart))
                .mapValues(value -> {
                    if (throwErrorNow) {
                        throwErrorNow = false;
                        throw new IllegalStateException("Retryable transient error");
                    }
                    return value.substring(value.indexOf("-") + 1);
                })
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value))
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(()-> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer();
            kafkaStreams.start();
            try {
                shutdownLatch.await();
            }catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        System.exit(0);
    }
}
