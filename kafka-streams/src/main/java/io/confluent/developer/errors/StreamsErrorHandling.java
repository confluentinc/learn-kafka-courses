package io.confluent.developer.errors;

import io.confluent.developer.StreamsUtils;
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
import java.util.Map;
import java.util.Properties;

public class StreamsErrorHandling {
    //This is for learning purposes only!
    static boolean throwErrorNow = true;
    
    public static class StreamsDeserializationErrorHandler implements DeserializationExceptionHandler {
        int errorCounter = 0;

        @Override
        public DeserializationHandlerResponse handle(ProcessorContext context,
                                                     ConsumerRecord<byte[], byte[]> record,
                                                     Exception exception) {
            if (errorCounter++ < 25) {
                return DeserializationHandlerResponse.CONTINUE;
            }
            return DeserializationHandlerResponse.FAIL;
        }

        @Override
        public void configure(Map<String, ?> configs) { }
    }

    public static class StreamsRecordProducerErrorHandler implements ProductionExceptionHandler {
        @Override
        public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
                                                         Exception exception) {
            if (exception instanceof RecordTooLargeException ) {
                return ProductionExceptionHandlerResponse.CONTINUE;
            }
            return ProductionExceptionHandlerResponse.FAIL;
        }

        @Override
        public void configure(Map<String, ?> configs) { }
    }

    public static class StreamsCustomUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
        @Override
        public StreamThreadExceptionResponse handle(Throwable exception) {
            if (exception instanceof StreamsException) {
                Throwable originalException = exception.getCause();
                if (originalException.getMessage().equals("Retryable transient error")) {
                    return StreamThreadExceptionResponse.REPLACE_THREAD;
                }
            }
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        }
    }

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = StreamsUtils.loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-error-handling");

        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                StreamsDeserializationErrorHandler.class);
        streamsProps.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                StreamsRecordProducerErrorHandler.class);


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

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.setUncaughtExceptionHandler(new StreamsCustomUncaughtExceptionHandler());
        TopicLoader.runProducer();
        kafkaStreams.start();
    }
}
