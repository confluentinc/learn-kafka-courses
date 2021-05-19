package io.confluent.developer.errors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class StreamsErrorHandling {

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = new Properties();
        streamsProps.load(new FileInputStream("src/main/resources/streams.properties"));
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-error-handling");
        streamsProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamsDeserializationErrorHandler.class);
        streamsProps.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, StreamsRecordProducerErrorHandler.class);


        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("error.input.topic");
        final String outputTopic = streamsProps.getProperty("error.output.topic");

        final String orderNumberStart = "orderNumber-";
        KStream<String, String> streamWithErrorHandling = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        streamWithErrorHandling.filter((key, value) -> value.contains(orderNumberStart))
                .mapValues(value -> value.substring(value.indexOf(orderNumberStart)))
                .filter((key, value) -> Long.parseLong(value) > 1000)
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps);
        kafkaStreams.setUncaughtExceptionHandler(new StreamsCustomUncaughtExceptionHandler());
        kafkaStreams.start();
    }

    static class StreamsDeserializationErrorHandler implements DeserializationExceptionHandler {
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
        public void configure(Map<String, ?> configs) {

        }
    }

    static class StreamsRecordProducerErrorHandler implements ProductionExceptionHandler {
        @Override
        public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record,
                                                         Exception exception) {
            if (exception instanceof RecordTooLargeException ) {
                return ProductionExceptionHandlerResponse.CONTINUE;
            }
            return ProductionExceptionHandlerResponse.FAIL;
        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

    static class StreamsCustomUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {
        @Override
        public StreamThreadExceptionResponse handle(Throwable exception) {
            if (exception instanceof NullPointerException
               || exception instanceof NumberFormatException) {
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            }
          return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        }
    }
}
