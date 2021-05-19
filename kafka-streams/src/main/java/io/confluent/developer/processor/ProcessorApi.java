package io.confluent.developer.processor;

import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ProcessorApi {

    final static String storeName = "total-price-store";
    static StoreBuilder<KeyValueStore<String, Double>> totalPriceStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.persistentKeyValueStore(storeName),
            Serdes.String(),
            Serdes.Double());

    public static void main(String[] args) throws IOException {
        final Properties streamsProps = new Properties();
        streamsProps.load(new FileInputStream("src/main/resources/streams.properties"));
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-api-application");

        final String inputTopic = streamsProps.getProperty("processor.input.topic");
        final String outputTopic = streamsProps.getProperty("processor.output.topic");
        final Map<String, String> configMap = propertiesToMap(streamsProps);

        final SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Double> doubleSerde = Serdes.Double();

        final Topology topology = new Topology();
        topology.addSource("source-node", stringSerde.deserializer(), electronicSerde.deserializer(), inputTopic);
        topology.addProcessor("aggregate-price", new TotalPriceOrderProcessorSupplier(storeName), "source-node");
        topology.addSink("sink-node", outputTopic, stringSerde.serializer(), doubleSerde.serializer(), "aggregate-price");

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProps);
        kafkaStreams.start();
    }


    static Map<String, String> propertiesToMap(final Properties properties) {
        final Map<String, String> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String) key, (String) value));
        return configs;
    }

    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, String> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

    static class TotalPriceOrderProcessorSupplier implements ProcessorSupplier<String, ElectronicOrder, String, Double> {
        final String storeName;

        public TotalPriceOrderProcessorSupplier(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public Processor<String, ElectronicOrder, String, Double> get() {
            return new Processor<>() {
                private ProcessorContext<String, Double> context;
                private KeyValueStore<String, Double> store;

                @Override
                public void init(ProcessorContext<String, Double> context) {
                    this.context = context;
                    store = context.getStateStore(storeName);
                    this.context.schedule(Duration.ofSeconds(30), PunctuationType.STREAM_TIME, this::forwardAll);
                }

                private void forwardAll(final long timestamp) {
                    try (KeyValueIterator<String, Double> iterator = store.all()) {
                        while (iterator.hasNext()) {
                            final KeyValue<String, Double> nextKV = iterator.next();
                            final Record<String, Double> totalPriceRecord = new Record<>(nextKV.key, nextKV.value, timestamp);
                            context.forward(totalPriceRecord);
                        }
                    }
                }

                @Override
                public void process(Record<String, ElectronicOrder> record) {
                    final String key = record.key();
                    Double currentTotal = store.get(key);
                    if (currentTotal == null) {
                        currentTotal = 0.0;
                    }
                    Double newTotal = record.value().getPrice() + currentTotal;
                    store.put(key, newTotal);
                }
            };
        }

        @Override
        public Set<StoreBuilder<?>> stores() {
            return Collections.singleton(totalPriceStoreBuilder);
        }
    }
}
