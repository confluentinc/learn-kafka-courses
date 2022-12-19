package io.confluent.developer.serdes;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ProductOrder;
import io.confluent.developer.avro.ProductOrder.Builder;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.rocksdb.CompactRangeOptions;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;

public class SRTopicLoader {
    static final Random random = new Random();
    public static void main(String[] args) throws IOException {
        runProducer();
    }

     public static void runProducer() throws IOException {
        //Schema Registry properties placed in property file as shown in instructions
        Properties properties = StreamsUtils.loadProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        try(Admin adminClient = Admin.create(properties);
            Producer<String, ProductOrder> producer = new KafkaProducer<>(properties)) {
            final String inputTopic = properties.getProperty("sr.input.topic");
            final String outputTopic = properties.getProperty("sr.output.topic");
            var topics = List.of(StreamsUtils.createTopic(inputTopic), StreamsUtils.createTopic(outputTopic));
            adminClient.createTopics(topics);

            Callback callback = (metadata, exception) -> {
                if(exception != null) {
                    System.out.printf("Producing records encountered error %s %n", exception);
                } else {
                    System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
                }

            };

            Builder productOrderBuilder = ProductOrder.newBuilder();

            var orderIdList = List.of("orderNumber-1001",
                                               "orderNumber-5000",
                                               "orderNumber-999",
                                               "orderNumber-3330",
                                               "bogus-1",
                                               "bogus-2",
                                               "orderNumber-8400");

            var productOrders = orderIdList.stream().map(orderId-> productOrderBuilder.setOrderId(orderId)
                    .setProduct("HDTV-2333")
                    .setUserId("user-" + random.nextInt(100))
                    .setTime(Instant.now().toEpochMilli())
                    .build()).collect(Collectors.toList());
            var producerRecords = productOrders.stream().map(r -> new ProducerRecord<>(inputTopic,"order-key", r)).collect(Collectors.toList());
            producerRecords.forEach((pr -> producer.send(pr, callback)));
        }
    }
}

