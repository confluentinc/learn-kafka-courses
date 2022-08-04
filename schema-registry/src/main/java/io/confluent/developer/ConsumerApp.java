package io.confluent.developer;

import io.confluent.developer.proto.PurchaseProto.Purchase;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConsumerApp {

     public void consumePurchaseEvents() {
         Properties properties = loadProperties();
         Map<String, Object> consumerConfigs = new HashMap<>();
         properties.forEach((key, value) -> consumerConfigs.put((String) key, value));
         consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-registry-course-consumer");
         consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

         consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, <Replace this with key deserializer class>);
         consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, <Replace this with value deserializer class>);
         consumerConfigs.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, Purchase.class );
         
         // Obvious duplication but this is done to emphasize what's needed to use SchemaRegistry
         consumerConfigs.put("basic.auth.credentials.source", "USER_INFO");
         consumerConfigs.put("schema.registry.url", "<Replace this with schema.registry.url value from confluent.properties>");
         consumerConfigs.put("basic.auth.user.info", "<Replace this with basic.auth.user.info value from confluent.properties>");

         try(Consumer<String, Purchase> consumer = new KafkaConsumer<>(consumerConfigs)){
             consumer.subscribe(Collections.singletonList("proto-purchase"));
             while (true) {
                 ConsumerRecords<String, Purchase> consumerRecords = consumer.poll(Duration.ofSeconds(2));
                 consumerRecords.forEach(consumerRecord -> {
                     Purchase purchase = consumerRecord.value();
                     System.out.print("Purchase details { ");
                     System.out.printf("Customer: %s, ", purchase.getCustomerId());
                     System.out.printf("Amount: %f, ", purchase.getAmount());
                     System.out.printf("Item: %s } %n", purchase.getItem());
                 });
             }
         }
     }

    Properties loadProperties() {
        try (InputStream inputStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream("confluent.properties")) {
            Properties props = new Properties();
            props.load(inputStream);
            return props;
        } catch (IOException exception) {
            throw new RuntimeException(exception);
        }
    }

    public static void main(String[] args) {
        ConsumerApp consumerApp = new ConsumerApp();
        consumerApp.consumePurchaseEvents();
    }
}
