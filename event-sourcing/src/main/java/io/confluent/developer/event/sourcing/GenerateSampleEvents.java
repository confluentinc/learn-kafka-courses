package io.confluent.developer.event.sourcing;

import io.confluent.developer.event.sourcing.avro.ShoppingCartAction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class GenerateSampleEvents {

    private static Future<RecordMetadata> produce(
            Producer<String, ShoppingCartAction> producer, ShoppingCartAction event) {
        final ProducerRecord<String, ShoppingCartAction> producerRecord = new ProducerRecord<>(
                ShoppingCartApp.SHOPPING_CART_EVENT_TOPIC_NAME, event.getCustomer(), event);
        return producer.send(producerRecord);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "This program takes one argument: the path to an environment configuration file.");
        }

        final Properties props = ShoppingCartApp.loadEnvProperties(args[0]);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        try (final Producer<String, ShoppingCartAction> producer = new KafkaProducer<String, ShoppingCartAction>(props)) {
            List.of(
                    new ShoppingCartAction("yeva", "trousers", "add"),
                    new ShoppingCartAction("bill", "trousers", "add"),
                    new ShoppingCartAction("yeva", "trousers", "add"),
                    new ShoppingCartAction("yeva", "jumpers", "add"),
                    new ShoppingCartAction("bill", "trousers", "add"),
                    new ShoppingCartAction("bill", "jumpers", "add"),
                    new ShoppingCartAction("yeva", "trousers", "remove"),
                    new ShoppingCartAction("yeva", "hat", "add"),
                    new ShoppingCartAction("bill", "trousers", "remove"),
                    new ShoppingCartAction("yeva", "", "checkout"),
                    new ShoppingCartAction("bill", "hat", "add"),
                    new ShoppingCartAction("bill", "", "checkout")
            ).forEach( e -> produce(producer, e));

            producer.flush();
        }

    }
}