package io.confluent.developer;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamsUtils {

    public static final String PROPERTIES_FILE_PATH = "src/main/resources/streams.properties";
    public static final short REPLICATION_FACTOR = 3;
    public static final int PARTITIONS = 6;

    public static Properties loadProperties() throws IOException {
            Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            properties.load(fis);
            return properties;
        }
    }
   public static Map<String,Object> propertiesToMap(final Properties properties) {
        final Map<String, Object> configs = new HashMap<>();
        properties.forEach((key, value) -> configs.put((String)key, (String)value));
        return configs;
    }

    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, Object> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

    public static Callback callback() {
        return (metadata, exception) -> {
            if(exception != null) {
                System.out.printf("Producing records encountered error %s %n", exception);
            } else {
                System.out.printf("Record produced - offset - %d timestamp - %d %n", metadata.offset(), metadata.timestamp());
            }

        };
    }

    public static NewTopic createTopic(final String topicName){
              return new NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR);
    }
}
