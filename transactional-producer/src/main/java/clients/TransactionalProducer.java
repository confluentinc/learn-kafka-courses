/**
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.TopicExistsException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;

public class TransactionalProducer {

  // Create topic in Confluent Cloud
  public static void createTopic(final String topic,
                          final Properties cloudConfig) {
      final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
      try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
          adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
      } catch (final InterruptedException | ExecutionException e) {
          // Ignore if TopicExistsException, which may be valid if topic exists
          if (!(e.getCause() instanceof TopicExistsException)) {
              throw new RuntimeException(e);
          }
      }
  }

  public static void main(final String[] args) throws IOException {

		Scanner in = new Scanner(System.in);

    if (args.length != 3) {
      System.out.println("Please provide command line arguments: configPath topic transactional_id");
      System.exit(1);
    }

    // Load properties from a local configuration file
    // Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
    // to connect to your Kafka cluster, which can be on your local host, Confluent Cloud, or any other cluster.
    // Follow these instructions to create this file: https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/java.html
    final Properties props = loadConfig(args[0]);

    // Create topic if needed
    final String topic = args[1];
    createTopic(topic, props);

    // Add additional properties.
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put("transactional.id", args[2]);
    props.put("transaction.timeout.ms", "20000");

    Producer<String, String> producer = new KafkaProducer<>(props);

    producer.initTransactions();

    try {
	  producer.beginTransaction();
	  System.out.println("*** Begin Transaction ***");
	  System.out.printf("*** transactional.id %s ***\n", props.get("transactional.id"));
	
	  for (int i = 0; i < 5; i++) {
		double randomDouble = Math.random();
		int randomNum = (int) (randomDouble * 10);
		producer.send(new ProducerRecord<>("transaction-topic", Integer.toString(i),
		Integer.toString(randomNum)));
		System.out.printf("Sent %s:%s\n", i, randomNum);
		in.nextLine();
	  }
	  producer.commitTransaction();
	  System.out.println("*** Commit Transaction ***");
    }
    catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
	// We can't recover from these exceptions, so our only option is to close the producer and exit.
	  System.out.println(e.toString());
	  producer.close();
    }
    catch (KafkaException e) {
	// For all other exceptions, just abort the transaction and try again.
	  System.out.println(e.toString());
	  producer.abortTransaction();
    }
    in.close();
    producer.close();
  }

  public static Properties loadConfig(final String configFile) throws IOException {
    if (!Files.exists(Paths.get(configFile))) {
      throw new IOException(configFile + " not found.");
    }
    final Properties cfg = new Properties();
    try (InputStream inputStream = new FileInputStream(configFile)) {
      cfg.load(inputStream);
    }
    return cfg;
  }

}
