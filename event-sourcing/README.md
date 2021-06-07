# Event Sourcing

## Example Instructions
1. Log into confluent cloud. 
1. Create a topic called `shopping-cart` to act as your event log
1. Run the shopping cart app with `./gradlew run --args="configuration/dev.properties"`
1. In another terminal produce some sample input records (TODO)

## Notes
The avro console consumer works to read the events (requires an AK distribution locally)
```
kafka-avro-console-consumer --bootstrap-server localhost:29092 --topic shopping-cart --from-beginning
```

## ksqlDB Version
```
CREATE STREAM purchases (customer VARCHAR, item VARCHAR, qty INT
  WITH (kafka_topic='purchases-topic', value_format='json', partitions=1);

INSERT INTO purchases (customer, item, qty) VALUES ('jsmith', 'hats', 1);
INSERT INTO purchases (customer, item, qty) VALUES ('jsmith', 'hats', 1);
INSERT INTO purchases (customer, item, qty) VALUES ('jsmith', 'pants', 1);
INSERT INTO purchases (customer, item, qty) VALUES ('jsmith', 'sweaters', 1);
INSERT INTO purchases (customer, item, qty) VALUES ('jsmith', 'pants', 1);
INSERT INTO purchases (customer, item, qty) VALUES ('jsmith', 'pants', -1);

-- Test/Visualize the data in the stream
SELECT * FROM purchases WHERE customer = 'jsmith' EMIT CHANGES;

CREATE TABLE customer_purchases WITH (KEY_FORMAT='JSON') AS
  SELECT customer, item, SUM(qty) as total_qty from purchases GROUP BY customer, item emit changes;

-- Test/Visualize the data in the aggregate stream
SELECT * FROM customer_purchases EMIT CHANGES;
```

