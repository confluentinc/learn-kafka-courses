# Event Sourcing

## Example Instructions
1. Log into confluent cloud. 
1. Create a topic called shopping cart to act as your event log
1. Create a simple schema with the fields: {id, item-name, customer [maybe?], action}
1. sing the console producer produce the following messages to the event log.
    * Add Trousers  
    * Add Trousers  
    * Add Jumper
    * Remove trousers
    * Add hat
    * Checkout
1. Create a new program in your favourite language, we’ll use Java / Kafka Streams, that reads the contents of the shopping cart topic and writes it to the console. 
1. Now reduce the items by id. You should get the output: 1 trousers, 1 jumper, 1 hat. i.e. the table (store it in a HashTable or similar data structure)
Exercise 2
1. Create a CQRS version of this that materializes the basket table in ksqlDB so it can be queried 

