# Module 6 / Exercise 1 - Streaming Data to External Systems

This exercise is the culmination of a pipeline project that takes streams of ratings events, filters them, and enriches them with information about the customer using data streamed from a database.

![Data Lineage](images/dp06-01-01.png)

For the final step, we will stream the enriched data out to Elasticsearch from where it can be built into a dashboard. You need to have an Elasticsearch instance created as described in the first exercise &lt;TODO: INSERT NAME OF EXERCISE AND ADD ANCHOR LINK&gt;, and it must be accessible from the internet.

1.  In Confluent Cloud, click on the **Connectors** link, click **Add connector**, and search for the "Elasticsearch Service Sink" connector.

    ![Elasticsearch sink connector in Confluent Cloud](images/dp06-01-02.png)

    Click on the tile to create the sink connector.

2.  Configure the connector as follows. You can leave blank any options that are not specified below.

    <table><caption>Elasticsearch sink configuration options</caption><colgroup><col style="width: 50%" /><col style="width: 50%" /></colgroup><tbody><tr class="odd"><td style="text-align: left;" colspan="2"><p><strong>Which topics do you want to get data from?</strong></p></td></tr><tr class="even"><td style="text-align: left;"><p>topics</p></td><td style="text-align: left;"><p><code>ratings-enriched</code></p></td></tr><tr class="odd"><td style="text-align: left;" colspan="2"><p><strong>Input messages</strong></p></td></tr><tr class="even"><td style="text-align: left;"><p>Input message format</p></td><td style="text-align: left;"><p>AVRO</p></td></tr><tr class="odd"><td style="text-align: left;" colspan="2"><p><strong>Kafka Cluster credentials</strong></p></td></tr><tr class="even"><td style="text-align: left;"><p>Kafka API Key</p></td><td style="text-align: left;"><p><em>Use the same API details as you created for the Datagen connector previously. You can create a new API key if necessary, but API key numbers are limited so for the purposes of this exercise only it’s best to re-use if you can.</em></p></td></tr><tr class="odd"><td style="text-align: left;"><p>Kafka API Secret</p></td><td></td></tr><tr class="even"><td style="text-align: left;" colspan="2"><p><strong>How should we connect to your Elasticsearch Service?</strong></p></td></tr><tr class="odd"><td style="text-align: left;"><p>Connection URI</p></td><td style="text-align: left;"><p><em>These values will depend on where your Elasticsearch instance is and how you have configured it. Elasticsearch needs to be open to inbound connections from the internet.</em></p></td></tr><tr class="even"><td style="text-align: left;"><p>Connection username</p></td><td></td></tr><tr class="odd"><td style="text-align: left;"><p>Connection password</p></td><td></td></tr><tr class="even"><td style="text-align: left;" colspan="2"><p><strong>Data Conversion</strong></p></td></tr><tr class="odd"><td style="text-align: left;"><p>Type name</p></td><td style="text-align: left;"><p><code>_doc</code></p></td></tr><tr class="even"><td style="text-align: left;"><p>Key ignore</p></td><td style="text-align: left;"><p><code>true</code></p></td></tr><tr class="odd"><td style="text-align: left;"><p>Schema ignore</p></td><td style="text-align: left;"><p><code>true</code></p></td></tr><tr class="even"><td style="text-align: left;" colspan="2"><p><strong>Connection Details</strong></p></td></tr><tr class="odd"><td style="text-align: left;"><p>Batch size</p></td><td style="text-align: left;"><p><code>5</code> <em>(this is a setting only suitable for this exercise; in practice you would leave it as the default or set it much higher for performance reasons).</em></p></td></tr><tr class="even"><td style="text-align: left;" colspan="2"><p><strong>Number of tasks for this connector</strong></p></td></tr><tr class="odd"><td style="text-align: left;"><p>Tasks</p></td><td style="text-align: left;"><p>1</p></td></tr></tbody></table>

    Elasticsearch sink configuration options

    Click **Next** to test the connection and validate the configuration.

3.  On the next screen, the JSON configuration should be similar to that shown below. If it is not, return to the previous screen to amend it as needed.

        {
          "name": "ElasticsearchSinkConnector_0",
          "config": {
            "topics": "ratings-enriched",
            "input.data.format": "AVRO",
            "connector.class": "ElasticsearchSink",
            "name": "ElasticsearchSinkConnector_0",
            "kafka.api.key": "****************",
            "kafka.api.secret": "****************************************************************",
            "connection.url": "https://es-host:port",
            "connection.username": "elastic",
            "connection.password": "************************",
            "type.name": "_doc",
            "key.ignore": "true",
            "schema.ignore": "true",
            "batch.size": "5",
            "tasks.max": "1"
          }
        }

    Click **Launch**.

4.  After a few moments, the connector will be provisioned and shortly thereafter you should see that it is "Running" (alongside the existing connectors that you created in previous exercises):

    ![All three connectors running](images/dp06-01-03.png)

5.  In Elasticsearch, check that data has been received in the index. You can do this using the REST API or with Kibana itself. Here’s an example using `curl` to do it:

        curl -u $ES_USER:$ES_PW $ES_ENDPOINT/_cat/indices/ratings\*\?v=true
        health status index            uuid                   pri rep docs.count docs.deleted store.size pri.store.size
        green  open   ratings-enriched Wj-o_hEwR8ekHSF7M7aVug   1   1     101091            0     12.1mb            6mb

    Note that the `docs.count` value should be above zero.

6.  You can now use the data. In our example, we’re streaming it to Elasticsearch so as to be able to build an operational dashboard using Kibana. The following assumes that you are familiar with the use of Kibana.

    -   In Kibana, create an index pattern for the `ratings-enriched` index, with `RATING_TS` as the time field.

        ![Creating a Kibana index pattern](images/dp06-01-04.png)

    -   Use the **Discover** view to explore the data and its characteristics

        ![Kibana Discover view](images/dp06-01-05.png)

        Create visualizations to build a dashboard showing relevant details in the data.

        ![Kibana Dashboard](images/dp06-01-06.png)

_Make sure that when you have finished the exercises in this course you use the Confluent Cloud UI or CLI to destroy all the resources you created. Verify they are destroyed to avoid unexpected charges._
