package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Date;
import java.util.Properties;

/**
 * Created by johndavis on 25.08.17.
 */
public class Demo {

    public static void main(String[] args) {
        System.out.println("Starting things up");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                StringSerializer.class);
        props.put("value.serializer",
                StringSerializer.class);

        Producer<String, String> producer = new KafkaProducer<>(props);

        createEvents(producer);

        producer.close();

        KStreamBuilder builder = new KStreamBuilder();

        System.out.println("this is doing stuff");

        KTable<String, String> allEvents = builder.stream(Serdes.String(), Serdes.String(), "streams-demo-all-events")
                .map((key, value) -> {
                    String extractedKey = value.split(",")[0].trim();
                    String extractedValue = value.split(",")[1].trim();
                    System.out.println("the key: " + extractedKey + "the value: " + extractedValue);
                    return KeyValue.pair(extractedKey, extractedValue);
                })
                .groupByKey()
                .aggregate(
                        () -> "",
                        (key, value, aggr) -> value,
                        Serdes.String(),
                        "streams-demo-all-v3"
                );

        System.out.println("the end of stuff");

        KTable<String, String> fraudEvents = builder.stream(Serdes.String(), Serdes.String(), "streams-demo-fraud-events")
                .map((key, value) -> {
                    String extractedKey = value.split(",")[0].trim();
                    String extractedValue = value.split(",")[1].trim();
                    System.out.println("the key: " + extractedKey + "the value: " + extractedValue);
                    return KeyValue.pair(extractedKey, extractedValue);
                })
                .groupByKey()
                .aggregate(
                        () -> "",
                        (key, value, aggr) -> value,
                        Serdes.String(),
                        "streams-demo-fraud-v3"
                );

        fraudEvents
                .leftJoin(allEvents, (fraudgarbage, country) -> country)
                .filter((key, value) -> {
            System.out.println("this is a hit: " + key + ": " +value);
            return true;})
                .toStream()
                .to(Serdes.String(), Serdes.String(), "streams-demo-joined-topic");

        KStream<String, Long> count = builder.stream(Serdes.String(), Serdes.String(), "streams-demo-joined-topic")
                .groupBy((adId, country) -> country)
                .count("Counts").toStream();

        count.to(Serdes.String(), Serdes.Long(), "streams-demo-country-count");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        try {
            Thread.sleep(15000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        streams.close();

        System.out.println("basic stream processing has ended");
    }

    private static void createEvents(Producer<String, String> producer) {
        String id1 = String.valueOf(new Date().getTime());
        String allEvents = "streams-demo-all-events";
        String fraudEvents = "streams-demo-fraud-events";
        producer.send(new ProducerRecord<String, String>(allEvents, id1 + ", thailand"));

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String id2 = String.valueOf(new Date().getTime());
        producer.send(new ProducerRecord<String, String>(allEvents, id2 + ", germany"));

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String id3 = String.valueOf(new Date().getTime());
        producer.send(new ProducerRecord<String, String>(allEvents, id3 + ", germany"));

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String id4 = String.valueOf(new Date().getTime());
        producer.send(new ProducerRecord<String, String>(allEvents, id4 + ", usa"));
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.send(new ProducerRecord<String, String>(allEvents,  String.valueOf(new Date().getTime()) + ", portugal"));

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String id5 = String.valueOf(new Date().getTime());
        producer.send(new ProducerRecord<String, String>(allEvents, id5 + ", cambodia"));

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String id6 = String.valueOf(new Date().getTime());
        producer.send(new ProducerRecord<String, String>(allEvents, id6 + ", cambodia"));

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.send(new ProducerRecord<String, String>(allEvents, String.valueOf(new Date().getTime()) + ", cambodia"));

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.send(new ProducerRecord<String, String>(allEvents,String.valueOf(new Date().getTime()) + ", germany"));

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.send(new ProducerRecord<String, String>(allEvents,String.valueOf(new Date().getTime()) + ", germany"));

        //this sends all the fraud ads
        producer.send(new ProducerRecord<String, String>(fraudEvents,id1 + ", garbage"));
        producer.send(new ProducerRecord<String, String>(fraudEvents,id2 + ", garbage"));
        producer.send(new ProducerRecord<String, String>(fraudEvents,id3 + ", garbage"));
        producer.send(new ProducerRecord<String, String>(fraudEvents,id3 + ", garbage"));
        producer.send(new ProducerRecord<String, String>(fraudEvents,id3 + ", garbage"));
        producer.send(new ProducerRecord<String, String>(fraudEvents,id4 + ", garbage"));
        producer.send(new ProducerRecord<String, String>(fraudEvents,id5 + ", garbage"));
        producer.send(new ProducerRecord<String, String>(fraudEvents,id6 + ", garbage"));
    }
}
