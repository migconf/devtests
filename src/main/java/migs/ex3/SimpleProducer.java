package migs.ex3;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.stream.IntStream;

public class SimpleProducer {

    public static void main(String[] args) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProps);

        IntStream.range(20, 30).forEach( i -> {
            String msg = String.format("message_%s", i);
            System.out.println(msg);
            ProducerRecord<String, String> pr = new ProducerRecord<>("migs-producer-1", msg);

            kafkaProducer.send(pr);
        });

//        try {
//            Thread.sleep(5*1000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        kafkaProducer.close();
        System.out.println("Sending complete!");
    }

}
