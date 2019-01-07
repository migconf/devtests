package migs.ex4;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class FromStartConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "migs-cons-grp-two");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> kConsumer = new KafkaConsumer<>(props);

        ConsumerRebalanceListener crl = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {}

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                kConsumer.seekToBeginning(collection);
            }
        };

        kConsumer.subscribe(Arrays.asList("migs-producer-1"), crl);
        kConsumer.poll(Duration.ZERO);

        try {
            while(true){
                System.out.println("Polling...");
                kConsumer.poll(Duration.ofSeconds(5)).forEach( rec -> {
                    System.out.printf("OS: %d | PRT: %d | Key: %s | Val: %s\n", rec.offset(), rec.partition(), rec.key(), rec.value());
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kConsumer.close();
        }
    }

}
