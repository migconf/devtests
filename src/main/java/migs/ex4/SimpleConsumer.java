package migs.ex4;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "migs-cons-grp");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        KafkaConsumer<String, String> kConsumer = new KafkaConsumer<>(props);
        kConsumer.subscribe(Arrays.asList("migs-producer-1"));

        try {
            listenerLoop:
                while(true){
                    System.out.println("About to Poll...");
                    ConsumerRecords<String, String> rex = kConsumer.poll(Duration.ofSeconds(5));

                    for(ConsumerRecord<String, String> rec : rex){
                        if(rec.value().equals("break"))
                            break listenerLoop;

                        System.out.printf("Offset: %d | Key: %s | Val: %s\n", rec.offset(), rec.key(), rec.value());
                    }
                }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kConsumer.close();
        }
    }

}
