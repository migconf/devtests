package migs.avrobasics;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import solution.model.ShakespearKey;
import solution.model.ShakespearValue;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class ShakespeareAvroReader {

    private static String brokerHost = "localhost:9092";
    private static String groupId = "shakespear_avro_read_grp";
    private static String readTopic = "shakespeare_topic";
    private static String schemaRegUrl = "http://localhost:8081";
    private static String avroTopic = "shakespeare_avro_topic";

    private KafkaConsumer<ShakespearKey, ShakespearValue> avroConsumer;


    public ShakespeareAvroReader() {
        Properties p = new Properties();

        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHost);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        p.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegUrl);
        p.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        avroConsumer = new KafkaConsumer<>(p);
    }

    public void readTopic(){
        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {}

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                avroConsumer.seekToBeginning(collection);
            }
        };

        avroConsumer.subscribe(Arrays.asList(avroTopic), listener);
        avroConsumer.poll(Duration.ZERO);

        int x = 0;
        AtomicInteger totalRecords = new AtomicInteger(0);

        while(true){

            ConsumerRecords<ShakespearKey, ShakespearValue> recList = avroConsumer.poll(Duration.ofSeconds(1));

            if(recList.count() == 0 && x++ < 3)
                break;

            recList.forEach( rec -> {
                totalRecords.incrementAndGet();
                System.out.printf("K: %s | V: %s\n", rec.key(), rec.value());
            });
        }

        System.out.printf("Total Records: %d\n", totalRecords.get());
        avroConsumer.close();

        System.out.println("DONE");
    }

    public static void main(String[] args) {
        ShakespeareAvroReader sar = new ShakespeareAvroReader();
        sar.readTopic();
    }
}
