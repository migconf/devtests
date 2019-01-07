package migs.avrobasics;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import solution.model.ShakespearKey;
import solution.model.ShakespearValue;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ShakespeareAvroConverter {

    private static String brokerHost = "localhost:9092";
    private static String groupId = "shakespear_std_read_grp";
    private static String readTopic = "shakespeare_topic";
    private static String schemaRegUrl = "http://localhost:8081";
    private static String avroTopic = "shakespeare_avro_topic";

    private KafkaConsumer<String, String> stdConsumer;
    private KafkaProducer<Object, Object> avroProducer;

    private int TOTAL_CHECKS = 3;

    private static Map<String, Integer> yearMap;

    static {
        yearMap = new HashMap<>();
        yearMap.put("Hamlet", 1600);
        yearMap.put("Julius Caesar", 1599);
        yearMap.put("Macbeth", 1605);
        yearMap.put("Merchant of Venice", 1596);
        yearMap.put("Othello", 1604);
        yearMap.put("Romeo and Juliet", 1594);
    }

    public ShakespeareAvroConverter() {
        Properties cp = new Properties();
        cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHost);
        cp.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        stdConsumer = new KafkaConsumer(cp);

        Properties ap = new Properties();
        ap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHost);
        ap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        ap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        ap.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegUrl);

        avroProducer = new KafkaProducer<Object, Object>(ap);
    }

    public void readTopic(){
        ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {}

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                stdConsumer.seekToBeginning(collection);
            }
        };

        stdConsumer.subscribe(Arrays.asList(readTopic), rebalanceListener);
        stdConsumer.poll(Duration.ZERO);

        AtomicInteger totalMessages = new AtomicInteger(0);
        int x = 0;

        while(true){
            ConsumerRecords<String, String> recList = stdConsumer.poll(Duration.ofSeconds(1));

            if(recList.count() == 0 && x++ >= TOTAL_CHECKS){
                break;
            }
            else {
//                System.out.printf("Records Retrieved: %d\n", recList.count());

                recList.forEach(rec -> {
//                    System.out.printf("K: %s | Prt: %d | V: %s\n", rec.key(), rec.partition(), rec.value());
                    ShakespearKey key = new ShakespearKey(rec.key(), yearMap.get(rec.key().trim()));

                    String[] lnParts = rec.value().trim().split(" ");
                    String lineNum = lnParts[0];
                    String line = "";

                    if(lnParts.length > 1)
                        line = String.join(" ", Arrays.copyOfRange(lnParts, 1, lnParts.length-1));

                    ShakespearValue val = new ShakespearValue(Integer.valueOf(lineNum), line.trim());
                    publishAvro(key, val);

                    totalMessages.incrementAndGet();
                });
            }
        }

        System.out.printf("Total Read: %d\n", totalMessages.get());
        stdConsumer.close();
        avroProducer.close();
    }

    public void publishAvro(ShakespearKey key, ShakespearValue val){
//        System.out.printf("k: %s | v: %s\n", key.toString(), val.toString());
        ProducerRecord<Object, Object> avroPR = new ProducerRecord<>(avroTopic, key, val);
        avroProducer.send(avroPR);
    }

    public static void main(String[] args) {
        ShakespeareAvroConverter sac = new ShakespeareAvroConverter();
        sac.readTopic();
    }
}
