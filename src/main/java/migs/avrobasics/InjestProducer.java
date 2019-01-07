package migs.avrobasics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class InjestProducer {

    private static final String dirPath = "/shakespeare";
    private static final String topic = "shakespeare_topic";

    private KafkaProducer<String, String> producer;

    public InjestProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        producer = new KafkaProducer<>(p);

    }

    public void startLoading() throws Exception {
        List<Path> fileList = getShakespeareFiles();

        fileList.forEach( f -> {
            String fn = f.getFileName().toString();
            fn = fn.substring(0, fn.lastIndexOf("."));

            System.out.printf("File = %s\n", fn);

            try {
                List<String> lines = Files.readAllLines(f);
                System.out.printf("\tTotal lines: %d\n", lines.size());

                loadTopic(fn, lines);

            } catch (IOException e) {
                e.printStackTrace();
            }

        });
    }

    private void loadTopic(String fileName, List<String> lines){
        System.out.printf("\tSending: FN = %s | Total lines: %d\n", fileName, lines.size());

        lines.forEach( ln -> {
            ProducerRecord<String, String> pr = new ProducerRecord<>(topic, fileName, ln);
            producer.send(pr);
        });

        System.out.println("\tSending complete\n");
    }

    private List<Path> getShakespeareFiles() throws Exception {
        return Files.list(Paths.get(this.getClass().getResource(dirPath).toURI())).collect(Collectors.toList());
    }

    public void cleanup(){
        producer.close();
    }

    public static void main(String[] args) throws Exception {
        InjestProducer ip = new InjestProducer();
        ip.startLoading();
    }
}
