package migs.streamBasics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class SimpleShakespeareStreamApp {

    public SimpleShakespeareStreamApp() {

        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-shakespeare-streams-app");
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    }

    public static void main(String[] args) {

    }
}
