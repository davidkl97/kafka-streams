package streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColour {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> favoriteColourInput = builder.stream("favorite-colour-input");

        //to lower case
        favoriteColourInput
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((key, value) -> Arrays.asList("red", "blue", "green").contains(value))
                .to("favorite-colour-user");


        KTable<String, String> favoriteColourUser = builder.table("favorite-colour-user");
        favoriteColourUser
                .groupBy((key, value) -> new KeyValue<>(value, value))
                .count()
                .toStream()
                .to("favorite-colour-output");



        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), config);
        kafkaStreams.start();
        System.out.println(kafkaStreams.toString());


        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }
}
