package streams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG,"all");
        properties.put(ProducerConfig.RETRIES_CONFIG,"3");
        properties.put(ProducerConfig.LINGER_MS_CONFIG,"1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(properties);
        int i = 0;
        while (true){
            System.out.println("Producing batch: "+i);
            try {
                producer.send(newRandomTransaction("david"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("katie"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("ronnie"));
                Thread.sleep(100);
            }
            catch (InterruptedException e){
                break;
            }
            i++;
        }


    }

    public static ProducerRecord<String, JsonNode> newRandomTransaction(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        Integer amount = ThreadLocalRandom.current().nextInt(0,100);
        Instant now = Instant.now();
        transaction.put("name",name);
        transaction.put("amount",amount);
        transaction.put("time",now.toString());
        return new ProducerRecord<>("bank-transactions",name,transaction);
    }
}
