package streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

public class BankTransactionsProducerTests {
    @Test
    public void newRandomTranasctionsTest() throws JsonProcessingException {
        ProducerRecord<String,JsonNode> record = BankTransactionProducer.newRandomTransaction("klinberg");
        String key = record.key();
        JsonNode value = record.value();
        ObjectMapper mapper = new ObjectMapper();
        Assert.assertEquals(key,"klinberg");
    }

}
