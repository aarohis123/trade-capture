package com.assn.tcap.ingestor.producer;

import com.assn.tcap.ingestor.model.TradeDTO;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.assn.tcap.ingestor.config.KafkaConfig.TOPIC_NAME;

@Service
@AllArgsConstructor
@Slf4j
public class TradeProducer {
    private final KafkaTemplate<String, TradeDTO> kafkaTemplate;


    public void sendMessages(List<TradeDTO> incomingTrades) {
        log.info("Sending trade to Kafka: {}", incomingTrades);
        incomingTrades.forEach(t->kafkaTemplate.send(TOPIC_NAME, t.getTradeId().toString(), t));
    }

    public void sendMessage(TradeDTO incomingTrade){
        kafkaTemplate.send(TOPIC_NAME, incomingTrade.getTradeId().toString(), incomingTrade)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("TradeEntity sent successfully. Topic: {}, Offset: {}, partition: {}", result.getRecordMetadata().topic(), result.getRecordMetadata().offset(),result.getRecordMetadata().partition());
                    } else {
                        log.error("Failed to send trade", ex);
                    }
                });
    }

}
