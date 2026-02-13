package com.assn.tcap.ingestor.consumer;

import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.service.TradeService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.assn.tcap.ingestor.config.KafkaConfig.TOPIC_NAME;

@Service
@AllArgsConstructor
@Slf4j
public class TradeConsumer {

    private final TradeService tradeService;


    @KafkaListener(topics = TOPIC_NAME, containerFactory = "batchKafkaListenerContainerFactory")
    public void consume(List<TradeDTO> trades,
                        Acknowledgment acknowledgment) {
        log.info("Received {} trades in batch", trades.size());
        try {
            tradeService.processTrades(trades);
            acknowledgment.acknowledge();
        } catch (Exception ex) {
            log.error("Error processing trade", ex);
            throw ex;
        }
    }
}
