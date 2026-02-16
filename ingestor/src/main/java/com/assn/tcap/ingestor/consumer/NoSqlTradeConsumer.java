package com.assn.tcap.ingestor.consumer;

import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.service.NoSQLTradeService;
import com.assn.tcap.ingestor.service.SQLTradeService;
import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.assn.tcap.ingestor.config.KafkaConfig.TOPIC_NAME;

@Slf4j
@Service
@Transactional
@AllArgsConstructor
public class NoSqlTradeConsumer {

    private final NoSQLTradeService noSQLTradeService;

    @KafkaListener(topics = TOPIC_NAME, groupId = "nosql-trade-group", containerFactory = "batchKafkaListenerContainerFactory")
    public void consume(List<TradeDTO> trades,
                        Acknowledgment acknowledgment) {
        log.info("Received {} NoSQL trades in batch", trades.size());
        try {
            noSQLTradeService.processTrades(trades);
            acknowledgment.acknowledge();
        } catch (Exception ex) {
            log.error("NoSQL Error processing trade", ex);
            throw ex;
        }
    }
}
