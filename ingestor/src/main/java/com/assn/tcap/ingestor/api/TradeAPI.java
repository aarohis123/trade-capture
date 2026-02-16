package com.assn.tcap.ingestor.api;

import com.assn.tcap.ingestor.model.RejectedTradeDTO;
import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.producer.TradeProducer;
import com.assn.tcap.ingestor.service.NoSQLTradeService;
import com.assn.tcap.ingestor.service.SQLTradeService;
import lombok.AllArgsConstructor;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.assn.tcap.ingestor.util.TradePayloadGenerator.getPayload;

@RestController
@AllArgsConstructor
public class TradeAPI {

    private final SQLTradeService sqlTradeService;
    private final NoSQLTradeService noSQLTradeService;

    private final TradeProducer tradeProducer;

    @GetMapping("/sql/getAllTrades")
    public List<TradeDTO> getAllTrades(){
        return sqlTradeService.getAllTrades();
    }

    @GetMapping("/nosql/getAllTrades")
    public List<TradeDTO> getNoSqlTrades(){
        return noSQLTradeService.getAllTrades();
    }

    @PostMapping("/publishTrades")
    public String sendTrades(@RequestBody List<TradeDTO> incomingTrades){
        incomingTrades.forEach(tradeProducer::sendMessage);
        return "Success";
    }

    @GetMapping("/publishAutoTrades")
    public String publishTestTrades(){
        var payload = getPayload();
        tradeProducer.sendMessages(payload);
        return "Success";
    }


}
