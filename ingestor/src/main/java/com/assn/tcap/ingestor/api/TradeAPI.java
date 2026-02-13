package com.assn.tcap.ingestor.api;

import com.assn.tcap.ingestor.model.RejectedTradeDTO;
import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.producer.TradeProducer;
import com.assn.tcap.ingestor.service.TradeService;
import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@AllArgsConstructor
public class TradeAPI {

    private final TradeService tradeService;

    private final TradeProducer tradeProducer;


    @GetMapping("/getAllTrades")
    public List<TradeDTO> getAllTrades(){
        return tradeService.getAllTrades();
    }

    @PostMapping("/saveTrades")
    public List<RejectedTradeDTO> saveTrades(@RequestBody List<TradeDTO> tradeDTOList){
        return tradeService.processTrades(tradeDTOList);
    }

    @PostMapping("/sendTrades")
    public String sendTrades(@RequestBody List<TradeDTO> incomingTrades){
        incomingTrades.forEach(tradeProducer::sendMessage);
        return "Success";
    }


}
