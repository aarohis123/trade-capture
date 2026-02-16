package com.assn.tcap.ingestor.service;

import com.assn.tcap.ingestor.model.RejectedTradeDTO;
import com.assn.tcap.ingestor.model.TradeDTO;
import org.modelmapper.ModelMapper;

import java.time.LocalDate;
import java.util.*;


public interface ITradeService {

    List<RejectedTradeDTO> processTrades(List<TradeDTO> incomingTrades);

    List<TradeDTO> getAllTrades();

    default Map<Long, TradeDTO> validateTrades(List<TradeDTO> incomingTrades, List<RejectedTradeDTO> rejectedTrades, LocalDate today){
        // Maintain order & keep highest version per tradeId in this batch
        Map<Long, TradeDTO> highestVersionInBatch = new LinkedHashMap<>();
        for (TradeDTO trade : incomingTrades) {

            if (trade.getMaturityDate().isBefore(today)) {
//                log.error("TRADE REJECTED: Maturity date expired for TradeEntity {}",trade);
                rejectedTrades.add(
                        getRejectedTrade(trade,
                                "Maturity date expired: " + trade.getMaturityDate()));
                continue;
            }

            TradeDTO existing = highestVersionInBatch.get(trade.getTradeId());

            if (existing == null) {
                highestVersionInBatch.put(trade.getTradeId(), trade);
            } else {
                if (trade.getVersion() >= existing.getVersion()) {
                    highestVersionInBatch.put(trade.getTradeId(), trade);
                } else {
//                    log.error("TRADE REJECTED: Lower version in same batch {}",trade);
                    rejectedTrades.add(
                            getRejectedTrade(trade,
                                    "Lower version in same batch: " + trade.getVersion()));
                }
            }
        }
        return highestVersionInBatch;
    }

    default RejectedTradeDTO getRejectedTrade(TradeDTO tradeDTO, String reason){
        ModelMapper mapper = new ModelMapper();
        RejectedTradeDTO rejectedTradeDTO = mapper.map(tradeDTO, RejectedTradeDTO.class);
        rejectedTradeDTO.setReason(reason);
        return rejectedTradeDTO;
    }
}
