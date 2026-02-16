package com.assn.tcap.ingestor.service;

import com.assn.tcap.ingestor.entity.TradeDocument;
import com.assn.tcap.ingestor.model.RejectedTradeDTO;
import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.repo.TradeMongoRepository;
import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Service
@AllArgsConstructor
@Transactional
@Slf4j
public class NoSQLTradeService implements ITradeService{

    private final TradeMongoRepository mongoRepository;

    @Override
    public List<RejectedTradeDTO> processTrades(List<TradeDTO> incomingTrades) {
        if (incomingTrades == null || incomingTrades.isEmpty()) {
            return Collections.emptyList();
        }


        LocalDate today = LocalDate.now();
        List<RejectedTradeDTO> rejectedTrades = new ArrayList<>();

        Map<Long, TradeDTO> highestVersionInBatch = validateTrades(incomingTrades,rejectedTrades,today);

        Map<Long, TradeDocument> existingTrades = mongoRepository
                .findLatestByTradeIds(highestVersionInBatch.keySet())
                .stream()
                .collect(Collectors.toMap(TradeDocument::getTradeId, t -> t));

        List<TradeDocument> tradesToSave = new ArrayList<>();
        for (TradeDTO tradeDTO : highestVersionInBatch.values()) {
            try {
                TradeDocument existing = existingTrades.get(tradeDTO.getTradeId());
                if (existing != null && tradeDTO.getVersion() < existing.getVersion()) {
                    log.error("TRADE REJECTED NO-SQL: Lower version of TradeEntity exist in store {}",tradeDTO);
                    rejectedTrades.add(
                            getRejectedTrade(tradeDTO,
                                    "NO-SQL - Lower version than DB: " + tradeDTO.getVersion()));
                    continue;
                }
                TradeDocument tradeDocumentToSave = tradeDTO.toDocument();
                if (existing != null &&
                        Objects.equals(existing.getVersion(), tradeDTO.getVersion())) {
                    tradeDocumentToSave.setId(existing.getId());
                }
                tradesToSave.add(tradeDocumentToSave);
            } catch (Exception e) {
                log.error("NO-SQL - Error parsing trade {}", tradeDTO);
                rejectedTrades.add(
                        getRejectedTrade(tradeDTO,
                                "NO-SQL Processing error: " + e.getMessage()));
            }finally {
                rejectedTrades.forEach(r-> log.error("NO-SQL - TRADE REJECTED: {}, Trade:{}",r.getReason(),r ));
            }
        }

        try {
            mongoRepository.bulkUpsertWithVersionHandling(tradesToSave);
        } catch (Exception e) {
            log.error("NO-SQL - Bulk Update failure: {}", e.getMessage());
            throw e;
        }
        return rejectedTrades;
    }

    @Override
    public List<TradeDTO> getAllTrades() {
        var documents =  mongoRepository.findAll();
            return    documents.stream().map(TradeDocument::toDTO).toList();
    }
}
