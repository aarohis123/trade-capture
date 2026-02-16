package com.assn.tcap.ingestor.service;

import com.assn.tcap.ingestor.entity.TradeEntity;
import com.assn.tcap.ingestor.model.RejectedTradeDTO;
import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.repo.TradeSQLRepo;
import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Transactional
@AllArgsConstructor
@Slf4j
public class SQLTradeService implements ITradeService{
    private final TradeSQLRepo tradeSQLRepo;

    @Override
    public List<RejectedTradeDTO> processTrades(List<TradeDTO> incomingTrades) {

        if (incomingTrades == null || incomingTrades.isEmpty()) {
            return Collections.emptyList();
        }

        LocalDate today = LocalDate.now();
        List<RejectedTradeDTO> rejectedTrades = new ArrayList<>();

        Map<Long, TradeDTO> highestVersionInBatch = validateTrades(incomingTrades,rejectedTrades,today);

        // Fetch latest trades from DB only for valid batch entries
        Map<Long, TradeEntity> existingTrades = tradeSQLRepo
                .findLatestTradesByTradeIds(highestVersionInBatch.keySet())
                .stream()
                .collect(Collectors.toMap(TradeEntity::getTradeId, t -> t));

        List<TradeEntity> tradesToSave = new ArrayList<>();

        for (TradeDTO tradeDTO : highestVersionInBatch.values()) {
            try {
                TradeEntity existing = existingTrades.get(tradeDTO.getTradeId());
                if (existing != null && tradeDTO.getVersion() < existing.getVersion()) {
                    log.error("TRADE REJECTED: Lower version of TradeEntity exist in store {}",tradeDTO);
                    rejectedTrades.add(
                            getRejectedTrade(tradeDTO,
                                    "Lower version than DB: " + tradeDTO.getVersion()));
                    continue;
                }
                TradeEntity tradeEntityToSave =tradeDTO.toEntity();
                if (existing != null &&
                        Objects.equals(existing.getVersion(), tradeDTO.getVersion())) {

                    tradeEntityToSave.setId(existing.getId());
                    tradeEntityToSave.setRowVersion(existing.getRowVersion());// Replace
                }
                tradesToSave.add(tradeEntityToSave);
            } catch (Exception e) {
                log.error("Error parsing trade {}", tradeDTO);
                rejectedTrades.add(
                        getRejectedTrade(tradeDTO,
                                "Processing error: " + e.getMessage()));
            }finally {
                rejectedTrades.forEach(r-> log.error("TRADE REJECTED: {}, Trade:{}",r.getReason(),r ));
            }
        }

        try {
            tradeSQLRepo.saveAll(tradesToSave);
        } catch (ObjectOptimisticLockingFailureException e) {
            log.error("Optimistic locking failure: {}", e.getMessage());
            throw e;
        }
        return rejectedTrades;
    }



    public List<TradeDTO> getAllTrades() {
        List<TradeEntity> tradeEntities = tradeSQLRepo.findAll();
        return tradeEntities.stream().map(TradeEntity::toDTO).toList();
    }
}
