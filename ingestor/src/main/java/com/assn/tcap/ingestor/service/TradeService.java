package com.assn.tcap.ingestor.service;

import com.assn.tcap.ingestor.entity.Trade;
import com.assn.tcap.ingestor.model.RejectedTradeDTO;
import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.repo.TradeRepo;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.modelmapper.record.RecordModule;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Transactional
@Slf4j
public class TradeService {
    private final TradeRepo tradeRepo;

    private final ModelMapper mapper;

    public TradeService(TradeRepo tradeRepo) {
        this.tradeRepo = tradeRepo;
        this.mapper = new ModelMapper();
        mapper.registerModule(new RecordModule());

    }

    public List<RejectedTradeDTO> processTrades(List<TradeDTO> incomingTrades) {

        if (incomingTrades == null || incomingTrades.isEmpty()) {
            return Collections.emptyList();
        }

        LocalDate today = LocalDate.now();

        // Maintain order & keep highest version per tradeId in this batch
        Map<Long, TradeDTO> highestVersionInBatch = new LinkedHashMap<>();
        List<RejectedTradeDTO> rejectedTrades = new ArrayList<>();

        for (TradeDTO trade : incomingTrades) {

            if (trade.getMaturityDate().isBefore(today)) {
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
                    rejectedTrades.add(
                            getRejectedTrade(trade,
                                    "Lower version in same batch: " + trade.getVersion()));
                }
            }
        }

        // Fetch latest trades from DB only for valid batch entries
        Map<Long, Trade> existingTrades = tradeRepo
                .findLatestTradesByTradeIds(highestVersionInBatch.keySet())
                .stream()
                .collect(Collectors.toMap(Trade::getTradeId, t -> t));

        List<Trade> tradesToSave = new ArrayList<>();

        for (TradeDTO tradeDTO : highestVersionInBatch.values()) {

            try {

                Trade existing = existingTrades.get(tradeDTO.getTradeId());

                if (existing != null && tradeDTO.getVersion() < existing.getVersion()) {
                    rejectedTrades.add(
                            getRejectedTrade(tradeDTO,
                                    "Lower version than DB: " + tradeDTO.getVersion()));
                    continue;
                }

                Trade tradeToSave = mapper.map(tradeDTO, Trade.class);

                if (existing != null &&
                        Objects.equals(existing.getVersion(), tradeDTO.getVersion())) {

                    tradeToSave.setId(existing.getId()); // Replace
                }

                tradeToSave.setCreatedDate(today);
                tradeToSave.setExpired("N");
                tradesToSave.add(tradeToSave);

            } catch (Exception e) {

                rejectedTrades.add(
                        getRejectedTrade(tradeDTO,
                                "Processing error: " + e.getMessage()));
            }
        }

        try {
            tradeRepo.saveAll(tradesToSave);
        } catch (ObjectOptimisticLockingFailureException e) {
            log.error("Optimistic locking failure: {}", e.getMessage());
            throw e;
        }
        return rejectedTrades;
    }

    private RejectedTradeDTO getRejectedTrade(TradeDTO incomingTrade, String reason) {
        RejectedTradeDTO rejectedTradeDTO = mapper.map(incomingTrade, RejectedTradeDTO.class);
        rejectedTradeDTO.setReason(reason);
        return rejectedTradeDTO;
    }

    public List<TradeDTO> getAllTrades() {
        List<Trade> trades = tradeRepo.findAll();
        return trades.stream().map(t -> mapper.map(t, TradeDTO.class)).toList();
    }
}
