package com.assn.tcap.ingestor.service;

import com.assn.tcap.ingestor.entity.Trade;
import com.assn.tcap.ingestor.model.RejectedTradeDTO;
import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.repo.TradeRepo;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.modelmapper.record.RecordModule;
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
        if (null == incomingTrades || incomingTrades.isEmpty()) {
            return Collections.emptyList();
        }
        LocalDate today = LocalDate.now();

        Map<Long, Trade> existingTrades = tradeRepo
                .findLatestTradesByTradeIds(
                        incomingTrades.stream()
                                .map(TradeDTO::getTradeId)
                                .toList()
                )
                .stream()
                .collect(Collectors.toMap(Trade::getTradeId, t -> t));

        List<Trade> tradesToSave = new ArrayList<>();
        List<RejectedTradeDTO> rejectedTrades = new ArrayList<>();

        for (TradeDTO incomingTradeDTO : incomingTrades) {

            try {

                // 🔴 Validation 2: Maturity date must not be in past
                if (incomingTradeDTO.getMaturityDate().isBefore(today)) {
                    rejectedTrades.add(getRejectedTrade(incomingTradeDTO, String.format("Maturity date expired: %s", incomingTradeDTO.getMaturityDate())));
                    log.error("REJECTED TRADE: Maturity date expired: {}", incomingTradeDTO);
                    continue;
                }

                Trade existing = existingTrades.get(incomingTradeDTO.getTradeId());
                Trade tradeToSave = null;
                if (existing != null && incomingTradeDTO.getVersion() < existing.getVersion()) {
                    // 🔴 Validation 1: Reject lower version
                    rejectedTrades.add(getRejectedTrade(incomingTradeDTO, String.format("Lower version received: %d", incomingTradeDTO.getVersion())));
                    log.error("REJECTED TRADE: Lower version received: {}", incomingTradeDTO);
                } else if (existing != null && Objects.equals(existing.getVersion(), incomingTradeDTO.getVersion())) {
                    tradeToSave = mapper.map(incomingTradeDTO, Trade.class);
                    tradeToSave.setId(existing.getId());
                } else {
                    // New trade
                    tradeToSave = mapper.map(incomingTradeDTO, Trade.class);
                }

                if (null != tradeToSave) {
                    tradeToSave.setCreatedDate(today);
                    tradeToSave.setExpired("N");
                    tradeToSave.setTradeKey(incomingTradeDTO.tradeKey());
                    tradesToSave.add(tradeToSave);
                }
            } catch (Exception e) {
                log.error("ERROR: Error parsing trade: {}", incomingTradeDTO);
                rejectedTrades.add(getRejectedTrade(incomingTradeDTO, String.format("%s: Error parsing trade: %s", e.getMessage(), incomingTradeDTO.toString())));
            }
        }
        tradeRepo.saveAll(tradesToSave);
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
