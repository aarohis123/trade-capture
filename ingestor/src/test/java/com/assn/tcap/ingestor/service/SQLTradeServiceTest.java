package com.assn.tcap.ingestor.service;

import com.assn.tcap.ingestor.entity.TradeEntity;
import com.assn.tcap.ingestor.model.RejectedTradeDTO;
import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.repo.TradeSQLRepo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SQLTradeServiceTest {

    @Mock
    private TradeSQLRepo tradeSQLRepo;

    @InjectMocks
    private SQLTradeService sqlTradeService;

    private LocalDate today;

    @BeforeEach
    void setup() {
        today = LocalDate.now();
    }

    private TradeDTO buildTrade(Long tradeId, Long version, LocalDate maturity) {
        return TradeDTO.builder()
                .tradeId(tradeId)
                .version(version)
                .maturityDate(maturity)
                .build();
    }

    private TradeEntity buildExistingTrade(Long id, Long tradeId, Long version) {
        TradeEntity tradeEntity = new TradeEntity();
        tradeEntity.setId(id);
        tradeEntity.setTradeId(tradeId);
        tradeEntity.setVersion(version);
        tradeEntity.setMaturityDate(LocalDate.of(2026,2,11));
        return tradeEntity;
    }

    // ==============================
    // Positive Test Cases
    // ==============================

    @Test
    void givenNewTrade_whenProcessTrades_thenTradeIsSavedSuccessfully() {

        TradeDTO incoming = buildTrade(1L, 1L, today.plusDays(5));

        when(tradeSQLRepo.findLatestTradesByTradeIds(any()))
                .thenReturn(Collections.emptyList());

        List<RejectedTradeDTO> rejected =
                sqlTradeService.processTrades(List.of(incoming));

        assertTrue(rejected.isEmpty());
        verify(tradeSQLRepo, times(1)).saveAll(any());
    }

    @Test
    void givenExistingTradeWithSameVersion_whenProcessTrades_thenTradeIsReplaced() {

        TradeDTO incoming = buildTrade(1L, 2L, today.plusDays(5));

        TradeEntity existing = buildExistingTrade(10L, 1L, 2L);

        when(tradeSQLRepo.findLatestTradesByTradeIds(any()))
                .thenReturn(List.of(existing));

        List<RejectedTradeDTO> rejected =
                sqlTradeService.processTrades(List.of(incoming));

        assertTrue(rejected.isEmpty());
        verify(tradeSQLRepo, times(1)).saveAll(any());
    }

    @Test
    void givenMultipleValidTrades_whenProcessTrades_thenAllTradesAreSaved() {

        TradeDTO trade1 = buildTrade(1L, 1L, today.plusDays(10));
        TradeDTO trade2 = buildTrade(2L, 1L, today.plusDays(20));

        when(tradeSQLRepo.findLatestTradesByTradeIds(any()))
                .thenReturn(Collections.emptyList());

        List<RejectedTradeDTO> rejected =
                sqlTradeService.processTrades(List.of(trade1, trade2));

        assertTrue(rejected.isEmpty());
        verify(tradeSQLRepo, times(1)).saveAll(any());
    }

    // ==============================
    // ❌ Negative Test Cases
    // ==============================

    @Test
    void givenLowerVersionTrade_whenProcessTrades_thenTradeIsRejected() {

        TradeDTO incoming = buildTrade(1L, 1L, today.plusDays(5));

        TradeEntity existing = buildExistingTrade(10L, 1L, 5L);

        when(tradeSQLRepo.findLatestTradesByTradeIds(any()))
                .thenReturn(List.of(existing));

        List<RejectedTradeDTO> rejected =
                sqlTradeService.processTrades(List.of(incoming));

        assertEquals(1, rejected.size());
        assertTrue(rejected.get(0).getReason().contains("Lower version"));
        verify(tradeSQLRepo).saveAll(Collections.emptyList());
    }

    @Test
    void givenTradeWithPastMaturityDate_whenProcessTrades_thenTradeIsRejected() {

        TradeDTO incoming =
                buildTrade(1L, 1L, today.minusDays(1));

        when(tradeSQLRepo.findLatestTradesByTradeIds(any()))
                .thenReturn(Collections.emptyList());

        List<RejectedTradeDTO> rejected =
                sqlTradeService.processTrades(List.of(incoming));

        assertEquals(1, rejected.size());
        assertTrue(rejected.get(0).getReason().contains("Maturity date expired"));
        verify(tradeSQLRepo).saveAll(any());
    }

    @Test
    void givenNullInput_whenProcessTrades_thenEmptyListIsReturned() {

        List<RejectedTradeDTO> rejected =
                sqlTradeService.processTrades(null);

        assertTrue(rejected.isEmpty());
        verify(tradeSQLRepo, never()).saveAll(any());
    }

    @Test
    void givenEmptyInput_whenProcessTrades_thenEmptyListIsReturned() {

        List<RejectedTradeDTO> rejected =
                sqlTradeService.processTrades(Collections.emptyList());

        assertTrue(rejected.isEmpty());
        verify(tradeSQLRepo, never()).saveAll(any());
    }

    @Test
    void givenRepositoryFailure_whenProcessTrades_thenExceptionIsThrown() {

        TradeDTO incoming =
                buildTrade(1L, 1L, today.plusDays(5));

        when(tradeSQLRepo.findLatestTradesByTradeIds(any()))
                .thenThrow(new RuntimeException("DB error"));

        assertThrows(RuntimeException.class, () ->
                sqlTradeService.processTrades(List.of(incoming))
        );

        verify(tradeSQLRepo, never()).saveAll(any());
    }

    // ==============================
    // 🔄 getAllTrades() Test
    // ==============================

    @Test
    void givenExistingTrades_whenGetAllTrades_thenAllTradesAreReturned() {

        TradeEntity tradeEntity = buildExistingTrade(1L, 1L, 1L);

        when(tradeSQLRepo.findAll())
                .thenReturn(List.of(tradeEntity));

        List<TradeDTO> result = sqlTradeService.getAllTrades();

        assertEquals(1, result.size());
        assertEquals(1L, result.get(0).getTradeId());
    }
}