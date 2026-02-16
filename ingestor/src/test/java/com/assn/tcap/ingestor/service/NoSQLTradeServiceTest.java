package com.assn.tcap.ingestor.service;

import com.assn.tcap.ingestor.entity.TradeDocument;
import com.assn.tcap.ingestor.entity.TradeEntity;
import com.assn.tcap.ingestor.model.RejectedTradeDTO;
import com.assn.tcap.ingestor.model.TradeDTO;
import com.assn.tcap.ingestor.repo.TradeMongoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NoSQLTradeServiceTest {
    @Mock
    private TradeMongoRepository tradeMongoRepository;

    @InjectMocks
    private NoSQLTradeService nosqlTradeService;

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

    private TradeDocument buildExistingTrade(String id, Long tradeId, Long version) {
        return TradeDocument.builder()
                .id(id)
                .tradeId(tradeId)
                .version(version)
                .maturityDate(LocalDate.of(2026,2,11))
                .build();
    }

    // ==============================
    // Positive Test Cases
    // ==============================

    @Test
    void givenNewTrade_whenProcessTrades_thenTradeIsSavedSuccessfully() {

        TradeDTO incoming = buildTrade(1L, 1L, today.plusDays(5));

        when(tradeMongoRepository.findLatestByTradeIds(any()))
                .thenReturn(Collections.emptyList());

        List<RejectedTradeDTO> rejected =
                nosqlTradeService.processTrades(List.of(incoming));

        assertTrue(rejected.isEmpty());
        verify(tradeMongoRepository, times(1)).bulkUpsertWithVersionHandling(any());
    }

    @Test
    void givenExistingTradeWithSameVersion_whenProcessTrades_thenTradeIsReplaced() {

        TradeDTO incoming = buildTrade(1L, 2L, today.plusDays(5));

        TradeDocument existing = buildExistingTrade("10L", 1L, 2L);

        when(tradeMongoRepository.findLatestByTradeIds(any()))
                .thenReturn(List.of(existing));

        List<RejectedTradeDTO> rejected =
                nosqlTradeService.processTrades(List.of(incoming));

        assertTrue(rejected.isEmpty());
        verify(tradeMongoRepository, times(1)).bulkUpsertWithVersionHandling(any());
    }

    @Test
    void givenMultipleValidTrades_whenProcessTrades_thenAllTradesAreSaved() {

        TradeDTO trade1 = buildTrade(1L, 1L, today.plusDays(10));
        TradeDTO trade2 = buildTrade(2L, 1L, today.plusDays(20));

        when(tradeMongoRepository.findLatestByTradeIds(any()))
                .thenReturn(Collections.emptyList());

        List<RejectedTradeDTO> rejected =
                nosqlTradeService.processTrades(List.of(trade1, trade2));

        assertTrue(rejected.isEmpty());
        verify(tradeMongoRepository, times(1)).bulkUpsertWithVersionHandling(any());
    }

    // ==============================
    // ❌ Negative Test Cases
    // ==============================

    @Test
    void givenLowerVersionTrade_whenProcessTrades_thenTradeIsRejected() {

        TradeDTO incoming = buildTrade(1L, 1L, today.plusDays(5));

        TradeDocument existing = buildExistingTrade("10L", 1L, 5L);

        when(tradeMongoRepository.findLatestByTradeIds(any()))
                .thenReturn(List.of(existing));

        List<RejectedTradeDTO> rejected =
                nosqlTradeService.processTrades(List.of(incoming));

        assertEquals(1, rejected.size());
        assertTrue(rejected.get(0).getReason().contains("Lower version"));
        verify(tradeMongoRepository).bulkUpsertWithVersionHandling(Collections.emptyList());
    }

    @Test
    void givenTradeWithPastMaturityDate_whenProcessTrades_thenTradeIsRejected() {

        TradeDTO incoming =
                buildTrade(1L, 1L, today.minusDays(1));

        when(tradeMongoRepository.findLatestByTradeIds(any()))
                .thenReturn(Collections.emptyList());

        List<RejectedTradeDTO> rejected =
                nosqlTradeService.processTrades(List.of(incoming));

        assertEquals(1, rejected.size());
        assertTrue(rejected.get(0).getReason().contains("Maturity date expired"));
        verify(tradeMongoRepository).bulkUpsertWithVersionHandling(any());
    }

    @Test
    void givenNullInput_whenProcessTrades_thenEmptyListIsReturned() {

        List<RejectedTradeDTO> rejected =
                nosqlTradeService.processTrades(null);

        assertTrue(rejected.isEmpty());
        verify(tradeMongoRepository, never()).bulkUpsertWithVersionHandling(any());
    }

    @Test
    void givenEmptyInput_whenProcessTrades_thenEmptyListIsReturned() {

        List<RejectedTradeDTO> rejected =
                nosqlTradeService.processTrades(Collections.emptyList());

        assertTrue(rejected.isEmpty());
        verify(tradeMongoRepository, never()).bulkUpsertWithVersionHandling(any());
    }

    @Test
    void givenRepositoryFailure_whenProcessTrades_thenExceptionIsThrown() {

        TradeDTO incoming =
                buildTrade(1L, 1L, today.plusDays(5));

        when(tradeMongoRepository.findLatestByTradeIds(any()))
                .thenThrow(new RuntimeException("DB error"));

        assertThrows(RuntimeException.class, () ->
                nosqlTradeService.processTrades(List.of(incoming))
        );

        verify(tradeMongoRepository, never()).bulkUpsertWithVersionHandling(any());
    }

    // ==============================
    // 🔄 getAllTrades() Test
    // ==============================

    @Test
    void givenExistingTrades_whenGetAllTrades_thenAllTradesAreReturned() {

        TradeDocument tradeEntity = buildExistingTrade("1L", 1L, 1L);

        when(tradeMongoRepository.findAll())
                .thenReturn(List.of(tradeEntity));

        List<TradeDTO> result = nosqlTradeService.getAllTrades();

        assertEquals(1, result.size());
        assertEquals(1L, result.get(0).getTradeId());
    }

}