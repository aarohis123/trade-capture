package com.assn.tcap.ingestor.repo;

import com.assn.tcap.ingestor.entity.TradeEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
class TradeEntityRepoTest {

    @Autowired
    protected TradeSQLRepo tradeSQLRepo;

    private TradeEntity buildTrade(Long tradeId, Long version) {
        return TradeEntity.builder()
                .tradeId(tradeId)
                .version(version)
                .counterPartyId("CP1")
                .bookId("BOOK1")
                .maturityDate(LocalDate.now().plusDays(10))
                .createdDate(LocalDate.now())
                .expired("N")
                .build();
    }

    private TradeEntity buildTrade(Long tradeId, Long version, LocalDate maturityDate, String expired) {
        return TradeEntity.builder()
                .tradeId(tradeId)
                .version(version)
                .counterPartyId("CP1")
                .bookId("BOOK1")
                .maturityDate(maturityDate)
                .createdDate(LocalDate.now())
                .expired(expired)
                .build();
    }

    // ==============================
    // findLatestTradesByTradeIds()
    // ==============================

    @Test
    void givenMultipleVersions_whenFindLatestTrades_thenOnlyMaxVersionReturned() {

        tradeSQLRepo.save(buildTrade(1L, 1L));
        tradeSQLRepo.save(buildTrade(1L, 2L));
        tradeSQLRepo.save(buildTrade(1L, 3L));

        List<TradeEntity> result =
                tradeSQLRepo.findLatestTradesByTradeIds(List.of(1L));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getVersion()).isEqualTo(3L);
    }

    @Test
    void givenMultipleTradeIds_whenFindLatestTrades_thenReturnLatestForEach() {

        tradeSQLRepo.save(buildTrade(1L, 1L));
        tradeSQLRepo.save(buildTrade(1L, 2L));
        tradeSQLRepo.save(buildTrade(2L, 1L));
        tradeSQLRepo.save(buildTrade(2L, 5L));

        List<TradeEntity> result =
                tradeSQLRepo.findLatestTradesByTradeIds(List.of(1L, 2L));

        assertThat(result).hasSize(2);
        assertThat(result)
                .extracting(TradeEntity::getVersion)
                .containsExactlyInAnyOrder(2L, 5L);
    }

    @Test
    void givenNonExistingTradeId_whenFindLatestTrades_thenReturnEmpty() {

        List<TradeEntity> result =
                tradeSQLRepo.findLatestTradesByTradeIds(List.of(999L));

        assertThat(result).isEmpty();
    }

    @Test
    void givenEmptyList_whenFindLatestTrades_thenReturnEmpty() {

        List<TradeEntity> result =
                tradeSQLRepo.findLatestTradesByTradeIds(List.of());

        assertThat(result).isEmpty();
    }



    // =====================================================
    // expireTrades() - POSITIVE TESTS
    // =====================================================

    @Test
    void givenExpiredMaturityDates_whenExpireTrades_thenTradesMarkedExpired() throws Exception {

        tradeSQLRepo.save(buildTrade(1L, 1L, LocalDate.now().minusDays(5), "N"));
        tradeSQLRepo.save(buildTrade(2L, 1L, LocalDate.now().plusDays(5), "N"));
        int updatedCount = tradeSQLRepo.expireTrades(LocalDate.now());

        assertThat(updatedCount).isEqualTo(1);

        List<TradeEntity> allTradeEntities = tradeSQLRepo.findAll();

        TradeEntity expiredTradeEntity = allTradeEntities.stream()
                .filter(t -> t.getTradeId().equals(1L))
                .findFirst()
                .orElseThrow();

        assertThat(expiredTradeEntity.getExpired()).isEqualTo("Y");
    }

    @Test
    void givenAlreadyExpiredTrades_whenExpireTrades_thenDoNotUpdateAgain() {

        tradeSQLRepo.save(buildTrade(1L, 1L, LocalDate.now().minusDays(5), "Y"));

        int updatedCount = tradeSQLRepo.expireTrades(LocalDate.now());

        assertThat(updatedCount).isZero();
    }

    @Test
    void givenMaturityDateEqualsToday_whenExpireTrades_thenDoNotExpire() {

        tradeSQLRepo.save(buildTrade(1L, 1L, LocalDate.now(), "N"));

        int updatedCount = tradeSQLRepo.expireTrades(LocalDate.now());

        assertThat(updatedCount).isZero();
    }

    // =====================================================
    // expireTrades() - NEGATIVE / EDGE TESTS
    // =====================================================

    @Test
    void givenNoEligibleTrades_whenExpireTrades_thenReturnZero() {

        tradeSQLRepo.save(buildTrade(1L, 1L, LocalDate.now().plusDays(10), "N"));

        int updatedCount = tradeSQLRepo.expireTrades(LocalDate.now());

        assertThat(updatedCount).isZero();
    }

    @Test
    void givenMultipleExpiredTrades_whenExpireTrades_thenReturnCorrectUpdateCount() {

        tradeSQLRepo.save(buildTrade(1L, 1L, LocalDate.now().minusDays(2), "N"));
        tradeSQLRepo.save(buildTrade(2L, 1L, LocalDate.now().minusDays(3), "N"));
        tradeSQLRepo.save(buildTrade(3L, 1L, LocalDate.now().plusDays(3), "N"));

        int updatedCount = tradeSQLRepo.expireTrades(LocalDate.now());

        assertThat(updatedCount).isEqualTo(2);
    }


}