package com.assn.tcap.ingestor.repo;

import com.assn.tcap.ingestor.entity.Trade;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
class TradeRepoTest {

    @Autowired
    protected TradeRepo tradeRepo;

    private Trade buildTrade(Long tradeId, Long version) {
        return Trade.builder()
                .tradeId(tradeId)
                .version(version)
                .counterPartyId("CP1")
                .bookId("BOOK1")
                .maturityDate(LocalDate.now().plusDays(10))
                .createdDate(LocalDate.now())
                .expired("N")
                .tradeKey(tradeId + "-" + version)
                .build();
    }

    // ==============================
    // findLatestTradesByTradeIds()
    // ==============================

    @Test
    @DisplayName("Should return latest version per tradeId")
    void givenMultipleVersions_whenFindLatestTrades_thenOnlyMaxVersionReturned() {

        tradeRepo.save(buildTrade(1L, 1L));
        tradeRepo.save(buildTrade(1L, 2L));
        tradeRepo.save(buildTrade(1L, 3L));

        List<Trade> result =
                tradeRepo.findLatestTradesByTradeIds(List.of(1L));

        assertThat(result).hasSize(1);
        assertThat(result.get(0).getVersion()).isEqualTo(3L);
    }

    @Test
    @DisplayName("Should return latest trades for multiple tradeIds")
    void givenMultipleTradeIds_whenFindLatestTrades_thenReturnLatestForEach() {

        tradeRepo.save(buildTrade(1L, 1L));
        tradeRepo.save(buildTrade(1L, 2L));
        tradeRepo.save(buildTrade(2L, 1L));
        tradeRepo.save(buildTrade(2L, 5L));

        List<Trade> result =
                tradeRepo.findLatestTradesByTradeIds(List.of(1L, 2L));

        assertThat(result).hasSize(2);
        assertThat(result)
                .extracting(Trade::getVersion)
                .containsExactlyInAnyOrder(2L, 5L);
    }

    @Test
    @DisplayName("Should return empty list when tradeId not found")
    void givenNonExistingTradeId_whenFindLatestTrades_thenReturnEmpty() {

        List<Trade> result =
                tradeRepo.findLatestTradesByTradeIds(List.of(999L));

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Should return empty list when input list is empty")
    void givenEmptyList_whenFindLatestTrades_thenReturnEmpty() {

        List<Trade> result =
                tradeRepo.findLatestTradesByTradeIds(List.of());

        assertThat(result).isEmpty();
    }

    // ==============================
    // existsHigherVersion()
    // ==============================

    @Test
    @DisplayName("Should return true when higher version exists")
    void givenHigherVersionExists_whenCheckExistsHigherVersion_thenReturnTrue() {

        tradeRepo.save(buildTrade(1L, 5L));

        boolean exists =
                tradeRepo.existsHigherVersion(1L, 2L);

        assertThat(exists).isTrue();
    }

    @Test
    @DisplayName("Should return false when no higher version exists")
    void givenNoHigherVersion_whenCheckExistsHigherVersion_thenReturnFalse() {

        tradeRepo.save(buildTrade(1L, 2L));

        boolean exists =
                tradeRepo.existsHigherVersion(1L, 5L);

        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("Should return false when tradeId does not exist")
    void givenNonExistingTradeId_whenCheckExistsHigherVersion_thenReturnFalse() {

        boolean exists =
                tradeRepo.existsHigherVersion(999L, 1L);

        assertThat(exists).isFalse();
    }
}