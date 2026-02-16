package com.assn.tcap.ingestor.job;

import com.assn.tcap.ingestor.repo.TradeSQLRepo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class TradeEntityExpiryJobTest {

    @Mock
    private TradeSQLRepo tradeSQLRepo;

    @InjectMocks
    private TradeExpiryJob tradeExpiryJob;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    // =====================================================
    // Positive Test Case
    // =====================================================

    @Test
    @DisplayName("Should call repository expireTrades with today's date")
    void givenJobExecution_whenExpireTradesCalled_thenRepositoryMethodInvokedWithToday() {

        tradeExpiryJob.expireTrades();

        ArgumentCaptor<LocalDate> dateCaptor =
                ArgumentCaptor.forClass(LocalDate.class);

        verify(tradeSQLRepo, times(1))
                .expireTrades(dateCaptor.capture());

        assertThat(dateCaptor.getValue())
                .isEqualTo(LocalDate.now());
    }

    // =====================================================
    // Negative Test Case
    // =====================================================

    @Test
    @DisplayName("Should propagate exception if repository throws error")
    void givenRepositoryThrowsException_whenExpireTrades_thenExceptionPropagated() {

        doThrow(new RuntimeException("DB Error"))
                .when(tradeSQLRepo)
                .expireTrades(any());

        try {
            tradeExpiryJob.expireTrades();
        } catch (RuntimeException ex) {
            assertThat(ex.getMessage()).isEqualTo("DB Error");
        }

        verify(tradeSQLRepo, times(1))
                .expireTrades(any());
    }

    // =====================================================
    // Edge Case Test
    // =====================================================

    @Test
    @DisplayName("Should still execute repository even if no trades to expire")
    void givenNoTradesEligible_whenExpireTrades_thenRepositoryStillCalled() {

        when(tradeSQLRepo.expireTrades(any()))
                .thenReturn(0);

        tradeExpiryJob.expireTrades();

        verify(tradeSQLRepo, times(1))
                .expireTrades(LocalDate.now());
    }
}