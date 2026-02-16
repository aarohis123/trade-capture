package com.assn.tcap.ingestor.job;

import com.assn.tcap.ingestor.repo.TradeSQLRepo;
import jakarta.transaction.Transactional;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDate;

public class TradeExpiryJob {

    private final TradeSQLRepo repository;

    public TradeExpiryJob(TradeSQLRepo repository) {
        this.repository = repository;
    }


    @Scheduled(cron = "0 0 0 * * ?")
    @Transactional
    public void expireTrades() {
        repository.expireTrades(LocalDate.now());
    }
}
