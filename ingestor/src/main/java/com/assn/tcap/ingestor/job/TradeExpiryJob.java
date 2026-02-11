package com.assn.tcap.ingestor.job;

import com.assn.tcap.ingestor.repo.TradeRepo;
import jakarta.transaction.Transactional;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.LocalDate;

public class TradeExpiryJob {

    private final TradeRepo repository;

    public TradeExpiryJob(TradeRepo repository) {
        this.repository = repository;
    }


    @Scheduled(cron = "0 0 0 * * ?")
    @Transactional
    public void expireTrades() {
        repository.expireTrades(LocalDate.now());
    }
}
