package com.assn.tcap.ingestor.repo;

import com.assn.tcap.ingestor.entity.Trade;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;

@Repository
public interface TradeRepo extends JpaRepository<Trade, Long> {

    @Query("""
       SELECT t FROM Trade t
       WHERE t.tradeId IN :tradeIds
       AND t.version = (
           SELECT MAX(t2.version)
           FROM Trade t2
           WHERE t2.tradeId = t.tradeId
       )
       """)
    List<Trade> findLatestTradesByTradeIds(@Param("tradeIds") Collection<Long> tradeIds);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Trade t SET t.expired = 'Y' WHERE t.maturityDate < :today and t.expired = 'N'")
    int expireTrades(@Param("today") LocalDate today);
}
