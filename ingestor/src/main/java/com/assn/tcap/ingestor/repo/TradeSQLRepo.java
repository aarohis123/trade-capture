package com.assn.tcap.ingestor.repo;

import com.assn.tcap.ingestor.entity.TradeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;

@Repository
public interface TradeSQLRepo extends JpaRepository<TradeEntity, Long> {

    @Query("""
       SELECT t FROM TradeEntity t
       WHERE t.tradeId IN :tradeIds
       AND t.version = (
           SELECT MAX(t2.version)
           FROM TradeEntity t2
           WHERE t2.tradeId = t.tradeId
       )
       """)
    List<TradeEntity> findLatestTradesByTradeIds(@Param("tradeIds") Collection<Long> tradeIds);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE TradeEntity t SET t.expired = 'Y' WHERE t.maturityDate < :today and t.expired = 'N'")
    int expireTrades(@Param("today") LocalDate today);
}
