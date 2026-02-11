package com.assn.tcap.ingestor.repo;

import com.assn.tcap.ingestor.entity.Trade;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

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
    List<Trade> findLatestTradesByTradeIds(@Param("tradeIds") List<Long> tradeIds);

    @Query("""
       SELECT COUNT(t) > 0
       FROM Trade t
       WHERE t.tradeId = :tradeId
       AND t.version > :version
       """)
    boolean existsHigherVersion(@Param("tradeId") Long tradeId,
                                @Param("version") Long version);
}
