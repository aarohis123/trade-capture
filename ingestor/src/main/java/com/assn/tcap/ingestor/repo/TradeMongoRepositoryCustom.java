package com.assn.tcap.ingestor.repo;

import com.assn.tcap.ingestor.entity.TradeDocument;

import java.util.List;
import java.util.Set;

public interface TradeMongoRepositoryCustom {
    List<TradeDocument> findLatestByTradeIds(Set<Long> tradeIds);
    void bulkUpsertWithVersionHandling(List<TradeDocument> trades);
}
