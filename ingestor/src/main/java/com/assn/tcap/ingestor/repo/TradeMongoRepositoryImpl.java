package com.assn.tcap.ingestor.repo;


import com.assn.tcap.ingestor.entity.TradeDocument;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Set;

@Repository
@RequiredArgsConstructor
public class TradeMongoRepositoryImpl implements TradeMongoRepositoryCustom {

    private final MongoTemplate mongoTemplate;

    @Override
    public List<TradeDocument> findLatestByTradeIds(Set<Long> tradeIds) {

        Aggregation aggregation = Aggregation.newAggregation(

                // 1. Filter only required tradeIds
                Aggregation.match(
                        Criteria.where("tradeId").in(tradeIds)
                ),

                // 2. Sort by tradeId asc, version desc
                Aggregation.sort(
                        Sort.by(
                                Sort.Order.asc("tradeId"),
                                Sort.Order.desc("version")
                        )
                ),

                // 3. Group by tradeId and take first (highest version)
                Aggregation.group("tradeId")
                        .first(Aggregation.ROOT).as("latestTrade"),

                // 4. Replace root with latestTrade object
                Aggregation.replaceRoot("latestTrade")
        );

        return mongoTemplate.aggregate(
                aggregation,
                "trades",
                TradeDocument.class
        ).getMappedResults();

    }

    @Override
    public void bulkUpsertWithVersionHandling(List<TradeDocument> trades) {
        if (trades == null || trades.isEmpty()) {
            return;
        }

        BulkOperations bulkOps = mongoTemplate.bulkOps(
                BulkOperations.BulkMode.UNORDERED,
                TradeDocument.class
        );

        for (TradeDocument trade : trades) {

            Query query = Query.query(
                    Criteria.where("tradeId").is(trade.getTradeId())
                            .and("version").lte(trade.getVersion())
            );

            Update update = new Update()
                    .set("tradeId", trade.getTradeId())
                    .set("version", trade.getVersion())
                    .set("counterPartyId", trade.getCounterPartyId())
                    .set("bookId", trade.getBookId())
                    .set("maturityDate", trade.getMaturityDate())
                    .set("expired", trade.getExpired());
            bulkOps.upsert(query, update);
        }

        bulkOps.execute();
    }
}
