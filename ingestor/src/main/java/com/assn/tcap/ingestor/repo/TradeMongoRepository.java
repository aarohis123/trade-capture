package com.assn.tcap.ingestor.repo;


import com.assn.tcap.ingestor.entity.TradeDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface TradeMongoRepository
        extends MongoRepository<TradeDocument, String>, TradeMongoRepositoryCustom {
}