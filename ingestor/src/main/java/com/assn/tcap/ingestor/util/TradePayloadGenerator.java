package com.assn.tcap.ingestor.util;

import com.assn.tcap.ingestor.model.TradeDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.util.*;

@Slf4j
public class TradePayloadGenerator {

    private static final int TOTAL_RECORDS = 1000;
    private static final Random random = new Random();

    public static List<TradeDTO> getPayload() {

        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        List<TradeDTO> trades = new ArrayList<>();
        Map<Long, Integer> tradeVersionMap = new HashMap<>();

        for (int i = 1; i <= TOTAL_RECORDS; i++) {

            int scenario = random.nextInt(5); // 5 scenarios
            Long tradeId = random.nextLong(200) + 1; // reuse IDs to test versioning
            Integer version;
            LocalDate maturityDate;
            switch (scenario) {

                // ✅ Valid TradeEntity
                case 0:
                    version = tradeVersionMap.getOrDefault(tradeId, 0) + 1;
                    maturityDate = LocalDate.now().plusDays(random.nextInt(1000) + 1);
                    tradeVersionMap.put(tradeId, version);
                    break;

                // ✅ Higher Version Update
                case 1:
                    version = tradeVersionMap.getOrDefault(tradeId, 1) + 1;
                    maturityDate = LocalDate.now().plusDays(500);
                    tradeVersionMap.put(tradeId, version);
                    break;

                // ❌ Lower Version (Should Reject)
                case 2:
                    version = Math.max(1, tradeVersionMap.getOrDefault(tradeId, 1) - 1);
                    maturityDate = LocalDate.now().plusDays(400);
                    break;

                // ❌ Past Maturity Date
                case 3:
                    version = tradeVersionMap.getOrDefault(tradeId, 1);
                    maturityDate = LocalDate.now().minusDays(random.nextInt(100) + 1);
                    break;

                // ✅ Long Future TradeEntity
                default:
                    version = tradeVersionMap.getOrDefault(tradeId, 1);
                    maturityDate = LocalDate.now().plusYears(10);
                    break;
            }
            trades.add(TradeDTO.builder()
                    .tradeId(tradeId)
                    .version(version.longValue())
                    .bookId("Book-" + (random.nextInt(50) + 1))
                    .counterPartyId("CP-" + (random.nextInt(500) + 1))
                    .maturityDate(maturityDate)
                    .build());
        }


        log.info("Generated {} trade test payloads",  TOTAL_RECORDS);
        return trades;
    }
}