package com.assn.tcap.ingestor.model;

import com.assn.tcap.ingestor.entity.TradeDocument;
import com.assn.tcap.ingestor.entity.TradeEntity;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.*;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class TradeDTO {
    //@Nullable Long id;
    @Nonnull Long tradeId;
    @Nonnull Long version;
    @Nullable String counterPartyId;
    @Nullable String bookId;
    @Nonnull LocalDate maturityDate;
    @Builder.Default
    @Nullable LocalDate createdDate=LocalDate.now();
    @Builder.Default
    @Nullable String expired="N";

    public TradeEntity toEntity(){
        return TradeEntity.builder()
                .tradeId(tradeId)
                .version(version)
                .bookId(bookId)
                .counterPartyId(counterPartyId)
                .maturityDate(maturityDate)
                .expired(expired)
                .build();

    }

    public TradeDocument toDocument(){
        return TradeDocument.builder()
                .tradeId(tradeId)
                .version(version)
                .bookId(bookId)
                .counterPartyId(counterPartyId)
                .maturityDate(maturityDate)
                .expired(expired)
                .build();

    }
}
