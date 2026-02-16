package com.assn.tcap.ingestor.entity;

import com.assn.tcap.ingestor.model.TradeDTO;
import jakarta.persistence.Column;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Version;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.Indexed;

import java.time.LocalDate;

@Document(collection = "trades")
@CompoundIndex(
        name = "trade_version_unique_idx",
        def = "{'tradeId':1, 'version':1}",
        unique = true
)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeDocument {

    @Id
    private String id;

    @Indexed
    private Long tradeId;

    @Indexed
    private Long version;

    private String counterPartyId;

    private String bookId;

    private LocalDate maturityDate;

    private LocalDate createdDate;

    private String expired;

    public TradeDTO toDTO(){
        return TradeDTO.builder()
                .tradeId(tradeId)
                .version(version)
                .bookId(bookId)
                .counterPartyId(counterPartyId)
                .maturityDate(maturityDate)
                .expired(expired)
                .build();

    }

}