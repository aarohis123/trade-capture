package com.assn.tcap.ingestor.entity;

import com.assn.tcap.ingestor.model.TradeDTO;
import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@Table(name = "trades",
        uniqueConstraints = {@UniqueConstraint(columnNames = {"tradeId", "version"})})
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TradeEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "trade_id", nullable = false)
    private Long tradeId;

    @Column(nullable = false)
    private Long version;

    @Column(name = "counter_party_id")
    private String counterPartyId;

    @Column(name = "book_id")
    private String bookId;

    @Column(name = "maturity_date", nullable = false)
    private LocalDate maturityDate;

    @Column(name = "created_date")
    private LocalDate createdDate;

    @Column(nullable = false)
    private String expired;

    @Version
    private Long rowVersion;

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
