package com.assn.tcap.ingestor.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@Table(name = "trades")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Trade {
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

    @Column(name="trade_key", nullable = false)
    private String tradeKey;

}
