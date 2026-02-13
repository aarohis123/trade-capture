package com.assn.tcap.ingestor.model;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.*;

import java.time.LocalDate;

import static java.lang.String.format;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class TradeDTO {
    @Nullable Long id;
    @Nonnull Long tradeId;
    @Nonnull Long version;
    @Nullable String counterPartyId;
    @Nullable String bookId;
    @Nonnull LocalDate maturityDate;
    @Nullable LocalDate createdDate;
    @Nullable String expired;
}
