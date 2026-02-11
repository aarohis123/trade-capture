package com.assn.tcap.ingestor.model;

import lombok.*;

@EqualsAndHashCode(callSuper = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RejectedTradeDTO extends TradeDTO {
    private String reason;
}
