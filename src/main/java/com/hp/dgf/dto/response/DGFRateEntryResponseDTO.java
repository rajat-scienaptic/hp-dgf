package com.hp.dgf.dto.response;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Builder(toBuilder = true)
@Data
public class DGFRateEntryResponseDTO {
    private String category;
    private String dgfGroup;
    private String productLine;
    private String seller;
    private BigDecimal dgfRate;
    private List<DGFRateChangeLogDTO> dgfRateChangeLogs;
}
