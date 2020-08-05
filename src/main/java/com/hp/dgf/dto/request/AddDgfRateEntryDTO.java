package com.hp.dgf.dto.request;

import lombok.Data;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;

@Data
public class AddDgfRateEntryDTO {
    @NotNull @Min(value = 0)
    private BigDecimal dgfRate;
    private Integer dgfSubGroupLevel2Id;
    private Integer dgfSubGroupLevel3Id;
    private String productLineName;
}
