package com.hp.dgf.dto.request;

import lombok.Data;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;

@Data
public class UpdateDGFRateEntryDTO {
    private Integer attachmentId;
    @NotNull @Min(value = 0)
    private BigDecimal dgfRate;
    private String note;
}
