package com.hp.dgf.dto.request;

import lombok.Data;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;

@Data
public class UpdateDGFRateEntryDTO {
    @NotNull @Min(value = 0)
    private BigDecimal dgfRate;
    @NotNull @NotBlank
    private String note;
}
