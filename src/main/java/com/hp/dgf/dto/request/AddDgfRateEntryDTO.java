package com.hp.dgf.dto.request;

import lombok.Data;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
public class AddDgfRateEntryDTO {
    private Integer attachmentId;
    @NotNull @NotBlank
    private String createdBy;
    @NotNull
    private LocalDateTime createdOn;
    @NotNull @Min(value = 0)
    private BigDecimal dgfRate;
    private Integer dgfSubGroupLevel2Id;
    private Integer dgfSubGroupLevel3Id;
    private String note;
    private Integer productLineId;
}
