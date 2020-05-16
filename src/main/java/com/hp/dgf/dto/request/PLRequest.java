package com.hp.dgf.dto.request;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;

@Data
public class PLRequest {
    @NotNull @NotBlank
    private String code;
    @NotNull @NotBlank
    private Integer businessSubCategoryId;
    @NotNull @NotBlank
    private Integer colorCodeId;
    @NotNull @NotBlank
    private BigDecimal baseRate;
    @NotNull @NotBlank
    private Integer modifiedBy;
    @NotNull @NotBlank
    private Integer dgfSubGroup2Id;
    @NotNull @NotBlank
    private Integer dgfSubGroup3Id;
    @NotNull @NotBlank
    private Byte isActive;
}
