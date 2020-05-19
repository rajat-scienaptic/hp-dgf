package com.hp.dgf.dto.request;

import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;

@Data
public class PLRequestDTO {
    @NotNull @NotBlank
    private String code;
    @NotNull
    private Integer businessSubCategoryId;
    private Integer colorCodeId;
    @NotNull
    private BigDecimal baseRate;
    private Integer modifiedBy;
    private Integer dgfSubGroup2Id;
    private Integer dgfSubGroup3Id;
    @NotNull
    private Byte isActive;
}
