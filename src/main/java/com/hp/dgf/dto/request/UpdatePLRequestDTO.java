package com.hp.dgf.dto.request;

import lombok.Data;

import javax.validation.constraints.Min;
import java.math.BigDecimal;

@Data
public class UpdatePLRequestDTO {
    private String code;
    @Min(value = 0)
    private BigDecimal baseRate;
}
