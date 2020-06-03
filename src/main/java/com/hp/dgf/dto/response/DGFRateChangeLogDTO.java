package com.hp.dgf.dto.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Builder(toBuilder = true)
@Data
public class DGFRateChangeLogDTO{
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "MM/dd/yyyy HH:mm")
    private LocalDateTime date;
    private BigDecimal baseRate;
    private String notes;
    private String attachment;
    private String modifiedBy;
}
