package com.hp.dgf.dto.response;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Builder(toBuilder = true)
@Data
public class ExceptionDTO {
    private LocalDateTime timestamp;
    private Integer status;
    private String message;
}
