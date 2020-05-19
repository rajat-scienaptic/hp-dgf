package com.hp.dgf.dto.response;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Builder(toBuilder = true)
@Data
public class ApiResponseDTO {
    private LocalDateTime timestamp;
    private int status;
    private String message;
}
