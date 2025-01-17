package com.hp.dgf.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@AllArgsConstructor
@Data
public final class HeaderObjectDTO {
    private List<Object> column;
    private List<Object> data;
    private List<Object> business;
}
