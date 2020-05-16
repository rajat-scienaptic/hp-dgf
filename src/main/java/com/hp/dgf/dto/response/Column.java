package com.hp.dgf.dto.response;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@AllArgsConstructor
@Data
public class Column {
    private List<Object> columns;
}
