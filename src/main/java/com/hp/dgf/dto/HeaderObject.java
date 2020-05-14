package com.hp.dgf.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@AllArgsConstructor
@Data
public class HeaderObject {
    private List<Object> column;
    private List<Object> data;
}
