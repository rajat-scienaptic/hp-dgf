package com.hp.dgf.service;

import com.hp.dgf.dto.request.PLRequestDTO;

import java.util.List;

public interface DGFService {
    List<Object> getDgfGroups(int businessCategoryId);
    List<Object> getHeaderData();
    Object addPL(PLRequestDTO plRequestDTO);
    Object updatePL(PLRequestDTO plRequestDTO, int productLineId);
}
