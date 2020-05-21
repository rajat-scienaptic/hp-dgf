package com.hp.dgf.service;

import com.hp.dgf.dto.request.AddPLRequestDTO;
import com.hp.dgf.dto.request.UpdatePLRequestDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

public interface DGFService {
    List<Object> getDgfGroups(int businessCategoryId);
    List<Object> getHeaderData();
    ApiResponseDTO addPL(AddPLRequestDTO addPlRequestDTO, HttpServletRequest request);
    ApiResponseDTO updatePL(UpdatePLRequestDTO updatePLRequestDTO, int productLineId, HttpServletRequest request);
}
