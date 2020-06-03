package com.hp.dgf.service;

import com.hp.dgf.dto.request.AddPLRequestDTO;
import com.hp.dgf.dto.request.UpdatePLRequestDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

public interface DGFHeaderDataService {
    List<Object> getHeaderData(int businessCategoryId);
    ApiResponseDTO addPL(final AddPLRequestDTO addPlRequestDTO, final HttpServletRequest request);
    ApiResponseDTO updatePL(final UpdatePLRequestDTO updatePLRequestDTO, final int productLineId, final HttpServletRequest request);
    ApiResponseDTO deletePL(final int productLineId, HttpServletRequest request);
}
