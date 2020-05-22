package com.hp.dgf.service;

import com.hp.dgf.dto.request.AddDgfRateEntryDTO;
import com.hp.dgf.dto.request.UpdateDGFRateEntryDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.model.DGFRateEntry;

import javax.servlet.http.HttpServletRequest;

public interface DGFRateEntryService {
    DGFRateEntry addDGFRateEntry(AddDgfRateEntryDTO addDgfRateEntryDTO, HttpServletRequest httpServletRequest);
    ApiResponseDTO updateDGFRateEntry(UpdateDGFRateEntryDTO updateDGFRateEntryDTO, Integer dgfRateEntryId, HttpServletRequest httpServletRequest);
}
