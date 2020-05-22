package com.hp.dgf.service.impl;

import com.hp.dgf.dto.request.AddDgfRateEntryDTO;
import com.hp.dgf.dto.request.UpdateDGFRateEntryDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.model.DGFRateEntry;
import com.hp.dgf.service.DGFRateEntryService;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;

@Service
public class DGFRateEntryServiceImpl implements DGFRateEntryService {
    @Override
    public DGFRateEntry addDGFRateEntry(AddDgfRateEntryDTO addDgfRateEntryDTO, HttpServletRequest httpServletRequest) {
        return null;
    }

    @Override
    public ApiResponseDTO updateDGFRateEntry(UpdateDGFRateEntryDTO updateDGFRateEntryDTO, Integer dgfRateEntryId, HttpServletRequest httpServletRequest) {
        return null;
    }
}
