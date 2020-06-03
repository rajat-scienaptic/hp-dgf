package com.hp.dgf.service;

import com.hp.dgf.dto.request.AddDgfRateEntryDTO;
import com.hp.dgf.dto.request.UpdateDGFRateEntryDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;

public interface DGFRateEntryService {
    ApiResponseDTO addDGFRateEntry(AddDgfRateEntryDTO addDgfRateEntryDTO, HttpServletRequest request);
    ApiResponseDTO updateDGFRateEntry(UpdateDGFRateEntryDTO updateDGFRateEntryDTO, int dgfRateEntryId, HttpServletRequest request, MultipartFile file);
    Object getDgfRateEntryDataById(int dgfRateEntryId);
}
