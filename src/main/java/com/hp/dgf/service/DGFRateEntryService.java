package com.hp.dgf.service;

import com.hp.dgf.dto.request.AddDgfRateEntryDTO;
import com.hp.dgf.dto.request.UpdateDGFRateEntryDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;

public interface DGFRateEntryService {
    ApiResponseDTO addDGFRateEntry(final AddDgfRateEntryDTO addDgfRateEntryDTO,
                                   final HttpServletRequest request,
                                   final String cookie);
    ApiResponseDTO updateDGFRateEntry(final UpdateDGFRateEntryDTO updateDGFRateEntryDTO,
                                      final int dgfRateEntryId,
                                      final HttpServletRequest request,
                                      final MultipartFile file,
                                      final String cookie);
    Object getDgfRateEntryDataById(final int dgfRateEntryId);
}
