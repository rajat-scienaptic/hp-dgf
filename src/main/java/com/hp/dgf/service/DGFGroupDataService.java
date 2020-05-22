package com.hp.dgf.service;

import com.hp.dgf.dto.request.AddPLRequestDTO;
import com.hp.dgf.dto.request.UpdatePLRequestDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

public interface DGFGroupDataService {
    List<Object> getDgfGroups(final int businessCategoryId);
}
