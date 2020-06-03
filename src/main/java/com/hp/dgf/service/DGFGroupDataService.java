package com.hp.dgf.service;

import com.hp.dgf.dto.request.DGFGroupDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

public interface DGFGroupDataService {
    List<Object> getDgfGroups(final int businessCategoryId);
    ApiResponseDTO addDgfGroup(DGFGroupDTO dgfGroupDTO, HttpServletRequest request);
    ApiResponseDTO addDgfSubGroupLevel1(DGFGroupDTO dgfGroupDTO, HttpServletRequest request);
    ApiResponseDTO addDgfSubGroupLevel2(DGFGroupDTO dgfGroupDTO, HttpServletRequest request);
    ApiResponseDTO addDgfSubGroupLevel3(DGFGroupDTO dgfGroupDTO, HttpServletRequest request);
    ApiResponseDTO deleteDgfGroup(int dgfGroupId, HttpServletRequest request);
    ApiResponseDTO deleteDgfSubGroupLevel1(int dgfSubGroupLevel1Id, HttpServletRequest request);
    ApiResponseDTO deleteDgfSubGroupLevel2(int dgfSubGroupLevel2Id, HttpServletRequest request);
    ApiResponseDTO deleteDgfSubGroupLevel3(int dgfSubGroupLevel3Id, HttpServletRequest request);
    ApiResponseDTO updateDgfGroup(DGFGroupDTO dgfGroupDTO, int dgfGroupId, HttpServletRequest request);
    ApiResponseDTO updateDgfSubGroupLevel1(DGFGroupDTO dgfGroupDTO, int dgfGroupId, HttpServletRequest request);
    ApiResponseDTO updateDgfSubGroupLevel2(DGFGroupDTO dgfGroupDTO, int dgfGroupId, HttpServletRequest request);
    ApiResponseDTO updateDgfSubGroupLevel3(DGFGroupDTO dgfGroupDTO, int dgfGroupId, HttpServletRequest request);
}
