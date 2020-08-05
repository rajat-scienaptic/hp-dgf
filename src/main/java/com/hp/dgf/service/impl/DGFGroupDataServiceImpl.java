package com.hp.dgf.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.constants.MapKeys;
import com.hp.dgf.constants.Variables;
import com.hp.dgf.dto.request.DGFGroupDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.*;
import com.hp.dgf.repository.*;
import com.hp.dgf.service.DGFGroupDataService;
import com.hp.dgf.service.UserValidationService;
import com.hp.dgf.utils.MonthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public final class DGFGroupDataServiceImpl implements DGFGroupDataService {
    @Autowired
    BusinessCategoryRepository businessCategoryRepository;
    @Autowired
    ProductLineRepository productLineRepository;
    @Autowired
    MonthService monthService;
    @Autowired
    DGFGroupsRepository dgfGroupsRepository;
    @Autowired
    DGFSubGroupLevel1Repository dgfSubGroupLevel1Repository;
    @Autowired
    DGFSubGroupLevel2Repository dgfSubGroupLevel2Repository;
    @Autowired
    DGFSubGroupLevel3Repository dgfSubGroupLevel3Repository;
    @Autowired
    DGFLogRepository dgfLogRepository;
    @Autowired
    DGFRateEntryRepository dgfRateEntryRepository;
    @Autowired
    UserValidationService userValidationService;

    //Initialized Object Mapper to Map Json Object
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public final List<Object> getDgfGroups(int businessCategoryId) {
        //Getting List of Business Categories
        BusinessCategory businessCategory = businessCategoryRepository
                .findById(businessCategoryId)
                .<CustomException>orElseThrow(() -> {
                    throw new CustomException("No Business Category Found for ID : " + businessCategoryId, HttpStatus.BAD_REQUEST);
                });

        //Initialized final response object
        List<Object> dgfGroupData = new LinkedList<>();
        //If business category exists then process it else throw error
        //Output Map
        Map<String, Object> outputMap = new LinkedHashMap<>();
        //Mapped Business Categories as JSON Node
        JsonNode businessCategoryNode = mapper.convertValue(businessCategory, JsonNode.class);
        //If a business category has children object
        if (businessCategoryNode.has(Variables.CHILDREN)) {
            //Get all children of a business category
            JsonNode businessSubCategoryNode = businessCategoryNode.get(Variables.CHILDREN);
            //If business sub category is not empty
            if (!businessSubCategoryNode.isEmpty()) {
                //Initializing columns map list
                List<Object> columnsMapList = new LinkedList<>();
                //Processing rest of the children of a business category
                for (int i = 0; i < businessSubCategoryNode.size(); i++) {
                    int businessSubCategoryId = Integer.parseInt(businessSubCategoryNode.get(i).get("id").toString());
                    List<Object> columnList = new LinkedList<>();
                    //Processing first children to add title object to final output response
                    if (i == 0) {
                        Map<String, Object> titleMap = new LinkedHashMap<>();
                        titleMap.put(Variables.TITLE, Variables.TITLE_VALUE);
                        titleMap.put(Variables.DATA_INDEX, Variables.TITLE_DATA_INDEX_VALUE);
                        titleMap.put(MapKeys.BUSINESS_SUB_CATEGORY_ID, businessSubCategoryId);
                        columnList.add(titleMap);
                    }

                    JsonNode columns = businessSubCategoryNode.get(i).get(Variables.COLUMNS);

                    //Processing product lines for each sub category
                    columns.forEach(column -> {
                        Map<String, Object> columnData = new LinkedHashMap<>();
                        columnData.put(Variables.TITLE, column.get(Variables.CODE));
                        columnData.put(Variables.DATA_INDEX, column.get(Variables.CODE));
                        columnData.put(MapKeys.BUSINESS_SUB_CATEGORY_ID, businessSubCategoryId);
                        columnData.put(MapKeys.PRODUCT_LINE_ID, column.get("id"));
                        columnList.add(columnData);
                    });

                    //Added columns to children map
                    Map<String, Object> columnsMap = new LinkedHashMap<>();
                    columnsMap.put("title", businessSubCategoryNode.get(i).get("name"));
                    columnsMap.put("children", columnList);

                    columnsMapList.add(columnsMap);
                }
                //end of business sub category loop

                outputMap.put("columns", columnsMapList);
                outputMap.put("data", getDGFDataObject(businessCategoryId));

                //Added columns object to the final output
                dgfGroupData.add(outputMap);
            }
        }

        //Returning DGF group data
        return dgfGroupData;
    }

    public final List<Object> getDGFDataObject(int businessCategoryId) {
        AtomicInteger id = new AtomicInteger(1);
        //Initialized Data Object
        List<Object> dataObject = new LinkedList<>();

        //Getting business category data based on id
        Optional<BusinessCategory> businessCategory = businessCategoryRepository.findById(businessCategoryId);

        businessCategory.ifPresent(bc -> {
            //Getting Set of All Dgf Groups
            Set<DGFGroups> dgfGroupsSet = bc.getDgfGroups();
            for (DGFGroups dgfGroup : dgfGroupsSet) {
                //Parent DGF Group
                Map<String, Object> dg = new LinkedHashMap<>();
                dg.put(MapKeys.KEY, id.getAndIncrement());
                dg.put("dgfGroupsId", dgfGroup.getId());
                dg.put(MapKeys.IS_ACTIVE, dgfGroup.getIsActive());
                dg.put(MapKeys.MODIFIED_BY, dgfGroup.getModifiedBy());
                dg.put(MapKeys.BASE_RATE, dgfGroup.getBaseRate());
                JsonNode dgfGroupObject = mapper.convertValue(dgfGroup, JsonNode.class);

                //Processing DGF Sub Group 1
                JsonNode dgfSubGroup1Object = dgfGroupObject.get(Variables.CHILDREN);
                List<Map<String, Object>> dg1List = new LinkedList<>();
                Map<String, Object> addDgfSubGroup1Map = new LinkedHashMap<>();

                dgfSubGroup1Object.forEach(dgfSubGroup1 -> {
                    Map<String, Object> dg1 = new LinkedHashMap<>();
                    Map<String, Object> addDgfSubGroup2Map = new LinkedHashMap<>();

                    dg1.put(MapKeys.KEY, id.getAndIncrement());
                    dg1.put("dgfSubGroupLevel1Id", dgfSubGroup1.get("id"));
                    dg1.put(MapKeys.IS_ACTIVE, dgfSubGroup1.get(Variables.IS_ACTIVE));
                    dg1.put(MapKeys.MODIFIED_BY, dgfSubGroup1.get(Variables.MODIFIED_BY));
                    dg1.put(MapKeys.BASE_RATE, dgfSubGroup1.get(Variables.BASE_RATE));

                    //Processing DGF Sub Group 2
                    JsonNode dgfSubGroup2Object = dgfSubGroup1.get(Variables.CHILDREN);
                    List<Map<String, Object>> dg2List = new LinkedList<>();
                    dgfSubGroup2Object.forEach(dgfSubGroup2 -> {
                        Map<String, Object> dg2 = new LinkedHashMap<>();
                        Map<String, Object> addDgfSubGroup3Map = new LinkedHashMap<>();

                        dg2.put(MapKeys.KEY, id.getAndIncrement());
                        dg2.put("dgfSubGroupLevel2Id", dgfSubGroup2.get("id"));
                        dg2.put(MapKeys.IS_ACTIVE, dgfSubGroup2.get(Variables.IS_ACTIVE));
                        dg2.put(MapKeys.MODIFIED_BY, dgfSubGroup2.get(Variables.MODIFIED_BY));
                        dg2.put(MapKeys.BASE_RATE, dgfSubGroup2.get(Variables.BASE_RATE));

                        //Processing DGF Sub Group 2 PLs (Columns)
                        JsonNode subGroup2Data = dgfSubGroup2.get(Variables.COLUMNS);
                        subGroup2Data.forEach(data -> {
                            Map<String, Object> dg2DataValues = new LinkedHashMap<>();
                            dg2DataValues.put(MapKeys.QUARTER, monthService.getQuarter());
                            dg2DataValues.put(MapKeys.VALUE, new LinkedList<>(Collections.singletonList(data.get(Variables.DGF_RATE))));


                            int plId = Integer.parseInt(data.get("productLineId").toString());

                            ProductLine pl = productLineRepository.findById(plId)
                                    .<CustomException>orElseThrow(() -> {
                                        throw new CustomException("PL Not Found", HttpStatus.BAD_REQUEST);
                                    });

                            String k2 = pl.getCode().replaceAll("\"", "");
                            dg2DataValues.put(MapKeys.BUSINESS_SUB_CATEGORY_ID, productLineRepository.getBusinessSubCategoryIdByPLId(k2));
                            dg2DataValues.put(MapKeys.DGF_RATE_ENTRY_ID, data.get("id"));
                            dg2DataValues.put(MapKeys.PRODUCT_LINE_ID, plId);
                            dg2.put(k2, dg2DataValues);
                        });

                        //Processing DGF Sub Group 3
                        JsonNode dgSubGroup3Object = dgfSubGroup2.get(Variables.CHILDREN);
                        List<Map<String, Object>> dg3List = new LinkedList<>();
                        dgSubGroup3Object.forEach(dgfSubGroup3 -> {
                            Map<String, Object> dg3 = new LinkedHashMap<>();
                            dg3.put(MapKeys.KEY, id.getAndIncrement());
                            dg3.put("dgfSubGroupLevel3Id", dgfSubGroup3.get("id"));
                            dg3.put(MapKeys.IS_ACTIVE, dgfSubGroup3.get(Variables.IS_ACTIVE));
                            dg3.put(MapKeys.MODIFIED_BY, dgfSubGroup3.get(Variables.MODIFIED_BY));
                            dg3.put(MapKeys.BASE_RATE, dgfSubGroup3.get(Variables.BASE_RATE));

                            //Processing DGF Sub Group 3 PLs (Columns)
                            JsonNode subGroup3Data = dgfSubGroup3.get(Variables.COLUMNS);
                            subGroup3Data.forEach(data -> {
                                Map<String, Object> dg3DataValues = new LinkedHashMap<>();
                                dg3DataValues.put(MapKeys.QUARTER, monthService.getQuarter());
                                dg3DataValues.put(MapKeys.VALUE, new LinkedList<>(Collections.singletonList(data.get("dgfRate"))));

                                int plId = Integer.parseInt(data.get("productLineId").toString());
                                ProductLine pl = productLineRepository.findById(plId)
                                        .<CustomException>orElseThrow(() -> {
                                            throw new CustomException("PL Not Found", HttpStatus.BAD_REQUEST);
                                        });

                                String k3 = pl.getCode().replaceAll("\"", "");
                                dg3DataValues.put(MapKeys.BUSINESS_SUB_CATEGORY_ID, productLineRepository.getBusinessSubCategoryIdByPLId(k3));
                                dg3DataValues.put(MapKeys.DGF_RATE_ENTRY_ID, data.get("id"));
                                dg3DataValues.put(MapKeys.PRODUCT_LINE_ID, plId);
                                dg3.put(k3, dg3DataValues);
                            });
                            dg3List.add(dg3);
                        });

                        addDgfSubGroup3Map.put(MapKeys.KEY, id.getAndIncrement());
                        addDgfSubGroup3Map.put("dgfSubGroup2LevelId", dgfSubGroup2.get("id"));
                        addDgfSubGroup3Map.put(MapKeys.BASE_RATE, Variables.ADD_A_SUB_GROUP);

                        dg3List.add(addDgfSubGroup3Map);

                        dg2.put(MapKeys.CHILDREN, dg3List);
                        dg2List.add(dg2);
                    });

                    addDgfSubGroup2Map.put(MapKeys.KEY, id.getAndIncrement());
                    addDgfSubGroup2Map.put("dgfSubGroup1LevelId", dgfSubGroup1.get("id"));
                    addDgfSubGroup2Map.put(MapKeys.BASE_RATE, Variables.ADD_A_SUB_GROUP);

                    dg2List.add(addDgfSubGroup2Map);

                    dg1.put(MapKeys.CHILDREN, dg2List);
                    dg1List.add(dg1);
                });

                addDgfSubGroup1Map.put(MapKeys.KEY, id.getAndIncrement());
                addDgfSubGroup1Map.put("dgfGroupsId", dgfGroup.getId());
                addDgfSubGroup1Map.put(MapKeys.BASE_RATE, Variables.ADD_A_SUB_GROUP);

                dg1List.add(addDgfSubGroup1Map);

                dg.put(MapKeys.CHILDREN, dg1List);
                dataObject.add(dg);
            }
        });

        return dataObject;
    }

    @Override
    public ApiResponseDTO addDgfGroup(DGFGroupDTO dgfGroupDTO, HttpServletRequest request, String cookie) {
        try {
            DGFGroups dgfGroups = dgfGroupsRepository.getDGFGroup(dgfGroupDTO.getBusinessCategoryId(), dgfGroupDTO.getName());

            if (dgfGroups != null) {
                if (dgfGroups.getIsActive() == 1) {
                    throw new CustomException("DGF Group with name : " + dgfGroupDTO.getName() +
                            " already exists for Business Category with id : " + dgfGroupDTO.getBusinessCategoryId(),
                            HttpStatus.BAD_REQUEST);
                } else if (dgfGroups.getIsActive() == 0) {
                    dgfGroups.setIsActive((byte) 1);
                    dgfGroupsRepository.save(dgfGroups);
                    if (request != null) {
                        dgfLogRepository.save(DGFLogs.builder()
                                .ip(request.getRemoteAddr())
                                .endpoint(request.getRequestURI())
                                .type(request.getMethod())
                                .status(Variables.SUCCESS)
                                .username(userValidationService.getUserNameFromCookie(cookie))
                                .message("DGF Group with name : " + dgfGroupDTO.getName() + " and id : " + dgfGroups.getId() + " has been successfully reactivated !")
                                .createTime(LocalDateTime.now())
                                .build());
                    }
                    return ApiResponseDTO.builder()
                            .timestamp(LocalDateTime.now())
                            .message("DGF Group with name : " + dgfGroupDTO.getName() + " and id : " + dgfGroups.getId() + " has been successfully reactivated !")
                            .status(HttpStatus.CREATED.value())
                            .build();
                }
            }

            int dgfGroupId = dgfGroupsRepository.save(DGFGroups.builder()
                    .baseRate(dgfGroupDTO.getName())
                    .businessCategoryId(dgfGroupDTO.getBusinessCategoryId())
                    .modifiedBy(userValidationService.getUserNameFromCookie(cookie))
                    .isActive((byte) 1)
                    .lastModifiedTimestamp(LocalDateTime.now())
                    .build()).getId();


            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Group with name : " + dgfGroupDTO.getName() + " and id : " + dgfGroupId + " has been successfully created !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .message("DGF Group with name : " + dgfGroupDTO.getName() + " and id : " + dgfGroupId + " has been successfully created !")
                    .status(HttpStatus.CREATED.value())
                    .build();

        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO addDgfSubGroupLevel1(DGFGroupDTO dgfGroupDTO, HttpServletRequest request, String cookie) {
        try {
            DGFSubGroupLevel1 dgfSubGroupLevel1 = dgfSubGroupLevel1Repository.getDGFSubGroupLevel1(dgfGroupDTO.getDgfGroupsId(), dgfGroupDTO.getName());

            if (dgfSubGroupLevel1 != null) {
                if (dgfSubGroupLevel1.getIsActive() == 1) {
                    throw new CustomException("DGF Sub Group Level 1 with name : " + dgfGroupDTO.getName() +
                            " already exists for DGF Group with Id : " + dgfGroupDTO.getDgfGroupsId(),
                            HttpStatus.BAD_REQUEST);
                } else if (dgfSubGroupLevel1.getIsActive() == 0) {
                    dgfSubGroupLevel1.setIsActive((byte) 1);
                    dgfSubGroupLevel1Repository.save(dgfSubGroupLevel1);
                    if (request != null) {
                        dgfLogRepository.save(DGFLogs.builder()
                                .ip(request.getRemoteAddr())
                                .endpoint(request.getRequestURI())
                                .type(request.getMethod())
                                .status(Variables.SUCCESS)
                                .username(userValidationService.getUserNameFromCookie(cookie))
                                .message("DGF Sub Group Level 1 with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel1.getId() + " has been successfully reactivated !")
                                .createTime(LocalDateTime.now())
                                .build());
                    }
                    return ApiResponseDTO.builder()
                            .timestamp(LocalDateTime.now())
                            .message("DGF Group with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel1.getId() + " has been successfully reactivated !")
                            .status(HttpStatus.CREATED.value())
                            .build();
                }
            }

            int dgfSubGroupLevel1Id = dgfSubGroupLevel1Repository.save(DGFSubGroupLevel1.builder()
                    .baseRate(dgfGroupDTO.getName())
                    .dgfGroupsId(dgfGroupDTO.getDgfGroupsId())
                    .modifiedBy(userValidationService.getUserNameFromCookie(cookie))
                    .isActive((byte) 1)
                    .lastModifiedTimestamp(LocalDateTime.now())
                    .build()).getId();

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Sub Group Level 1 with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel1Id + " has been successfully created !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .message("DGF Sub Group Level 1 with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel1Id + " has been successfully created !")
                    .status(HttpStatus.CREATED.value())
                    .build();

        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO addDgfSubGroupLevel2(DGFGroupDTO dgfGroupDTO, HttpServletRequest request, String cookie) {
        try {
            DGFSubGroupLevel2 dgfSubGroupLevel2 = dgfSubGroupLevel2Repository.getDGFSubGroupLevel2(dgfGroupDTO.getDgfSubGroupLevel1Id(), dgfGroupDTO.getName());

            if (dgfSubGroupLevel2 != null) {
                if (dgfSubGroupLevel2.getIsActive() == 1) {
                    throw new CustomException("DGF Sub Group Level 2 with name : " + dgfGroupDTO.getName() +
                            " already exists for DGF Sub Group Level 1 with Id : " + dgfGroupDTO.getDgfSubGroupLevel1Id(),
                            HttpStatus.BAD_REQUEST);
                } else if (dgfSubGroupLevel2.getIsActive() == 0) {
                    dgfSubGroupLevel2.setIsActive((byte) 1);
                    dgfSubGroupLevel2Repository.save(dgfSubGroupLevel2);
                    if (request != null) {
                        dgfLogRepository.save(DGFLogs.builder()
                                .ip(request.getRemoteAddr())
                                .endpoint(request.getRequestURI())
                                .type(request.getMethod())
                                .status(Variables.SUCCESS)
                                .username(userValidationService.getUserNameFromCookie(cookie))
                                .message("DGF Sub Group Level 2 with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel2.getId() + " has been successfully reactivated !")
                                .createTime(LocalDateTime.now())
                                .build());
                    }
                    return ApiResponseDTO.builder()
                            .timestamp(LocalDateTime.now())
                            .message("DGF Group with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel2.getId() + " has been successfully reactivated !")
                            .status(HttpStatus.CREATED.value())
                            .build();
                }
            }
            int dgfSubGroupLevel2Id = dgfSubGroupLevel2Repository.save(DGFSubGroupLevel2.builder()
                    .baseRate(dgfGroupDTO.getName())
                    .dgfSubGroupLevel1Id(dgfGroupDTO.getDgfSubGroupLevel1Id())
                    .modifiedBy(userValidationService.getUserNameFromCookie(cookie))
                    .isActive((byte) 1)
                    .lastModifiedTimestamp(LocalDateTime.now())
                    .build()).getId();

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Sub Group Level 2 with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel2Id + " has been successfully created !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .message("DGF Sub Group Level 2 with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel2Id + " has been successfully created !")
                    .status(HttpStatus.CREATED.value())
                    .build();

        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO addDgfSubGroupLevel3(DGFGroupDTO dgfGroupDTO, HttpServletRequest request, String cookie) {
        try {
            DGFSubGroupLevel3 dgfSubGroupLevel3 = dgfSubGroupLevel3Repository.getDGFSubGroupLevel3(dgfGroupDTO.getDgfSubGroupLevel2Id(), dgfGroupDTO.getName());

            if (dgfSubGroupLevel3 != null) {
                if (dgfSubGroupLevel3.getIsActive() == 1) {
                    throw new CustomException("DGF Sub Group Level 3 with name : " + dgfGroupDTO.getName() +
                            " already exists for DGF Sub Group Level 2 with Id : " + dgfGroupDTO.getDgfSubGroupLevel1Id(),
                            HttpStatus.BAD_REQUEST);
                } else if (dgfSubGroupLevel3.getIsActive() == 0) {
                    dgfSubGroupLevel3.setIsActive((byte) 1);
                    dgfSubGroupLevel3Repository.save(dgfSubGroupLevel3);
                    if (request != null) {
                        dgfLogRepository.save(DGFLogs.builder()
                                .ip(request.getRemoteAddr())
                                .endpoint(request.getRequestURI())
                                .type(request.getMethod())
                                .status(Variables.SUCCESS)
                                .username(userValidationService.getUserNameFromCookie(cookie))
                                .message("DGF Sub Group Level 3 with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel3.getId() + " has been successfully reactivated !")
                                .createTime(LocalDateTime.now())
                                .build());
                    }
                    return ApiResponseDTO.builder()
                            .timestamp(LocalDateTime.now())
                            .message("DGF Group with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel3.getId() + " has been successfully reactivated !")
                            .status(HttpStatus.CREATED.value())
                            .build();
                }
            }

            int dgfSubGroupLevel3Id = dgfSubGroupLevel3Repository.save(DGFSubGroupLevel3.builder()
                    .baseRate(dgfGroupDTO.getName())
                    .dgfSubGroupLevel2Id(dgfGroupDTO.getDgfSubGroupLevel2Id())
                    .modifiedBy(userValidationService.getUserNameFromCookie(cookie))
                    .isActive((byte) 1)
                    .lastModifiedTimestamp(LocalDateTime.now())
                    .build()).getId();

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Sub Group Level 3 with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel3Id + " has been successfully created !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .message("DGF Sub Group Level 3 with name : " + dgfGroupDTO.getName() + " and id : " + dgfSubGroupLevel3Id + " has been successfully created !")
                    .status(HttpStatus.CREATED.value())
                    .build();

        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message(e.getMessage())
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO deleteDgfGroup(int dgfGroupId, HttpServletRequest request, String cookie) {
        try {
            DGFGroups dgfGroups = dgfGroupsRepository.findById(dgfGroupId)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("Deletion failed, DGF Group not found for id : " + dgfGroupId, HttpStatus.BAD_REQUEST);
                    });
            dgfGroups.setIsActive((byte) 0);
            dgfGroupsRepository.save(dgfGroups);

            JsonNode dgfGroupNode = mapper.convertValue(dgfGroups, JsonNode.class);

            JsonNode dgfSubGroupLevel1Node = dgfGroupNode.get(Variables.CHILDREN);

            for (JsonNode dgfSubGroupLevel1 : dgfSubGroupLevel1Node) {
                int dgfSubGroupLevel1Id = Integer.parseInt(dgfSubGroupLevel1.get("id").toString());
                deleteDgfSubGroupLevel1(dgfSubGroupLevel1Id, request, cookie);
            }

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .status(Variables.SUCCESS)
                        .message("DGF Group with id : " + dgfGroupId + " has been successfully deleted !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .message("DGF Group with id : " + dgfGroupId + " has been successfully deleted !")
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.OK.value())
                    .build();
        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO deleteDgfSubGroupLevel1(int dgfSubGroupLevel1Id, HttpServletRequest request, String cookie) {
        try {
            DGFSubGroupLevel1 dgfSubGroupLevel1 = dgfSubGroupLevel1Repository.findById(dgfSubGroupLevel1Id)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("Deletion failed, DGF Sub Group Level 1 not found for id : " + dgfSubGroupLevel1Id, HttpStatus.BAD_REQUEST);
                    });
            dgfSubGroupLevel1.setIsActive((byte) 0);
            dgfSubGroupLevel1Repository.save(dgfSubGroupLevel1);

            JsonNode dgfSubGroupLevel1Node = mapper.convertValue(dgfSubGroupLevel1, JsonNode.class);

            JsonNode dgfSubGroupLevel2Node = dgfSubGroupLevel1Node.get(Variables.CHILDREN);

            for (JsonNode dgfSubGroupLevel2 : dgfSubGroupLevel2Node) {
                int dgfSubGroupLevel2Id = Integer.parseInt(dgfSubGroupLevel2.get("id").toString());
                deleteDgfSubGroupLevel2(dgfSubGroupLevel2Id, request, cookie);
            }

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Sub Group Level 1 with id : " + dgfSubGroupLevel1Id + " has been successfully deleted !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .message("DGF Sub Group Level 1 with id : " + dgfSubGroupLevel1Id + " has been successfully deleted !")
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.OK.value())
                    .build();
        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO deleteDgfSubGroupLevel2(int dgfSubGroupLevel2Id, HttpServletRequest request, String cookie) {
        try {
            DGFSubGroupLevel2 dgfSubGroupLevel2 = dgfSubGroupLevel2Repository.findById(dgfSubGroupLevel2Id)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("Deletion failed, DGF Sub Group Level 2 not found for id : " + dgfSubGroupLevel2Id, HttpStatus.BAD_REQUEST);
                    });
            dgfSubGroupLevel2.setIsActive((byte) 0);

            List<DGFRateEntry> dgfRateEntryList = dgfRateEntryRepository.findEntryIdByDgfSubGroupLevel2Id(dgfSubGroupLevel2Id);

            dgfRateEntryList.forEach(dgfRateEntry -> {
                dgfRateEntry.setDgfSubGroupLevel2Id(null);
                dgfRateEntryRepository.save(dgfRateEntry);
            });

            dgfSubGroupLevel2Repository.save(dgfSubGroupLevel2);

            JsonNode dgfSubGroupLevel2Node = mapper.convertValue(dgfSubGroupLevel2, JsonNode.class);

            JsonNode dgfSubGroupLevel3Node = dgfSubGroupLevel2Node.get(Variables.CHILDREN);

            for (JsonNode dgfSubGroupLevel3 : dgfSubGroupLevel3Node) {
                int dgfSubGroupLevel3Id = Integer.parseInt(dgfSubGroupLevel3.get("id").toString());
                deleteDgfSubGroupLevel3(dgfSubGroupLevel3Id, request, cookie);
            }

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Sub Group Level 2 with id : " + dgfSubGroupLevel2Id + " has been successfully deleted !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .message("DGF Sub Group Level 2 with id : " + dgfSubGroupLevel2Id + " has been successfully deleted !")
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.OK.value())
                    .build();
        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO deleteDgfSubGroupLevel3(int dgfSubGroupLevel3Id, HttpServletRequest request, String cookie) {
        try {
            DGFSubGroupLevel3 dgfSubGroupLevel3 = dgfSubGroupLevel3Repository.findById(dgfSubGroupLevel3Id)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("Deletion failed, DGF Sub Group Level 3 not found for id : " + dgfSubGroupLevel3Id, HttpStatus.BAD_REQUEST);
                    });
            dgfSubGroupLevel3.setIsActive((byte) 0);

            List<DGFRateEntry> dgfRateEntryList = dgfRateEntryRepository.findEntryIdByDgfSubGroupLevel3Id(dgfSubGroupLevel3Id);

            dgfRateEntryList.forEach(dgfRateEntry -> {
                dgfRateEntry.setDgfSubGroupLevel3Id(null);
                dgfRateEntryRepository.save(dgfRateEntry);
            });

            dgfSubGroupLevel3Repository.save(dgfSubGroupLevel3);

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Sub Group Level 3 with id : " + dgfSubGroupLevel3Id + " has been successfully deleted !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .message("DGF Sub Group Level 3 with id : " + dgfSubGroupLevel3Id + " has been successfully deleted !")
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.OK.value())
                    .build();
        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO updateDgfGroup(DGFGroupDTO dgfGroupDTO, int dgfGroupId, HttpServletRequest request, String cookie) {
        try {
            DGFGroups dgfGroups = dgfGroupsRepository.findById(dgfGroupId)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("DGF Group not found for id : " + dgfGroupId, HttpStatus.BAD_REQUEST);
                    });
            dgfGroups.setBaseRate(dgfGroupDTO.getName());

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .status(Variables.SUCCESS)
                        .message("DGF Group with id : " + dgfGroupId + " has been successfully updated !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            dgfGroupsRepository.save(dgfGroups);
            return ApiResponseDTO.builder()
                    .message("DGF Group with id : " + dgfGroupId + " has been successfully updated !")
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.OK.value())
                    .build();
        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO updateDgfSubGroupLevel1(DGFGroupDTO dgfGroupDTO, int dgfSubGroupLevel1Id, HttpServletRequest request, String cookie) {
        try {
            DGFSubGroupLevel1 dgfSubGroupLevel1 = dgfSubGroupLevel1Repository.findById(dgfSubGroupLevel1Id)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 1 not found for id : " + dgfSubGroupLevel1Id, HttpStatus.BAD_REQUEST);
                    });
            dgfSubGroupLevel1.setBaseRate(dgfGroupDTO.getName());

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Sub Group Level 1 with id : " + dgfSubGroupLevel1Id + " has been successfully updated !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            dgfSubGroupLevel1Repository.save(dgfSubGroupLevel1);

            return ApiResponseDTO.builder()
                    .message("DGF Sub Group Level 1 with id : " + dgfSubGroupLevel1Id + " has been successfully updated !")
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.OK.value())
                    .build();
        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO updateDgfSubGroupLevel2(DGFGroupDTO dgfGroupDTO, int dgfSubGroupLevel2Id, HttpServletRequest request, String cookie) {
        try {
            DGFSubGroupLevel2 dgfSubGroupLevel2 = dgfSubGroupLevel2Repository.findById(dgfSubGroupLevel2Id)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 2 not found for id : " + dgfSubGroupLevel2Id, HttpStatus.BAD_REQUEST);
                    });
            dgfSubGroupLevel2.setBaseRate(dgfGroupDTO.getName());

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Sub Group Level 2 with id : " + dgfSubGroupLevel2Id + " has been successfully updated !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            dgfSubGroupLevel2Repository.save(dgfSubGroupLevel2);

            return ApiResponseDTO.builder()
                    .message("DGF Sub Group Level 2 with id : " + dgfSubGroupLevel2Id + " has been successfully updated !")
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.OK.value())
                    .build();
        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO updateDgfSubGroupLevel3(DGFGroupDTO dgfGroupDTO, int dgfSubGroupLevel3Id, HttpServletRequest request, String cookie) {
        try {
            DGFSubGroupLevel3 dgfSubGroupLevel3 = dgfSubGroupLevel3Repository.findById(dgfSubGroupLevel3Id)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 3 not found for id : " + dgfSubGroupLevel3Id, HttpStatus.BAD_REQUEST);
                    });
            dgfSubGroupLevel3.setBaseRate(dgfGroupDTO.getName());

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Sub Group Level 3 with id : " + dgfSubGroupLevel3Id + " has been successfully updated !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            dgfSubGroupLevel3Repository.save(dgfSubGroupLevel3);

            return ApiResponseDTO.builder()
                    .message("DGF Sub Group Level 3 with id : " + dgfSubGroupLevel3Id + " has been successfully updated !")
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.OK.value())
                    .build();
        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message(e.getMessage())
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }
}
