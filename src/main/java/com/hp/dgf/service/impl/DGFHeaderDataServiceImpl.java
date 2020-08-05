package com.hp.dgf.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.constants.MapKeys;
import com.hp.dgf.constants.Variables;
import com.hp.dgf.dto.request.AddPLRequestDTO;
import com.hp.dgf.dto.request.UpdatePLRequestDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.BusinessCategory;
import com.hp.dgf.model.BusinessSubCategory;
import com.hp.dgf.model.DGFLogs;
import com.hp.dgf.model.ProductLine;
import com.hp.dgf.repository.BusinessCategoryRepository;
import com.hp.dgf.repository.DGFLogRepository;
import com.hp.dgf.repository.ProductLineRepository;
import com.hp.dgf.service.DGFHeaderDataService;
import com.hp.dgf.service.UserValidationService;
import com.hp.dgf.utils.MonthService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

@Service
public class DGFHeaderDataServiceImpl implements DGFHeaderDataService {
    @Autowired
    ProductLineRepository productLineRepository;
    @Autowired
    DGFLogRepository dgfLogRepository;
    @Autowired
    BusinessCategoryRepository businessCategoryRepository;
    @Autowired
    MonthService monthService;
    @Autowired
    UserValidationService userValidationService;

    @Override
    public final List<Object> getHeaderData(int businessCategoryId) {
        final BusinessCategory businessCategory = businessCategoryRepository
                .findById(businessCategoryId)
                .<CustomException>orElseThrow(() -> {
                    throw new CustomException("Business category with id : " + businessCategoryId + " does not exists !", HttpStatus.NOT_FOUND);
                });

        final ObjectMapper mapper = new ObjectMapper();
        final List<Object> headerData = new LinkedList<>();

        //Output Map
        final Map<String, Object> outputMap = new LinkedHashMap<>();
        final List<Object> columnsMapList = new LinkedList<>();
        final List<Object> businessMapList = new LinkedList<>();

        //Mapped Business Categories as JSON Node
        final JsonNode businessCategoryNode = mapper.convertValue(businessCategory, JsonNode.class);
        //If a business category has children object
        if (businessCategoryNode.has(Variables.CHILDREN)) {
            //Get all children of a business category
            final JsonNode businessSubCategoryNode = businessCategoryNode.get(Variables.CHILDREN);
            //If business sub category is not empty
            if (!businessSubCategoryNode.isEmpty()) {
                //Processing rest of the children of a business category
                for (int i = 0; i < businessSubCategoryNode.size(); i++) {
                    List<Object> columnList = new LinkedList<>();
                    //Processing first children to add title object to final output response
                    int businessSubCategoryId = Integer.parseInt(businessSubCategoryNode.get(i).get("id").toString());

                    if (i == 0) {
                        Map<String, Object> titleMap = new LinkedHashMap<>();
                        titleMap.put(Variables.TITLE, Variables.TITLE_VALUE);
                        titleMap.put(Variables.DATA_INDEX, Variables.TITLE_DATA_INDEX_VALUE);
                        titleMap.put(MapKeys.BUSINESS_SUB_CATEGORY_ID, businessSubCategoryId);
                        columnList.add(titleMap);
                    }

                    Map<String, Object> iconMap = new LinkedHashMap<>();
                    iconMap.put(Variables.TITLE, Variables.ICON_VALUE);
                    iconMap.put(Variables.DATA_INDEX, "");
                    iconMap.put(MapKeys.BUSINESS_SUB_CATEGORY_ID, businessSubCategoryId);
                    columnList.add(iconMap);

                    JsonNode columns = businessSubCategoryNode.get(i).get(Variables.COLUMNS);

                    //Processing product lines for each sub category
                    columns.forEach(column -> {
                        Map<String, Object> columnData = new LinkedHashMap<>();
                        columnData.put(Variables.TITLE, column.get(Variables.CODE));
                        columnData.put(Variables.DATA_INDEX, column.get(Variables.CODE));
                        columnData.put(MapKeys.BUSINESS_SUB_CATEGORY_ID, businessSubCategoryId);
                        columnData.put("id", column.get("id"));
                        columnList.add(columnData);
                    });

                    //Added columns to children map
                    Map<String, Object> columnsMap = new LinkedHashMap<>();
                    columnsMap.put("title", businessSubCategoryNode.get(i).get("name"));
                    columnsMap.put("children", columnList);

                    columnsMapList.add(columnsMap);
                }
                //end of business sub category loop
            }
        }

        final List<BusinessCategory> businessCategoryList = businessCategoryRepository.findAll();

        businessCategoryList.forEach(bc -> {
            //Business Category Map
            final Map<String, Object> businessMap = new LinkedHashMap<>();
            businessMap.put("id", bc.getId());
            businessMap.put("label", bc.getName());
            businessMapList.add(businessMap);
        });

        outputMap.put("columns", columnsMapList);
        outputMap.put("data", getHeaderDataObject(businessCategoryId));
        outputMap.put("business", businessMapList);

        //Added columns object to the final output
        headerData.add(outputMap);

        return headerData;
    }

    public final List<Object> getHeaderDataObject(int businessCategoryId) {
        //Initialized Data Object
        final List<Object> dataObject = new LinkedList<>();

        final LocalDate currentDate = LocalDate.now();
        final String year = String.valueOf(currentDate.getYear());
        final String fyYear = year.substring(year.length() - 2);

        final int quarter = monthService.getQuarter();
        final String headerBaseRateTitle = "BASE RATES FY" + fyYear;

        //Getting business category data based on id
        final BusinessCategory businessCategory = businessCategoryRepository
                .findById(businessCategoryId)
                .<CustomException>orElseThrow(() -> {
                    throw new CustomException("No data found for id : " + businessCategoryId, HttpStatus.NOT_FOUND);
                });

        final Map<String, Object> headerBaseRateMap = new LinkedHashMap<>();
        final Map<String, Object> effectiveFirstQuarterMap = new LinkedHashMap<>();
        final Map<String, Object> effectiveSecondQuarterMap = new LinkedHashMap<>();
        final Map<String, Object> effectiveThirdQuarterMap = new LinkedHashMap<>();
        final Map<String, Object> effectiveFourthQuarterMap = new LinkedHashMap<>();

        headerBaseRateMap.put(MapKeys.BASE_RATE, headerBaseRateTitle);

        final String effectiveFirstQuarterTitle = "Effective " + monthService.getMonthRange(quarter) + " " +
                monthService.getYear() + " " + "(" + monthService.getQuarterName(quarter) + ")";

        effectiveFirstQuarterMap.put(MapKeys.BASE_RATE, effectiveFirstQuarterTitle);

        final String effectiveSecondQuarterTitle = "Effective " + monthService.getMonthRange(quarter + 1) + " " +
                year + " " + "(" + monthService.getQuarterName(quarter + 1) + ")";

        effectiveSecondQuarterMap.put(MapKeys.BASE_RATE, effectiveSecondQuarterTitle);

        final String effectiveThirdQuarterTitle = "Effective " + monthService.getMonthRange(quarter + 2) + " " +
                year + " " + "(" + monthService.getQuarterName(quarter + 2) + ")";

        effectiveThirdQuarterMap.put(MapKeys.BASE_RATE, effectiveThirdQuarterTitle);


        final String effectiveFourthQuarterTitle = "Effective " + monthService.getMonthRange(quarter + 3) + " " +
                year + " " + "(" + monthService.getQuarterName(quarter + 3) + ")";

        effectiveFourthQuarterMap.put(MapKeys.BASE_RATE, effectiveFourthQuarterTitle);

        //Getting Set of All Sub Categories
        final Set<BusinessSubCategory> businessSubCategorySet = businessCategory.getChildren();

        businessSubCategorySet.forEach(businessSubCategory -> {
            //Getting list of PL data for each sub category
            final Set<ProductLine> productLineSet = businessSubCategory.getColumns();
            //Iterating over list of product lines
            productLineSet.forEach(productLine -> {
                final Map<String, Object> plMap = new LinkedHashMap<>();
                plMap.put(MapKeys.QUARTER, "");
                plMap.put(MapKeys.VALUE, new LinkedList<>(Collections.singletonList(productLine.getBaseRate())));
                plMap.put(MapKeys.BUSINESS_SUB_CATEGORY_ID, businessSubCategory.getId());
                String pl = productLine.getCode().replaceAll("\"", "");
                headerBaseRateMap.put(pl, plMap);

                final Map<String, Object> quartersMap1 = new LinkedHashMap<>();
                quartersMap1.put(MapKeys.QUARTER, monthService.getQuarter());
                quartersMap1.put(MapKeys.VALUE, new LinkedList<>());
                quartersMap1.put(MapKeys.BUSINESS_SUB_CATEGORY_ID, businessSubCategory.getId());

                final Map<String, Object> quartersMap2 = new LinkedHashMap<>();
                quartersMap2.put(MapKeys.QUARTER, monthService.getQuarter() + 1);
                quartersMap2.put(MapKeys.VALUE, new LinkedList<>());
                quartersMap2.put(MapKeys.BUSINESS_SUB_CATEGORY_ID, businessSubCategory.getId());

                effectiveFirstQuarterMap.put(pl, quartersMap1);
                effectiveSecondQuarterMap.put(pl, quartersMap2);
            });
        });

        dataObject.add(headerBaseRateMap);
        dataObject.add(effectiveFirstQuarterMap);
        dataObject.add(effectiveSecondQuarterMap);

        return dataObject;
    }

    @Override
    public final ApiResponseDTO addPL(final AddPLRequestDTO addPlRequestDTO,
                                      final HttpServletRequest request,
                                      final String cookie) {
        try {
            ProductLine productLine = productLineRepository.checkIfPlExists(addPlRequestDTO.getCode());

            if (productLine != null) {
                if (productLine.getIsActive() == 1) {
                    throw new CustomException("PL with code : " + addPlRequestDTO.getCode() + " already exists !", HttpStatus.BAD_REQUEST);
                } else if (productLine.getIsActive() == 0) {
                    productLine.setIsActive((byte) 1);
                    productLine.setBaseRate(addPlRequestDTO.getBaseRate());
                    productLineRepository.save(productLine);
                    if (request != null) {
                        dgfLogRepository.save(DGFLogs.builder()
                                .ip(request.getRemoteAddr())
                                .endpoint(request.getRequestURI())
                                .type(request.getMethod())
                                .status(Variables.SUCCESS)
                                .username(userValidationService.getUserNameFromCookie(cookie))
                                .message("PL with code : " + addPlRequestDTO.getCode() + " and id : " + productLine.getId() + " has been successfully reactivated !")
                                .createTime(LocalDateTime.now())
                                .build());
                    }
                    return ApiResponseDTO.builder()
                            .timestamp(LocalDateTime.now())
                            .status(HttpStatus.CREATED.value())
                            .message("PL with code : " + addPlRequestDTO.getCode() + " and id : " + productLine.getId() + " has been successfully reactivated !")
                            .build();
                }
            }

            final int productLineId = productLineRepository.save(ProductLine.builder()
                    .code(addPlRequestDTO.getCode())
                    .businessSubCategoryId(addPlRequestDTO.getBusinessSubCategoryId())
                    .baseRate(addPlRequestDTO.getBaseRate())
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
                        .message("PL with code : " + addPlRequestDTO.getCode() + " and id : " + productLineId + " has been successfully created !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.CREATED.value())
                    .message("PL with code : " + addPlRequestDTO.getCode() + " and id : " + productLineId + " has been successfully created !")
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
    public final ApiResponseDTO updatePL(final UpdatePLRequestDTO updatePLRequestDTO,
                                         final int productLineId,
                                         final HttpServletRequest request,
                                         final String cookie) {
        try {
            ProductLine productLine = productLineRepository
                    .findById(productLineId)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("Update failed, PL with id : " + productLineId + " doesn't exist !", HttpStatus.NOT_FOUND);
                    });

            if (updatePLRequestDTO.getCode() == null && updatePLRequestDTO.getBaseRate() != null) {
                productLine.setBaseRate(updatePLRequestDTO.getBaseRate());
                productLine.setLastModifiedTimestamp(LocalDateTime.now());
            } else if (updatePLRequestDTO.getBaseRate() == null && updatePLRequestDTO.getCode() != null) {
                productLine.setCode(updatePLRequestDTO.getCode());
                productLine.setLastModifiedTimestamp(LocalDateTime.now());
            } else if (updatePLRequestDTO.getCode() != null && updatePLRequestDTO.getBaseRate() != null) {
                productLine.setBaseRate(updatePLRequestDTO.getBaseRate());
                productLine.setCode(updatePLRequestDTO.getCode());
                productLine.setLastModifiedTimestamp(LocalDateTime.now());
            } else {
                throw new CustomException("Values of both code and base rate cannot be null", HttpStatus.BAD_REQUEST);
            }

            productLineRepository.save(productLine);

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("PL with id : " + productLineId + " has been successfully updated !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.OK.value())
                    .message("PL with id : " + productLineId + " has been successfully updated !")
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
    public ApiResponseDTO deletePL(final int productLineId, final HttpServletRequest request, final String cookie) {
        try {
            ProductLine productLine = productLineRepository.findById(productLineId)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("Deletion failed, PL not found for id : " + productLineId, HttpStatus.BAD_REQUEST);
                    });
            productLine.setIsActive((byte) 0);
            productLineRepository.save(productLine);
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("PL with id : " + productLineId + " has been successfully deleted !")
                        .createTime(LocalDateTime.now())
                        .build());
            }
            return ApiResponseDTO.builder()
                    .message("PL with id : " + productLineId + " has been successfully deleted !")
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
            throw new CustomException("Soft deletion failed for Product Line id : " + productLineId, HttpStatus.OK);
        }
    }

}
