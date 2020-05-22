package com.hp.dgf.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.constants.MapKeys;
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
import com.hp.dgf.repository.DgfRateChangeLogRepository;
import com.hp.dgf.repository.ProductLineRepository;
import com.hp.dgf.service.DGFHeaderDataService;
import com.hp.dgf.constants.Variables;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.*;

@Service
public class DGFHeaderDataServiceImpl implements DGFHeaderDataService {
    @Autowired
    private ProductLineRepository productLineRepository;
    @Autowired
    private DGFLogRepository dgfLogRepository;
    @Autowired
    private BusinessCategoryRepository businessCategoryRepository;
    @Autowired
    private DgfRateChangeLogRepository dgfRateChangeLogRepository;

    @Override
    public final List<Object> getHeaderData() {
        List<BusinessCategory> businessCategoryList = businessCategoryRepository.findAll();
        ObjectMapper mapper = new ObjectMapper();
        List<Object> headerData = new ArrayList<>();

        //Output Map
        Map<String, Object> outputMap = new LinkedHashMap<>();
        List<Object> columnsMapList = new ArrayList<>();
        List<Object> businessMapList = new ArrayList<>();

        businessCategoryList.forEach(businessCategory -> {
            Map<String, Object> businessMap = new LinkedHashMap<>();
            //Mapped Business Categories as JSON Node
            JsonNode businessCategoryNode = mapper.convertValue(businessCategory, JsonNode.class);
            //If a business category has children object
            if (businessCategoryNode.has(Variables.CHILDREN)) {
                //Get all children of a business category
                JsonNode businessSubCategoryNode = businessCategoryNode.get(Variables.CHILDREN);
                //If business sub category is not empty
                if (!businessSubCategoryNode.isEmpty()) {
                    //Processing rest of the children of a business category
                    for (int i = 0; i < businessSubCategoryNode.size(); i++) {
                        List<Object> columnList = new ArrayList<>();
                        //Processing first children to add title object to final output response
                        if (i == 0) {
                            Map<String, Object> titleMap = new LinkedHashMap<>();
                            titleMap.put(Variables.TITLE, Variables.TITLE_VALUE);
                            titleMap.put(Variables.DATA_INDEX, Variables.TITLE_DATA_INDEX_VALUE);
                            columnList.add(titleMap);
                        }

                        Map<String, Object> iconMap = new LinkedHashMap<>();
                        iconMap.put(Variables.TITLE, Variables.ICON_VALUE);
                        iconMap.put(Variables.DATA_INDEX, "");
                        columnList.add(iconMap);

                        JsonNode columns = businessSubCategoryNode.get(i).get(Variables.COLUMNS);

                        //Processing product lines for each sub category
                        columns.forEach(column -> {
                            Map<String, Object> columnData = new LinkedHashMap<>();
                            columnData.put(Variables.TITLE, column.get(Variables.CODE));
                            columnData.put(Variables.DATA_INDEX, column.get(Variables.CODE));
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

            businessMap.put("id", businessCategory.getId());
            businessMap.put("label", businessCategory.getName());
            businessMapList.add(businessMap);
        });

        outputMap.put("columns", columnsMapList);
        outputMap.put("data", getHeaderDataObject());
        outputMap.put("business", businessMapList);

        //Added columns object to the final output
        headerData.add(outputMap);

        return headerData;
    }

    public final List<Object> getHeaderDataObject() {
        //Initialized Data Object
        final List<Object> dataObject = new ArrayList<>();

        final LocalDate currentDate = LocalDate.now();
        Month month = currentDate.getMonth();
        String year = String.valueOf(currentDate.getYear());
        String fyYear = year.substring(year.length() - 2);

        final String headerBaseRateTitle = "BASE RATES FY" + fyYear;

        final StringBuilder effectiveFirstQuarterTitle = new StringBuilder("Effective ");
        effectiveFirstQuarterTitle.append(month).append(" ");
        effectiveFirstQuarterTitle.append(year).append(" ");
        effectiveFirstQuarterTitle.append("(FY").append(fyYear).append(")");

        final StringBuilder effectiveSecondQuarterTitle = new StringBuilder("Effective ");
        effectiveSecondQuarterTitle.append(month.plus(2)).append(" ");
        effectiveSecondQuarterTitle.append(year).append(" ");
        effectiveSecondQuarterTitle.append("(FY").append(fyYear).append(")");

        //Getting business category data based on id
        final List<BusinessCategory> businessCategoryList = businessCategoryRepository.findAll();

        final Map<String, Object> headerBaseRateMap = new LinkedHashMap<>();
        final Map<String, Object> effectiveFirstQuarterMap = new LinkedHashMap<>();
        final Map<String, Object> effectiveSecondQuarterMap = new LinkedHashMap<>();

        headerBaseRateMap.put(MapKeys.baseRate, headerBaseRateTitle);
        effectiveFirstQuarterMap.put(MapKeys.baseRate, effectiveFirstQuarterTitle.toString());
        effectiveSecondQuarterMap.put(MapKeys.baseRate, effectiveSecondQuarterTitle.toString());

        businessCategoryList.forEach(businessCategory -> {
            //Getting Set of All Sub Categories
            final Set<BusinessSubCategory> businessSubCategorySet = businessCategory.getChildren();
            businessSubCategorySet.forEach(businessSubCategory -> {
                //Getting list of PL data for each sub category
                final Set<ProductLine> productLineSet = businessSubCategory.getColumns();
                //Iterating over list of product lines
                productLineSet.forEach(productLine -> {
                    final Map<String, Object> plMap = new LinkedHashMap<>();
                    plMap.put("quarter", productLine.getColorCodeSet().getFyQuarter());
                    plMap.put("value", new ArrayList<>(Collections.singletonList(productLine.getBaseRate())));
                    String pl = productLine.getCode().replaceAll("\"", "");
                    headerBaseRateMap.put(pl, plMap);
                    effectiveFirstQuarterMap.put(pl, plMap);
                    effectiveSecondQuarterMap.put(pl, plMap);
                });
            });
        });

        dataObject.add(headerBaseRateMap);
        dataObject.add(effectiveFirstQuarterMap);
        dataObject.add(effectiveSecondQuarterMap);

        return dataObject;
    }

    @Override
    public final ApiResponseDTO addPL(final AddPLRequestDTO addPlRequestDTO, final HttpServletRequest request) {
        try {
            checkIfPLAlreadyExists(addPlRequestDTO.getCode());
            final int productLineId = productLineRepository.save(ProductLine.builder()
                    .code(addPlRequestDTO.getCode())
                    .businessSubCategoryId(addPlRequestDTO.getBusinessSubCategoryId())
                    .baseRate(addPlRequestDTO.getBaseRate())
                    .isActive(addPlRequestDTO.getIsActive())
                    .lastModifiedTimestamp(LocalDateTime.now())
                    .build()).getId();

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
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
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public final ApiResponseDTO updatePL(final UpdatePLRequestDTO updatePLRequestDTO, final int productLineId, final HttpServletRequest request) {
        try {
            ProductLine productLine = productLineRepository
                    .findById(productLineId)
                    .orElseThrow(() -> {
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
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    private void checkIfPLAlreadyExists(String code) {
        String productLine = productLineRepository.checkIfPlExists(code);
        if (productLine != null) {
            throw new CustomException("PL with code : " + code + " already exists !", HttpStatus.BAD_REQUEST);
        }
    }
}
