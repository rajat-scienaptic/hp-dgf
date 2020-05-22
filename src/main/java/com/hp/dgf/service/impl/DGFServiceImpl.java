package com.hp.dgf.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.dto.request.AddPLRequestDTO;
import com.hp.dgf.dto.request.UpdatePLRequestDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.*;
import com.hp.dgf.repository.BusinessCategoryRepository;
import com.hp.dgf.repository.DGFLogRepository;
import com.hp.dgf.repository.DgfRateChangeLogRepository;
import com.hp.dgf.repository.ProductLineRepository;
import com.hp.dgf.service.DGFService;
import com.hp.dgf.utils.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public final class DGFServiceImpl implements DGFService {
    @Autowired
    private BusinessCategoryRepository businessCategoryRepository;
    @Autowired
    private ProductLineRepository productLineRepository;
    @Autowired
    private DGFLogRepository dgfLogRepository;
    @Autowired
    private DgfRateChangeLogRepository dgfRateChangeLogRepository;

    final String key = Constants.KEY.replaceAll("\"", "");
    final String isActive = Constants.IS_ACTIVE.replaceAll("\"", "");
    final String modifiedBy = Constants.MODIFIED_BY.replaceAll("\"", "");
    final String baseRate = Constants.BASE_RATE.toLowerCase().replaceAll("\"", "");
    final String children = Constants.CHILDREN.replaceAll("\"", "");
    final String quarter = Constants.QUARTER.replaceAll("\"", "");
    final String value = Constants.VALUE.replaceAll("\"", "");

    @Override
    public final List<Object> getDgfGroups(int businessCategoryId) {
        //Getting List of Business Categories
        BusinessCategory businessCategory = businessCategoryRepository
                .findById(businessCategoryId)
                .orElseThrow(() -> {
                    throw new CustomException("No Business Category Found for ID : "+businessCategoryId, HttpStatus.BAD_REQUEST);
                });

        //Initialized Object Mapper to Map Json Object
        ObjectMapper mapper = new ObjectMapper();
        //Initialized final response object
        List<Object> dgfGroupData = new ArrayList<>();
        //If business category exists then process it else throw error
            //Output Map
            Map<String, Object> outputMap = new LinkedHashMap<>();
            //Mapped Business Categories as JSON Node
            JsonNode businessCategoryNode = mapper.convertValue(businessCategory, JsonNode.class);
            //If a business category has children object
            if (businessCategoryNode.has(Constants.CHILDREN)) {
                //Get all children of a business category
                JsonNode businessSubCategoryNode = businessCategoryNode.get(Constants.CHILDREN);
                //If business sub category is not empty
                if (!businessSubCategoryNode.isEmpty()) {
                    //Initializing columns map list
                    List<Object> columnsMapList = new ArrayList<>();
                    //Processing rest of the children of a business category
                    for (int i = 0; i < businessSubCategoryNode.size(); i++) {
                        List<Object> columnList = new ArrayList<>();
                        //Processing first children to add title object to final output response
                        if (i == 0) {
                            Map<String, Object> titleMap = new LinkedHashMap<>();
                            titleMap.put(Constants.TITLE, Constants.TITLE_VALUE);
                            titleMap.put(Constants.DATA_INDEX, Constants.TITLE_DATA_INDEX_VALUE);
                            columnList.add(titleMap);
                        }

                        JsonNode columns = businessSubCategoryNode.get(i).get(Constants.COLUMNS);

                        //Processing product lines for each sub category
                        columns.forEach(column -> {
                            Map<String, Object> columnData = new LinkedHashMap<>();
                            columnData.put(Constants.TITLE, column.get(Constants.CODE));
                            columnData.put(Constants.DATA_INDEX, column.get(Constants.CODE));
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
        List<Object> dataObject = new ArrayList<>();
        //Initialized Object Mapper
        ObjectMapper mapper = new ObjectMapper();

        //Getting business category data based on id
        Optional<BusinessCategory> businessCategory = businessCategoryRepository.findById(businessCategoryId);

        businessCategory.ifPresent(bc -> {
            //Getting Set of All Dgf Groups
            Set<DGFGroups> dgfGroupsSet = bc.getDgfGroups();
            for (DGFGroups dgfGroup : dgfGroupsSet) {
               //Parent DGF Group
               Map<String, Object> dg = new LinkedHashMap<>();
               dg.put(key, id.getAndIncrement());
               dg.put(isActive, dgfGroup.getIsActive());
               dg.put(modifiedBy, dgfGroup.getModifiedBy());
               dg.put(baseRate, dgfGroup.getBaseRate());
               JsonNode dgfGroupObject = mapper.convertValue(dgfGroup, JsonNode.class);

               //Processing DGF Sub Group 1
               JsonNode dgfSubGroup1Object = dgfGroupObject.get(Constants.CHILDREN);
               List<Map<String, Object>> dg1List = new ArrayList<>();
               Map<String, Object> addDgfSubGroup1Map = new LinkedHashMap<>();

               dgfSubGroup1Object.forEach(dgfSubGroup1 -> {
                   Map<String, Object> dg1 = new LinkedHashMap<>();
                   Map<String, Object> addDgfSubGroup2Map = new LinkedHashMap<>();

                   dg1.put(key, id.getAndIncrement());
                   dg1.put(isActive, dgfSubGroup1.get(Constants.IS_ACTIVE));
                   dg1.put(modifiedBy, dgfSubGroup1.get(Constants.MODIFIED_BY));
                   dg1.put(baseRate, dgfSubGroup1.get(Constants.BASE_RATE));

                   //Processing DGF Sub Group 2
                   JsonNode dgfSubGroup2Object = dgfSubGroup1.get(Constants.CHILDREN);
                   List<Map<String, Object>> dg2List = new ArrayList<>();
                   dgfSubGroup2Object.forEach(dgfSubGroup2 -> {
                       Map<String, Object> dg2 = new LinkedHashMap<>();
                       Map<String, Object> addDgfSubGroup3Map = new LinkedHashMap<>();

                       dg2.put(key, id.getAndIncrement());
                       dg2.put(isActive, dgfSubGroup2.get(Constants.IS_ACTIVE));
                       dg2.put(modifiedBy, dgfSubGroup2.get(Constants.MODIFIED_BY));
                       dg2.put(baseRate, dgfSubGroup2.get(Constants.BASE_RATE));

                       //Processing DGF Sub Group 2 PLs (Columns)
                       JsonNode subGroup2Data = dgfSubGroup2.get(Constants.COLUMNS);
                       subGroup2Data.forEach(data -> {
                           Map<String, Object> dg2DataValues = new LinkedHashMap<>();
                           dg2DataValues.put(quarter, data.get(Constants.COLOR_CODE_SET).get("fyQuarter"));
                           dg2DataValues.put(value, new ArrayList<>(Collections.singletonList(data.get(Constants.DGF_RATE_ENTRY).get(Constants.DGF_RATE))));
                           String k2 = data.get(Constants.CODE).toString().replaceAll("\"", "");
                           dg2.put(k2, dg2DataValues);
                       });

                       //Processing DGF Sub Group 3
                       JsonNode dgSubGroup3Object = dgfSubGroup2.get(Constants.CHILDREN);
                       List<Map<String, Object>> dg3List = new ArrayList<>();
                       dgSubGroup3Object.forEach(dgfSubGroup3 -> {
                           Map<String, Object> dg3 = new LinkedHashMap<>();
                           dg3.put(key, id.getAndIncrement());
                           dg3.put(isActive, dgfSubGroup3.get(Constants.IS_ACTIVE));
                           dg3.put(modifiedBy, dgfSubGroup3.get(Constants.MODIFIED_BY));
                           dg3.put(baseRate, dgfSubGroup3.get(Constants.BASE_RATE));

                           //Processing DGF Sub Group 3 PLs (Columns)
                           JsonNode subGroup3Data = dgfSubGroup3.get(Constants.COLUMNS);
                           subGroup3Data.forEach(data -> {
                               Map<String, Object> dg3DataValues = new LinkedHashMap<>();
                               dg3DataValues.put(quarter, data.get(Constants.COLOR_CODE_SET).get("fyQuarter"));
                               dg3DataValues.put(value, new ArrayList<>(Collections.singletonList(data.get(Constants.DGF_RATE_ENTRY).get("dgfRate"))));
                               String k3 = data.get(Constants.CODE).toString().replaceAll("\"", "");
                               dg3.put(k3, dg3DataValues);
                           });
                           dg3List.add(dg3);
                       });

                       addDgfSubGroup3Map.put(key, id.getAndIncrement());
                       addDgfSubGroup3Map.put("dgfSubGroup2Id", dgfSubGroup2.get("id"));
                       addDgfSubGroup3Map.put(baseRate, Constants.ADD_A_SUB_GROUP);

                       dg3List.add(addDgfSubGroup3Map);

                       dg2.put(children, dg3List);
                       dg2List.add(dg2);
                   });

                   addDgfSubGroup2Map.put(key, id.getAndIncrement());
                   addDgfSubGroup2Map.put("dgfSubGroup1Id", dgfSubGroup1.get("id"));
                   addDgfSubGroup2Map.put(baseRate, Constants.ADD_A_SUB_GROUP);

                   dg2List.add(addDgfSubGroup2Map);

                   dg1.put(children, dg2List);
                   dg1List.add(dg1);
               });

               addDgfSubGroup1Map.put(key, id.getAndIncrement());
               addDgfSubGroup1Map.put("dgfGroupsId", dgfGroup.getId());
               addDgfSubGroup1Map.put(baseRate, Constants.ADD_A_SUB_GROUP);

               dg1List.add(addDgfSubGroup1Map);

               dg.put(children, dg1List);
               dataObject.add(dg);
           }
        });

        return dataObject;
    }

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
            if (businessCategoryNode.has(Constants.CHILDREN)) {
                //Get all children of a business category
                JsonNode businessSubCategoryNode = businessCategoryNode.get(Constants.CHILDREN);
                //If business sub category is not empty
                if (!businessSubCategoryNode.isEmpty()) {
                    //Processing rest of the children of a business category
                    for (int i = 0; i < businessSubCategoryNode.size(); i++) {
                        List<Object> columnList = new ArrayList<>();
                        //Processing first children to add title object to final output response
                        if (i == 0) {
                            Map<String, Object> titleMap = new LinkedHashMap<>();
                            titleMap.put(Constants.TITLE, Constants.TITLE_VALUE);
                            titleMap.put(Constants.DATA_INDEX, Constants.TITLE_DATA_INDEX_VALUE);
                            columnList.add(titleMap);
                        }

                        Map<String, Object> iconMap = new LinkedHashMap<>();
                        iconMap.put(Constants.TITLE, Constants.ICON_VALUE);
                        iconMap.put(Constants.DATA_INDEX, "");
                        columnList.add(iconMap);

                        JsonNode columns = businessSubCategoryNode.get(i).get(Constants.COLUMNS);

                        //Processing product lines for each sub category
                        columns.forEach(column -> {
                            Map<String, Object> columnData = new LinkedHashMap<>();
                            columnData.put(Constants.TITLE, column.get(Constants.CODE));
                            columnData.put(Constants.DATA_INDEX, column.get(Constants.CODE));
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
        String fyYear = year.substring(year.length()-2);

        final String headerBaseRateTitle = "BASE RATES FY"+fyYear;

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

        headerBaseRateMap.put(baseRate, headerBaseRateTitle);
        effectiveFirstQuarterMap.put(baseRate, effectiveFirstQuarterTitle.toString());
        effectiveSecondQuarterMap.put(baseRate, effectiveSecondQuarterTitle.toString());

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
        try{
            checkIfPLAlreadyExists(addPlRequestDTO.getCode());
            final int productLineId = productLineRepository.save(ProductLine.builder()
                    .code(addPlRequestDTO.getCode())
                    .businessSubCategoryId(addPlRequestDTO.getBusinessSubCategoryId())
                    .baseRate(addPlRequestDTO.getBaseRate())
                    .isActive(addPlRequestDTO.getIsActive())
                    .build()).getId();

            if(request != null){
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Constants.SUCCESS)
                        .message("PL with code : "+ addPlRequestDTO.getCode()+" and id : "+productLineId+ " has been successfully created !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.CREATED.value())
                    .message("PL with code : "+ addPlRequestDTO.getCode()+" and id : "+productLineId+ " has been successfully created !")
                    .build();

        }catch(Exception e) {
            if(request != null){
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Constants.FAILURE)
                        .message(e.getMessage())
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public final ApiResponseDTO updatePL(final UpdatePLRequestDTO updatePLRequestDTO, final int productLineId, final HttpServletRequest request) {
        try{
            ProductLine productLine = productLineRepository
                    .findById(productLineId)
                    .orElseThrow(() -> {
                        throw new CustomException("Update failed, PL with id : "+productLineId+ " doesn't exist !", HttpStatus.NOT_FOUND);
                    });

                if(updatePLRequestDTO.getCode() == null && updatePLRequestDTO.getBaseRate() != null){
                    productLine.setBaseRate(updatePLRequestDTO.getBaseRate());
                }else if(updatePLRequestDTO.getBaseRate() == null && updatePLRequestDTO.getCode() != null){
                    productLine.setCode(updatePLRequestDTO.getCode());
                }else  if(updatePLRequestDTO.getCode() != null && updatePLRequestDTO.getBaseRate() != null){
                    productLine.setBaseRate(updatePLRequestDTO.getBaseRate());
                    productLine.setCode(updatePLRequestDTO.getCode());
                }else{
                    throw new CustomException("Values of both code and base rate cannot be null", HttpStatus.BAD_REQUEST);
                }

                productLineRepository.save(productLine);

            if(request != null){
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Constants.SUCCESS)
                        .message("PL with id : "+productLineId+ " has been successfully updated !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.OK.value())
                    .message("PL with id : "+productLineId+ " has been successfully updated !")
                    .build();
        }catch(Exception e) {
            if(request != null){
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Constants.FAILURE)
                        .message(e.getMessage())
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    private void checkIfPLAlreadyExists(String code){
      String productLine = productLineRepository.checkIfPlExists(code);
      if(productLine != null){
          throw new CustomException("PL with code : "+code+ " already exists !", HttpStatus.BAD_REQUEST);
      }
    }
}
