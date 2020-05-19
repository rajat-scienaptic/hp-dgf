package com.hp.dgf.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.dto.request.PLRequestDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.dto.response.HeaderObjectDTO;
import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.BusinessCategory;
import com.hp.dgf.model.DGFGroups;
import com.hp.dgf.model.DGFLogs;
import com.hp.dgf.model.ProductLine;
import com.hp.dgf.repository.BusinessCategoryRepository;
import com.hp.dgf.repository.DGFLogRepository;
import com.hp.dgf.repository.ProductLineRepository;
import com.hp.dgf.service.DGFService;
import com.hp.dgf.utils.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class DGFServiceImpl implements DGFService {
    @Autowired
    private BusinessCategoryRepository businessCategoryRepository;
    @Autowired
    private ProductLineRepository productLineRepository;
    @Autowired
    private DGFLogRepository dgfLogRepository;

    final String key = Constants.KEY.replaceAll("\"", "");
    final String isActive = Constants.IS_ACTIVE.replaceAll("\"", "");
    final String modifiedBy = Constants.MODIFIED_BY.replaceAll("\"", "");
    final String baseRate = Constants.BASE_RATE.replaceAll("\"", "");
    final String children = Constants.CHILDREN.replaceAll("\"", "");

    @Override
    public final List<Object> getDgfGroups(int businessCategoryId) {
        //Getting List of Business Categories
        BusinessCategory businessCategory = businessCategoryRepository.getBusinessCategoryData(businessCategoryId);
        //Initialized Object Mapper to Map Json Object
        ObjectMapper mapper = new ObjectMapper();
        //Initialized final response object
        List<Object> dgfGroupData = new ArrayList<>();
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

                outputMap.put("columns", columnsMapList);
                outputMap.put("data", getDataObject(businessCategoryId));

                //Added columns object to the final output
                dgfGroupData.add(outputMap);
            }
        }
        return dgfGroupData;
    }

    public final List<Object> getDataObject(int businessCategoryId) {
        AtomicInteger id = new AtomicInteger(1);
        //Initialized Data Object
        List<Object> dataObject = new ArrayList<>();
        //Initialized Object Mapper
        ObjectMapper mapper = new ObjectMapper();

        BusinessCategory businessCategory = businessCategoryRepository.getBusinessCategoryData(businessCategoryId);

            //Getting Set of All Dgf Groups
            Set<DGFGroups> dgfGroupsSet = businessCategory.getDgfGroups();

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

                dgfSubGroup1Object.forEach(dgfSubGroup1 -> {
                    Map<String, Object> dg1 = new LinkedHashMap<>();
                    dg1.put(key, id.getAndIncrement());
                    dg1.put(isActive, dgfSubGroup1.get(Constants.IS_ACTIVE));
                    dg1.put(modifiedBy, dgfSubGroup1.get(Constants.MODIFIED_BY));
                    dg1.put(baseRate, dgfSubGroup1.get(Constants.BASE_RATE));

                    //Processing DGF Sub Group 2
                    JsonNode dgfSubGroup2Object = dgfSubGroup1.get(Constants.CHILDREN);
                    List<Map<String, Object>> dg2List = new ArrayList<>();
                    dgfSubGroup2Object.forEach(dgfSubGroup2 -> {
                        Map<String, Object> dg2 = new LinkedHashMap<>();
                        dg2.put(key, id.getAndIncrement());
                        dg2.put(isActive, dgfSubGroup2.get(Constants.IS_ACTIVE));
                        dg2.put(modifiedBy, dgfSubGroup2.get(Constants.MODIFIED_BY));
                        dg2.put(baseRate, dgfSubGroup2.get(Constants.BASE_RATE));

                        //Processing DGF Sub Group 2 PLs (Columns)
                        JsonNode subGroup2Data = dgfSubGroup2.get(Constants.COLUMNS);
                        subGroup2Data.forEach(data -> {
                            Map<String, Object> dg2DataValues = new LinkedHashMap<>();
                            dg2DataValues.put("quarter", data.get(Constants.COLOR_CODE_SET).get("fyQuarter"));
                            dg2DataValues.put("value", new ArrayList<>(Collections.singletonList(data.get(Constants.DGF_RATE_ENTRY).get(Constants.DGF_RATE))));
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
                                dg3DataValues.put("quarter", data.get(Constants.COLOR_CODE_SET).get("fyQuarter"));
                                dg3DataValues.put("value", new ArrayList<>(Collections.singletonList(data.get(Constants.DGF_RATE_ENTRY).get("dgfRate"))));
                                String k3 = data.get(Constants.CODE).toString().replaceAll("\"", "");
                                dg3.put(k3, dg3DataValues);
                            });
                            dg3List.add(dg3);
                        });
                        dg2.put(children, dg3List);
                        dg2List.add(dg2);
                    });
                    dg1.put(children, dg2List);
                    dg1List.add(dg1);
                });
                dg.put(children, dg1List);
                dataObject.add(dg);
            }

        return dataObject;
    }

    @Override
    public final List<Object> getHeaderData() {
        List<BusinessCategory> businessCategoryList = businessCategoryRepository.findAll();
        ObjectMapper mapper = new ObjectMapper();

        List<Object> headerData = new ArrayList<>();

        businessCategoryList.forEach(businessCategory -> {
            JsonNode businessCategoryNode = mapper.convertValue(businessCategory, JsonNode.class);
            if (businessCategoryNode.has(Constants.CHILDREN)) {
                JsonNode businessSubCategoryNode = businessCategoryNode.get(Constants.CHILDREN);
                if (!businessSubCategoryNode.isEmpty()) {
                    for (int i = 0; i < businessSubCategoryNode.size(); i++) {

                        Map<String, Object> dataObject1 = new LinkedHashMap<>();
                        Map<String, Object> dataObject2 = new LinkedHashMap<>();
                        Map<String, Object> dataObject3 = new LinkedHashMap<>();

                        dataObject1.put(baseRate, "Base Rate FY20");
                        dataObject2.put(baseRate, "Effective Nov 2019 (FY20)");
                        dataObject3.put(baseRate, "Effective Jan 2020 (FY20)");

                        List<Object> columnList = new ArrayList<>();
                        List<Object> dataList = new ArrayList<>();

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

                        columns.forEach(column -> {
                            Map<String, Object> columnData = new LinkedHashMap<>();
                            columnData.put(Constants.TITLE, column.get(Constants.CODE));
                            columnData.put(Constants.DATA_INDEX, column.get(Constants.CODE));
                            columnList.add(columnData);
                            String pl = column.get(Constants.CODE).toString().replaceAll("\"", "");
                            dataObject1.put(pl, column.get(Constants.DGF_RATE_ENTRY).get(Constants.DGF_RATE));
                            dataObject2.put(pl, column.get(Constants.COLOR_CODE_SET).get("name"));
                            dataObject3.put(pl, column.get(Constants.COLOR_CODE_SET).get("name"));
                        });

                        dataList.add(dataObject1);
                        dataList.add(dataObject2);
                        dataList.add(dataObject3);

                        headerData.add(new HeaderObjectDTO(columnList, dataList));
                    }
                }
            }
        });
        return headerData;
    }

    @Override
    public Object addPL(PLRequestDTO plRequestDTO) {
        try{
            int productLineId = productLineRepository.save(ProductLine.builder()
                    .code(plRequestDTO.getCode())
                    .businessSubCategoryId(plRequestDTO.getBusinessSubCategoryId())
                    .baseRate(plRequestDTO.getBaseRate())
                    .isActive(plRequestDTO.getIsActive())
                    .build()).getId();

            dgfLogRepository.save(DGFLogs.builder()
                    .endpoint("/addPL")
                    .status("SUCCESS")
                    .message("PL with code : "+plRequestDTO.getCode()+" and id : "+productLineId+ " has been successfully created !")
                    .createTime(LocalDateTime.now())
                    .build());

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.CREATED.value())
                    .message("PL with code : "+plRequestDTO.getCode()+" and id : "+productLineId+ " has been successfully created !")
                    .build();

        }catch(Exception e) {
            dgfLogRepository.save(DGFLogs.builder()
                    .endpoint("/addPL")
                    .status("FAILURE")
                    .message(e.getMessage())
                    .createTime(LocalDateTime.now())
                    .build());
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public Object updatePL(PLRequestDTO plRequestDTO, int productLineId) {

        return null;
    }

    private void checkIfPLExists(){

    }
}
