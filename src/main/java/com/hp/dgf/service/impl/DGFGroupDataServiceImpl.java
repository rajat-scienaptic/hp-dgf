package com.hp.dgf.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.constants.MapKeys;
import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.*;
import com.hp.dgf.repository.BusinessCategoryRepository;
import com.hp.dgf.repository.DgfRateChangeLogRepository;
import com.hp.dgf.service.DGFGroupDataService;
import com.hp.dgf.constants.Variables;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public final class DGFGroupDataServiceImpl implements DGFGroupDataService {
    @Autowired
    private BusinessCategoryRepository businessCategoryRepository;

    @Autowired
    private DgfRateChangeLogRepository dgfRateChangeLogRepository;

    @Override
    public final List<Object> getDgfGroups(int businessCategoryId) {
        //Getting List of Business Categories
        BusinessCategory businessCategory = businessCategoryRepository
                .findById(businessCategoryId)
                .orElseThrow(() -> {
                    throw new CustomException("No Business Category Found for ID : " + businessCategoryId, HttpStatus.BAD_REQUEST);
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
        if (businessCategoryNode.has(Variables.CHILDREN)) {
            //Get all children of a business category
            JsonNode businessSubCategoryNode = businessCategoryNode.get(Variables.CHILDREN);
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
                        titleMap.put(Variables.TITLE, Variables.TITLE_VALUE);
                        titleMap.put(Variables.DATA_INDEX, Variables.TITLE_DATA_INDEX_VALUE);
                        columnList.add(titleMap);
                    }

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
                dg.put(MapKeys.key, id.getAndIncrement());
                dg.put(MapKeys.isActive, dgfGroup.getIsActive());
                dg.put(MapKeys.modifiedBy, dgfGroup.getModifiedBy());
                dg.put(MapKeys.baseRate, dgfGroup.getBaseRate());
                JsonNode dgfGroupObject = mapper.convertValue(dgfGroup, JsonNode.class);

                //Processing DGF Sub Group 1
                JsonNode dgfSubGroup1Object = dgfGroupObject.get(Variables.CHILDREN);
                List<Map<String, Object>> dg1List = new ArrayList<>();
                Map<String, Object> addDgfSubGroup1Map = new LinkedHashMap<>();

                dgfSubGroup1Object.forEach(dgfSubGroup1 -> {
                    Map<String, Object> dg1 = new LinkedHashMap<>();
                    Map<String, Object> addDgfSubGroup2Map = new LinkedHashMap<>();

                    dg1.put(MapKeys.key, id.getAndIncrement());
                    dg1.put(MapKeys.isActive, dgfSubGroup1.get(Variables.IS_ACTIVE));
                    dg1.put(MapKeys.modifiedBy, dgfSubGroup1.get(Variables.MODIFIED_BY));
                    dg1.put(MapKeys.baseRate, dgfSubGroup1.get(Variables.BASE_RATE));

                    //Processing DGF Sub Group 2
                    JsonNode dgfSubGroup2Object = dgfSubGroup1.get(Variables.CHILDREN);
                    List<Map<String, Object>> dg2List = new ArrayList<>();
                    dgfSubGroup2Object.forEach(dgfSubGroup2 -> {
                        Map<String, Object> dg2 = new LinkedHashMap<>();
                        Map<String, Object> addDgfSubGroup3Map = new LinkedHashMap<>();

                        dg2.put(MapKeys.key, id.getAndIncrement());
                        dg2.put(MapKeys.isActive, dgfSubGroup2.get(Variables.IS_ACTIVE));
                        dg2.put(MapKeys.modifiedBy, dgfSubGroup2.get(Variables.MODIFIED_BY));
                        dg2.put(MapKeys.baseRate, dgfSubGroup2.get(Variables.BASE_RATE));

                        //Processing DGF Sub Group 2 PLs (Columns)
                        JsonNode subGroup2Data = dgfSubGroup2.get(Variables.COLUMNS);
                        subGroup2Data.forEach(data -> {
                            Map<String, Object> dg2DataValues = new LinkedHashMap<>();
                            dg2DataValues.put(MapKeys.quarter, data.get(Variables.COLOR_CODE_SET).get("fyQuarter"));
                            dg2DataValues.put(MapKeys.value, new ArrayList<>(Collections.singletonList(data.get(Variables.DGF_RATE_ENTRY).get(Variables.DGF_RATE))));
                            String k2 = data.get(Variables.CODE).toString().replaceAll("\"", "");
                            dg2.put(k2, dg2DataValues);
                        });

                        //Processing DGF Sub Group 3
                        JsonNode dgSubGroup3Object = dgfSubGroup2.get(Variables.CHILDREN);
                        List<Map<String, Object>> dg3List = new ArrayList<>();
                        dgSubGroup3Object.forEach(dgfSubGroup3 -> {
                            Map<String, Object> dg3 = new LinkedHashMap<>();
                            dg3.put(MapKeys.key, id.getAndIncrement());
                            dg3.put(MapKeys.isActive, dgfSubGroup3.get(Variables.IS_ACTIVE));
                            dg3.put(MapKeys.modifiedBy, dgfSubGroup3.get(Variables.MODIFIED_BY));
                            dg3.put(MapKeys.baseRate, dgfSubGroup3.get(Variables.BASE_RATE));

                            //Processing DGF Sub Group 3 PLs (Columns)
                            JsonNode subGroup3Data = dgfSubGroup3.get(Variables.COLUMNS);
                            subGroup3Data.forEach(data -> {
                                Map<String, Object> dg3DataValues = new LinkedHashMap<>();
                                dg3DataValues.put(MapKeys.quarter, data.get(Variables.COLOR_CODE_SET).get("fyQuarter"));
                                dg3DataValues.put(MapKeys.value, new ArrayList<>(Collections.singletonList(data.get(Variables.DGF_RATE_ENTRY).get("dgfRate"))));
                                String k3 = data.get(Variables.CODE).toString().replaceAll("\"", "");
                                dg3.put(k3, dg3DataValues);
                            });
                            dg3List.add(dg3);
                        });

                        addDgfSubGroup3Map.put(MapKeys.key, id.getAndIncrement());
                        addDgfSubGroup3Map.put("dgfSubGroup2Id", dgfSubGroup2.get("id"));
                        addDgfSubGroup3Map.put(MapKeys.baseRate, Variables.ADD_A_SUB_GROUP);

                        dg3List.add(addDgfSubGroup3Map);

                        dg2.put(MapKeys.children, dg3List);
                        dg2List.add(dg2);
                    });

                    addDgfSubGroup2Map.put(MapKeys.key, id.getAndIncrement());
                    addDgfSubGroup2Map.put("dgfSubGroup1Id", dgfSubGroup1.get("id"));
                    addDgfSubGroup2Map.put(MapKeys.baseRate, Variables.ADD_A_SUB_GROUP);

                    dg2List.add(addDgfSubGroup2Map);

                    dg1.put(MapKeys.children, dg2List);
                    dg1List.add(dg1);
                });

                addDgfSubGroup1Map.put(MapKeys.key, id.getAndIncrement());
                addDgfSubGroup1Map.put("dgfGroupsId", dgfGroup.getId());
                addDgfSubGroup1Map.put(MapKeys.baseRate, Variables.ADD_A_SUB_GROUP);

                dg1List.add(addDgfSubGroup1Map);

                dg.put(MapKeys.children, dg1List);
                dataObject.add(dg);
            }
        });

        return dataObject;
    }

}
