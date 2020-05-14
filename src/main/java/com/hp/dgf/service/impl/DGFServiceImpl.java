package com.hp.dgf.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.dto.Column;
import com.hp.dgf.dto.DataObject;
import com.hp.dgf.dto.HeaderObject;
import com.hp.dgf.model.*;
import com.hp.dgf.repository.BusinessCategoryRepository;
import com.hp.dgf.repository.ColorCodeRepository;
import com.hp.dgf.repository.DGFRepository;
import com.hp.dgf.repository.DgfRateChangeLogRepository;
import com.hp.dgf.service.DGFService;
import com.hp.dgf.utils.Constants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class DGFServiceImpl implements DGFService {
    @Autowired
    private DGFRepository dgfRepository;

    @Autowired
    private ColorCodeRepository colorCodeRepository;

    @Autowired
    private BusinessCategoryRepository businessCategoryRepository;

    @Autowired
    private DgfRateChangeLogRepository dgfRateChangeLogRepository;

    final String key = Constants.KEY.replaceAll("\"", "");
    final String isActive = Constants.IS_ACTIVE.replaceAll("\"", "");
    final String modifiedBy = Constants.MODIFIED_BY.replaceAll("\"", "");
    final String baseRate = Constants.BASE_RATE.replaceAll("\"", "");
    final String children = Constants.CHILDREN.replaceAll("\"", "");
    final String code = Constants.CODE.replaceAll("\"", "");

    @Override
    public final List<Object> getDgfGroups() {
        //Getting List of Business Categories
        List<BusinessCategory> businessCategoryList = businessCategoryRepository.findAll();

        //Initialized Object Mapper to Map Json Object
        ObjectMapper mapper = new ObjectMapper();

        //Initialized final response object
        List<Object> dgfGroupData = new ArrayList<>();

        businessCategoryList.forEach(businessCategory -> {
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
                            Map<String, Object> titleMap = new HashMap<>();
                            titleMap.put(Constants.TITLE, Constants.TITLE_VALUE);
                            titleMap.put(Constants.DATA_INDEX, Constants.TITLE_DATA_INDEX_VALUE);
                            columnList.add(titleMap);
                        }

                        Map<String, Object> iconMap = new HashMap<>();
                        iconMap.put(Constants.TITLE, Constants.ICON_VALUE);
                        iconMap.put(Constants.DATA_INDEX, "");
                        columnList.add(iconMap);

                        JsonNode columns = businessSubCategoryNode.get(i).get(Constants.COLUMNS);

                        //Processing product lines for each sub category
                        columns.forEach(column -> {
                            Map<String, Object> columnData = new HashMap<>();
                            columnData.put(Constants.TITLE, column.get(Constants.CODE));
                            columnData.put(Constants.DATA_INDEX, column.get(Constants.CODE));
                            columnList.add(columnData);
                        });

                        //Added columns object to the final output
                        dgfGroupData.add(new Column(columnList));
                    }
                }
            }
        });

        //Added data object to the final output
        dgfGroupData.add(new DataObject(getDataObject()));

        return dgfGroupData;
    }

    public final List<Object> getDataObject() {
        //Initialized Data Object
        List<Object> dataObject = new ArrayList<>();
        //Initialized Object Mapper
        ObjectMapper mapper = new ObjectMapper();

        //Getting list of All Dgf Groups
        List<DGFGroups> dgfGroupsList = dgfRepository.findAll();

        //
        for (DGFGroups dgfGroup : dgfGroupsList) {
            //Parent DGF Group
            Map<String, Object> dg = new HashMap<>();
            dg.put(key, dgfGroup.getKey());
            dg.put(isActive, dgfGroup.getIsActive());
            dg.put(modifiedBy, dgfGroup.getModifiedBy());
            dg.put(baseRate, dgfGroup.getBaseRate());
            JsonNode dgfGroupObject = mapper.convertValue(dgfGroup, JsonNode.class);

            //Processing DGF Sub Group 1
            JsonNode dgfSubGroup1Object = dgfGroupObject.get(Constants.CHILDREN);
            List<Map<String, Object>> dg1List = new ArrayList<>();
            dgfSubGroup1Object.forEach(dgfSubGroup1 -> {
                Map<String, Object> dg1 = new HashMap<>();
                dg1.put(key, dgfSubGroup1.get(Constants.KEY));
                dg1.put(isActive, dgfSubGroup1.get(Constants.IS_ACTIVE));
                dg1.put(modifiedBy, dgfSubGroup1.get(Constants.MODIFIED_BY));
                dg1.put(baseRate, dgfSubGroup1.get(Constants.BASE_RATE));

                //Processing DGF Sub Group 2
                JsonNode dgfSubGroup2Object = dgfSubGroup1.get(Constants.CHILDREN);
                List<Map<String, Object>> dg2List = new ArrayList<>();
                dgfSubGroup2Object.forEach(dgfSubGroup2 -> {
                    Map<String, Object> dg2 = new HashMap<>();
                    dg2.put(key, dgfSubGroup2.get(Constants.KEY));
                    dg2.put(isActive, dgfSubGroup2.get(Constants.IS_ACTIVE));
                    dg2.put(modifiedBy, dgfSubGroup2.get(Constants.MODIFIED_BY));
                    dg2.put(baseRate, dgfSubGroup2.get(Constants.BASE_RATE));
                    List<Map<String, Object>> dg2PLsList = new ArrayList<>();

                    //Processing DGF Sub Group 2 PLs (Columns)
                    JsonNode subGroup2Data = dgfSubGroup2.get(Constants.COLUMNS);
                    subGroup2Data.forEach(data -> {
                        Map<String, Object> dg2Data = new HashMap<>();
                        Map<String, Object> dg2DataValues = new HashMap<>();
                        dg2DataValues.put("quarter", data.get("colorCodeSet").get("fyQuarter"));
                        dg2DataValues.put("value", new ArrayList<>(Collections.singletonList(data.get(Constants.DGF_RATE_ENTRY).get(Constants.DGF_RATE))));
                        String k2 = data.get(Constants.CODE).toString().replaceAll("\"", "");
                        dg2Data.put(k2, dg2DataValues);
                        dg2PLsList.add(dg2Data);
                    });

                    //Processing DGF Sub Group 3
                    JsonNode dgSubGroup3Object = dgfSubGroup2.get(Constants.CHILDREN);
                    List<Map<String, Object>> dg3List = new ArrayList<>();
                    dgSubGroup3Object.forEach(dgfSubGroup3 -> {
                        Map<String, Object> dg3 = new HashMap<>();
                        dg3.put(key, dgfSubGroup3.get(Constants.KEY));
                        dg3.put(isActive, dgfSubGroup3.get(Constants.IS_ACTIVE));
                        dg3.put(modifiedBy, dgfSubGroup3.get(Constants.MODIFIED_BY));
                        dg3.put(baseRate, dgfSubGroup3.get(Constants.BASE_RATE));
                        List<Map<String, Object>> dg3PLsList = new ArrayList<>();

                        //Processing DGF Sub Group 3 PLs (Columns)
                        JsonNode subGroup3Data = dgfSubGroup3.get(Constants.COLUMNS);
                        subGroup3Data.forEach(data -> {
                            Map<String, Object> dg3Data = new HashMap<>();
                            Map<String, Object> dg3DataValues = new HashMap<>();
                            dg3DataValues.put("quarter", data.get("colorCodeSet").get("fyQuarter"));
                            dg3DataValues.put("value", new ArrayList<>(Collections.singletonList(data.get(Constants.DGF_RATE_ENTRY).get("dgfRate"))));
                            String k3 = data.get(Constants.CODE).toString().replaceAll("\"", "");
                            dg3Data.put(k3, dg3DataValues);
                            dg3PLsList.add(dg3Data);
                        });
                        dg3.put("PLs", dg3PLsList);
                        dg3List.add(dg3);
                    });
                    dg2.put(children, dg3List);
                    dg2.put("PLs", dg2PLsList);
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

                        Map<String, Object> dataObject1 = new HashMap<>();
                        Map<String, Object> dataObject2 = new HashMap<>();
                        Map<String, Object> dataObject3 = new HashMap<>();

                        dataObject1.put(baseRate, "Base Rate FY20");
                        dataObject2.put(baseRate, "Effective Nov 2019 (FY20)");
                        dataObject3.put(baseRate, "Effective Jan 2020 (FY20)");

                        List<Object> columnList = new ArrayList<>();
                        List<Object> dataList = new ArrayList<>();

                        if (i == 0) {
                            Map<String, Object> titleMap = new HashMap<>();
                            titleMap.put(Constants.TITLE, Constants.TITLE_VALUE);
                            titleMap.put(Constants.DATA_INDEX, Constants.TITLE_DATA_INDEX_VALUE);
                            columnList.add(titleMap);
                        }

                        Map<String, Object> iconMap = new HashMap<>();
                        iconMap.put(Constants.TITLE, Constants.ICON_VALUE);
                        iconMap.put(Constants.DATA_INDEX, "");
                        columnList.add(iconMap);

                        JsonNode columns = businessSubCategoryNode.get(i).get(Constants.COLUMNS);

                        columns.forEach(column -> {
                            Map<String, Object> columnData = new HashMap<>();
                            columnData.put(Constants.TITLE, column.get(Constants.CODE));
                            columnData.put(Constants.DATA_INDEX, column.get(Constants.CODE));
                            columnList.add(columnData);
                            String pl = column.get(Constants.CODE).toString().replaceAll("\"", "");
                            dataObject1.put(pl, column.get(Constants.DGF_RATE_ENTRY).get(Constants.DGF_RATE));
                            dataObject2.put(pl, column.get("colorCodeSet").get("name"));
                            dataObject3.put(pl, column.get("colorCodeSet").get("name"));
                        });

                        dataList.add(dataObject1);
                        dataList.add(dataObject2);
                        dataList.add(dataObject3);

                        headerData.add(new HeaderObject(columnList, dataList));
                    }
                }
            }
        });
        return headerData;
    }

    @Override
    public ColorCode getColorCodeByFyQuarter(final String fyQuarter) {
        return colorCodeRepository.getColorCodeByFyQuarter(fyQuarter);
    }

    @Override
    public List<BusinessCategory> getBusinessCategories() {
        return businessCategoryRepository.findAll();
    }

    @Override
    public List<DGFRateEntry> getBaseRateByPLs() {
        return null;
    }

    @Override
    public List<DGFRateChangeLog> getDGFRateChangeLogByRateEntryId(int rateEntryId) {
        return dgfRateChangeLogRepository.getDGFRateChangeLogByRateEntryId(rateEntryId);
    }
}
