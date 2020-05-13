package com.hp.dgf.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.dto.Column;
import com.hp.dgf.dto.DataObject;
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

    @Override
    public final List<Object> getDgfGroups() {
        List<BusinessCategory> businessCategoryList = businessCategoryRepository.findAll();
        ObjectMapper mapper = new ObjectMapper();
        List<Object> finalOutput = new ArrayList<>();

        businessCategoryList.forEach(businessCategory -> {
            JsonNode businessCategoryNode = mapper.convertValue(businessCategory, JsonNode.class);
            if (businessCategoryNode.has(Constants.CHILDREN)) {
                JsonNode businessSubCategoryNode = businessCategoryNode.get(Constants.CHILDREN);
                if (!businessSubCategoryNode.isEmpty()) {
                    finalOutput.add(new Column(getTitleObject(businessSubCategoryNode.get(0))));
                    for (int i = 1; i < businessSubCategoryNode.size(); i++) {
                        List<Map<String, Object>> columnList = new ArrayList<>();

                        Map<String, Object> iconMap = new HashMap<>();
                        iconMap.put(Constants.TITLE, Constants.ICON_VALUE);
                        iconMap.put(Constants.DATA_INDEX, "");
                        columnList.add(iconMap);

                        JsonNode columns = businessSubCategoryNode.get(i).get(Constants.COLUMNS);
                        columns.forEach(column -> {
                            Map<String, Object> columnData = new HashMap<>();
                            columnData.put(Constants.TITLE, column.get("code"));
                            columnData.put(Constants.DATA_INDEX, column.get("code"));
                            columnList.add(columnData);
                        });

                        finalOutput.add(new Column(columnList));
                    }
                }
            }
        });

        finalOutput.add(new DataObject(getDataObject()));

        return finalOutput;
    }

    @Override
    public final List<Object> getHeaderData() {
        List<BusinessCategory> businessCategoryList = businessCategoryRepository.findAll();
        ObjectMapper mapper = new ObjectMapper();
        List<Object> finalOutput = new ArrayList<>();

        businessCategoryList.forEach(businessCategory -> {
            JsonNode businessCategoryNode = mapper.convertValue(businessCategory, JsonNode.class);
            if (businessCategoryNode.has(Constants.CHILDREN)) {
                JsonNode businessSubCategoryNode = businessCategoryNode.get(Constants.CHILDREN);
                if (!businessSubCategoryNode.isEmpty()) {
                    finalOutput.add(new Column(getTitleObject(businessSubCategoryNode.get(0))));
                    for (int i = 1; i < businessSubCategoryNode.size(); i++) {
                        List<Map<String, Object>> columnList = new ArrayList<>();

                        Map<String, Object> iconMap = new HashMap<>();
                        iconMap.put(Constants.TITLE, Constants.ICON_VALUE);
                        iconMap.put(Constants.DATA_INDEX, "");
                        columnList.add(iconMap);

                        JsonNode columns = businessSubCategoryNode.get(i).get(Constants.COLUMNS);
                        columns.forEach(column -> {
                            Map<String, Object> columnData = new HashMap<>();
                            columnData.put(Constants.TITLE, column.get("code"));
                            columnData.put(Constants.DATA_INDEX, column.get("code"));
                            columnList.add(columnData);
                        });

                        finalOutput.add(new Column(columnList));
                    }
                }
            }
        });

        finalOutput.add(new DataObject(getDataObject()));

        return finalOutput;
    }

    public final List<Map<String, Object>> getDataObject() {
        List<Map<String, Object>> dataObject = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        List<DGFGroups> dgfGroupsList = dgfRepository.findAll();
        for (DGFGroups dgfGroup : dgfGroupsList) {
            //Parent DGF Group
            Map<String, Object> dg = new HashMap<>();
            dg.put(key, dgfGroup.getKey());
            dg.put(isActive, dgfGroup.getIsActive());
            dg.put(modifiedBy, dgfGroup.getModifiedBy());
            dg.put(baseRate, dgfGroup.getBaseRate());
            JsonNode dgfGroupObject = mapper.convertValue(dgfGroup, JsonNode.class);

            //DGF Sub Group 1
            JsonNode dgfSubGroup1Object = dgfGroupObject.get(Constants.CHILDREN);
            List<Map<String, Object>> dg1List = new ArrayList<>();
            dgfSubGroup1Object.forEach(dgfSubGroup1 -> {
                Map<String, Object> dg1 = new HashMap<>();
                dg1.put(key, dgfSubGroup1.get(Constants.KEY));
                dg1.put(isActive, dgfSubGroup1.get(Constants.IS_ACTIVE));
                dg1.put(modifiedBy, dgfSubGroup1.get(Constants.MODIFIED_BY));
                dg1.put(baseRate, dgfSubGroup1.get(Constants.BASE_RATE));

                //DGF Sub Group 2
                JsonNode dgfSubGroup2Object = dgfSubGroup1.get(Constants.CHILDREN);
                List<Map<String, Object>> dg2List = new ArrayList<>();
                dgfSubGroup2Object.forEach(dgfSubGroup2 -> {
                    Map<String, Object> dg2 = new HashMap<>();
                    dg2.put(key, dgfSubGroup2.get(Constants.KEY));
                    dg2.put(isActive, dgfSubGroup2.get(Constants.IS_ACTIVE));
                    dg2.put(modifiedBy, dgfSubGroup2.get(Constants.MODIFIED_BY));
                    dg2.put(baseRate, dgfSubGroup2.get(Constants.BASE_RATE));
                    List<Map<String, Object>> dg2PLsList = new ArrayList<>();

                    //DGF Sub Group 2 PLs (Columns)
                    JsonNode subGroup2Data = dgfSubGroup2.get(Constants.COLUMNS);
                    subGroup2Data.forEach(data -> {
                        Map<String, Object> dg2Data = new HashMap<>();
                        Map<String, Object> dg2DataValues = new HashMap<>();
                        dg2DataValues.put("quarter", data.get("colorCodeSet").get("fyQuarter"));
                        dg2DataValues.put("value", new ArrayList<>(Collections.singletonList(data.get(Constants.DGF_RATE_ENTRY).get(Constants.DGF_RATE))));
                        String k2 = data.get("code").toString().replaceAll("\"", "");
                        dg2Data.put(k2, dg2DataValues);
                        dg2PLsList.add(dg2Data);
                    });

                    //DGF Sub Group 3
                    JsonNode dgSubGroup3Object = dgfSubGroup2.get(Constants.CHILDREN);
                    List<Map<String, Object>> dg3List = new ArrayList<>();
                    dgSubGroup3Object.forEach(dgfSubGroup3 -> {
                        Map<String, Object> dg3 = new HashMap<>();
                        dg3.put(key, dgfSubGroup3.get(Constants.KEY));
                        dg3.put(isActive, dgfSubGroup3.get(Constants.IS_ACTIVE));
                        dg3.put(modifiedBy, dgfSubGroup3.get(Constants.MODIFIED_BY));
                        dg3.put(baseRate, dgfSubGroup3.get(Constants.BASE_RATE));
                        List<Map<String, Object>> dg3PLsList = new ArrayList<>();

                        //DGF Sub Group 3 PLs (Columns)
                        JsonNode subGroup3Data = dgfSubGroup3.get(Constants.COLUMNS);
                        subGroup3Data.forEach(data -> {
                            Map<String, Object> dg3Data = new HashMap<>();
                            Map<String, Object> dg3DataValues = new HashMap<>();
                            dg3DataValues.put("quarter", data.get("colorCodeSet").get("fyQuarter"));
                            dg3DataValues.put("value", new ArrayList<>(Collections.singletonList(data.get(Constants.DGF_RATE_ENTRY).get("dgfRate"))));
                            String k3 = data.get("code").toString().replaceAll("\"", "");
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

    public final List<Map<String, Object>> getTitleObject(final JsonNode categoryList) {
        List<Map<String, Object>> titleColumnList = new ArrayList<>();

        Map<String, Object> titleMap = new HashMap<>();
        titleMap.put(Constants.TITLE, Constants.TITLE_VALUE);
        titleMap.put(Constants.DATA_INDEX, Constants.TITLE_DATA_INDEX_VALUE);

        titleColumnList.add(titleMap);

        Map<String, Object> titleIconMap = new HashMap<>();
        titleIconMap.put(Constants.TITLE, Constants.ICON_VALUE);
        titleIconMap.put(Constants.DATA_INDEX, "");

        titleColumnList.add(titleIconMap);
        JsonNode columns = categoryList.get("columns");
        
        columns.forEach(column -> {
            Map<String, Object> titleColumnData = new HashMap<>();
            titleColumnData.put(Constants.TITLE, column.get("code"));
            titleColumnData.put(Constants.DATA_INDEX, column.get("code"));
            titleColumnList.add(titleColumnData);
        });

        return titleColumnList;
    }

    public final List<Map<String, Object>> getHeaderTitleObject(final JsonNode categoryList) {
        List<Map<String, Object>> titleColumnList = new ArrayList<>();
        List<Map<String, Object>> dataList = new ArrayList<>();

        Map<String, Object> titleMap = new HashMap<>();
        titleMap.put(Constants.TITLE, Constants.TITLE_VALUE);
        titleMap.put(Constants.DATA_INDEX, Constants.TITLE_DATA_INDEX_VALUE);

        titleColumnList.add(titleMap);

        Map<String, Object> titleIconMap = new HashMap<>();
        titleIconMap.put(Constants.TITLE, Constants.ICON_VALUE);
        titleIconMap.put(Constants.DATA_INDEX, "");

        Map<String, Object> dataObject1 = new HashMap<>();
        Map<String, Object> dataObject2 = new HashMap<>();
        Map<String, Object> dataObject3 = new HashMap<>();
        dataObject1.put(baseRate, "Base Rate FY20");
        dataObject2.put(baseRate, "Effective Nov 2019 (FY20)");
        dataObject3.put(baseRate, "Effective Jan 2020 (FY20)");

        titleColumnList.add(titleIconMap);
        JsonNode columns = categoryList.get(Constants.COLUMNS);
        columns.forEach(column -> {
            Map<String, Object> titleColumnData = new HashMap<>();
            titleColumnData.put(Constants.TITLE, column.get("code"));
            titleColumnData.put(Constants.DATA_INDEX, column.get("code"));
            dataObject1.put(column.get("code").toString(), column.get(Constants.DGF_RATE_ENTRY).get(Constants.DGF_RATE));
            dataObject2.put(column.get("code").toString(), column.get(Constants.DGF_RATE_ENTRY).get(Constants.DGF_RATE));
            dataObject3.put(column.get("code").toString(), column.get(Constants.DGF_RATE_ENTRY).get(Constants.DGF_RATE));
            titleColumnList.add(titleColumnData);
        });
        return titleColumnList;
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
