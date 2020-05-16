package com.hp.dgf.service;

import com.hp.dgf.dto.request.PLRequest;
import com.hp.dgf.model.BusinessCategory;
import com.hp.dgf.model.ColorCode;
import com.hp.dgf.model.DGFRateChangeLog;
import com.hp.dgf.model.ProductLine;

import java.util.List;

public interface DGFService {
    List<Object> getDgfGroups();
    List<Object> getHeaderData();
    ColorCode getColorCodeByFyQuarter(String fyQuarter);
    List<BusinessCategory> getBusinessCategories();
    List<DGFRateChangeLog> getDGFRateChangeLogByRateEntryId(int rateEntryId);
    ProductLine addPL(PLRequest plRequest);
}
