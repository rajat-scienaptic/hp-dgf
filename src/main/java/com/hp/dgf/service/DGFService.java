package com.hp.dgf.service;

import com.hp.dgf.model.BusinessCategory;
import com.hp.dgf.model.ColorCode;
import com.hp.dgf.model.DGFRateChangeLog;
import com.hp.dgf.model.DGFRateEntry;

import java.util.List;

public interface DGFService {
    List<Object> getDgfGroups();
    List<Object> getHeaderData();
    ColorCode getColorCodeByFyQuarter(String fyQuarter);
    List<BusinessCategory> getBusinessCategories();
    List<DGFRateEntry> getBaseRateByPLs();
    List<DGFRateChangeLog> getDGFRateChangeLogByRateEntryId(int rateEntryId);
}
