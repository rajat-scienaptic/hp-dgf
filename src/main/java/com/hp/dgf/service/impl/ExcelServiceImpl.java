package com.hp.dgf.service.impl;

import com.hp.dgf.helper.ExcelHelper;
import com.hp.dgf.repository.DGFRateEntryRepository;
import com.hp.dgf.service.ExcelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.time.LocalDateTime;
@Service
public class ExcelServiceImpl implements ExcelService {
    @Autowired
    DGFRateEntryRepository dgfRateEntryRepository;

    @Autowired
    ExcelHelper excelHelper;

    @Override
    public ByteArrayInputStream generateExcel(LocalDateTime createdOn) {
        return excelHelper.generateReport(createdOn);
    }

}