package com.hp.dgf.service.impl;

import com.hp.dgf.helper.ExcelHelper;
import com.hp.dgf.model.DGFRateEntry;
import com.hp.dgf.repository.DGFRateEntryRepository;
import com.hp.dgf.service.ExcelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
@Service
public class ExcelServiceImpl implements ExcelService {
    @Autowired
    DGFRateEntryRepository dgfRateEntryRepository;

    @Autowired
    ExcelHelper excelHelper;

    @Override
    public void save(MultipartFile file) {
        List<DGFRateEntry> tutorials = new ArrayList<>();
        dgfRateEntryRepository.saveAll(tutorials);
    }

    @Override
    public ByteArrayInputStream load() {
        List<DGFRateEntry> tutorials = dgfRateEntryRepository.findAll();
        return null;
    }

    @Override
    public List<DGFRateEntry> getDgfEntryData() {
        return dgfRateEntryRepository.findAll();
    }

    @Override
    public InputStreamResource generateExcel() {
        List<DGFRateEntry> dgfRateEntryList = getDgfEntryData();
        return null;
    }
}