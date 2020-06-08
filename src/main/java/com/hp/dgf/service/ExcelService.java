package com.hp.dgf.service;

import com.hp.dgf.model.DGFRateEntry;
import org.springframework.web.multipart.MultipartFile;

import java.io.ByteArrayInputStream;
import java.time.LocalDateTime;
import java.util.List;

public interface ExcelService {
    void save(MultipartFile file);
    ByteArrayInputStream load();
    List<DGFRateEntry> getDgfEntryData();
    ByteArrayInputStream generateExcel(LocalDateTime createdOn);
}
