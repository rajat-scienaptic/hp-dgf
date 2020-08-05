package com.hp.dgf.service;

import java.io.ByteArrayInputStream;
import java.time.LocalDateTime;

public interface ExcelService {
    ByteArrayInputStream generateExcel(LocalDateTime createdOn);
}
