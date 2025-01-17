package com.hp.dgf.controller;

import com.hp.dgf.repository.DGFRateChangeLogRepository;
import com.hp.dgf.repository.DGFRateEntryRepository;
import com.hp.dgf.service.ExcelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

//@CrossOrigin(origins = "*")
@RequestMapping("/api/v1/")
@RestController
public class ExcelController {
  @Autowired
  private ExcelService excelService;

  @Autowired
  private DGFRateChangeLogRepository dgfRateChangeLogRepository;

  @Autowired
  private DGFRateEntryRepository dgfRateEntryRepository;

  @GetMapping("/generateReport/{createdOn}")
  public final ResponseEntity<InputStreamResource> generateExcel(@PathVariable("createdOn")
                                                                   @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime createdOn){
    HttpHeaders headers = new HttpHeaders();
    headers.add("Content-Disposition", "attachment; filename=dgf.xlsx");
    return ResponseEntity
            .ok()
            .headers(headers)
            .body(new InputStreamResource(excelService.generateExcel(createdOn)));
  }
}
