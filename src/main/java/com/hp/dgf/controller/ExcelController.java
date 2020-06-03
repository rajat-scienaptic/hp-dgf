package com.hp.dgf.controller;

import com.hp.dgf.service.ExcelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@CrossOrigin(origins = "*", maxAge = 3600)
@RequestMapping("/api/v1/")
@RestController
public class ExcelController {
  @Autowired
  private ExcelService excelService;

  @GetMapping("/generateReport")
  public final ResponseEntity<InputStreamResource> generateExcel(){
    HttpHeaders headers = new HttpHeaders();
    headers.add("Content-Disposition", "attachment; filename=dgf.xlsx");
    return ResponseEntity
            .ok()
            .headers(headers)
            .body(excelService.generateExcel());
  }
}
