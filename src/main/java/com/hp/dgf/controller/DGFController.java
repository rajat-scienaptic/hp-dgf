package com.hp.dgf.controller;

import com.hp.dgf.dto.request.PLRequest;
import com.hp.dgf.model.BusinessCategory;
import com.hp.dgf.model.ColorCode;
import com.hp.dgf.model.DGFRateChangeLog;
import com.hp.dgf.repository.BusinessCategoryRepository;
import com.hp.dgf.repository.DGFRepository;
import com.hp.dgf.service.DGFService;
import com.hp.dgf.service.ReportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.List;

@RequestMapping("/api/v1")
@RestController
public class DGFController {

  @Autowired
  private DGFService dgfService;

  @Autowired
  private ReportService reportService;

  @Autowired
  private DGFRepository dgfRepository;

  @Autowired
  private BusinessCategoryRepository businessCategoryRepository;

  @GetMapping("/getDgfGroups")
  public ResponseEntity<Object> getDgfGroups(){
    return new ResponseEntity<>(dgfService.getDgfGroups(), HttpStatus.OK);
  }

  @PostMapping("/addPL")
  public ResponseEntity<Object> addPL(@RequestBody @Valid PLRequest plRequest){
    return new ResponseEntity<>(dgfService.addPL(plRequest), HttpStatus.CREATED);
  }

  @GetMapping("/getBusinessGroups")
  public ResponseEntity<Object> getBusinessGroups(){
    return new ResponseEntity<>(businessCategoryRepository.findAll(), HttpStatus.OK);
  }

  @GetMapping("/getHeaderData")
  public ResponseEntity<Object> getHeaderData(){
    return new ResponseEntity<>(dgfService.getHeaderData(), HttpStatus.OK);
  }

  @GetMapping("/getColorCodeByFyQuarter/{fyQuarter}")
  public ResponseEntity<ColorCode> getColorCodeByFyQuarter(@PathVariable ("fyQuarter") String fyQuarter){
    return new ResponseEntity<>(dgfService.getColorCodeByFyQuarter(fyQuarter), HttpStatus.OK);
  }

  @GetMapping("/getBusinessCategories")
  public ResponseEntity<List<BusinessCategory>> getBusinessCategories(){
    return new ResponseEntity<>(dgfService.getBusinessCategories(), HttpStatus.OK);
  }

  @GetMapping("/downloadDGFReport")
  public ResponseEntity<Object> downloadDGFReport(){
    reportService.downloadDGFReport();
    return new ResponseEntity<>("Report Successfully Downloaded", HttpStatus.OK);
  }

  @GetMapping("/generateDGFReport")
  public ResponseEntity<Object> generateDGFReport(){
    reportService.generateDGFReport();
    return new ResponseEntity<>("Report Successfully Generated", HttpStatus.OK);
  }

  @GetMapping("/getDGFRateChangeLogByRateEntryId/{rateEntryId}")
  public ResponseEntity<List<DGFRateChangeLog>> getDGFRateChangeLogByRateEntryId(@PathVariable ("rateEntryId") int rateEntryId){
    return new ResponseEntity<>(dgfService.getDGFRateChangeLogByRateEntryId(rateEntryId), HttpStatus.OK);
  }
}
