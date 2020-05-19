package com.hp.dgf.controller;

import com.hp.dgf.dto.request.PLRequestDTO;
import com.hp.dgf.exception.CustomException;
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

  @GetMapping("/getDgfGroups/{id}")
  public ResponseEntity<Object> getDgfGroups(@PathVariable ("id") int businessCategoryId){
    List<Object> dgfGroupsList = dgfService.getDgfGroups(businessCategoryId);
    if(dgfGroupsList.isEmpty()){
      throw new CustomException("No Business Category Found for ID : "+businessCategoryId, HttpStatus.BAD_REQUEST);
    }
    return new ResponseEntity<>(dgfGroupsList, HttpStatus.OK);
  }

  @GetMapping("/getHeaderData")
  public ResponseEntity<Object> getHeaderData(){
    return new ResponseEntity<>(dgfService.getHeaderData(), HttpStatus.OK);
  }

  @PostMapping("/addPL")
  public ResponseEntity<Object> addPL(@RequestBody @Valid PLRequestDTO plRequestDTO){
    return new ResponseEntity<>(dgfService.addPL(plRequestDTO), HttpStatus.CREATED);
  }

  @PutMapping("/updatePL/{id}")
  public ResponseEntity<Object> updatePL(@RequestBody @Valid PLRequestDTO plRequestDTO, @PathVariable ("id") int productLineId){
    return new ResponseEntity<>(dgfService.updatePL(plRequestDTO, productLineId), HttpStatus.CREATED);
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
}
