package com.hp.dgf.controller;

import com.hp.dgf.dto.request.AddPLRequestDTO;
import com.hp.dgf.dto.request.UpdatePLRequestDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.repository.BusinessCategoryRepository;
import com.hp.dgf.repository.DGFRepository;
import com.hp.dgf.service.DGFService;
import com.hp.dgf.service.ReportService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.time.LocalDateTime;

@RequestMapping("/api/v1")
@RestController
public final class DGFController {

  @Autowired
  private DGFService dgfService;

  @Autowired
  private ReportService reportService;

  @Autowired
  private DGFRepository dgfRepository;

  @Autowired
  private BusinessCategoryRepository businessCategoryRepository;

  @ApiOperation(value = "Returns all dgf groups data corresponding to a business category", response = Iterable.class)
  @ApiResponses(value = {
          @ApiResponse(code = 200, message = "Successfully returned current date and time"),
          @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
          @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
          @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
  })
  @GetMapping("/getDgfGroupsData/{businessCategoryId}")
  public final ResponseEntity<Object> getDgfGroupsDataByBusinessId(@PathVariable ("businessCategoryId") final int businessCategoryId){
    return new ResponseEntity<>(dgfService.getDgfGroups(businessCategoryId), HttpStatus.OK);
  }

  @ApiOperation(value = "Returns header data for all business categories", response = Iterable.class)
  @ApiResponses(value = {
          @ApiResponse(code = 200, message = "Successfully returned current date and time"),
          @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
          @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
          @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
  })
  @GetMapping("/getHeaderData")
  public final ResponseEntity<Object> getHeaderData(){
    return new ResponseEntity<>(dgfService.getHeaderData(), HttpStatus.OK);
  }

  @ApiOperation(value = "To add a new product line", response = Iterable.class)
  @ApiResponses(value = {
          @ApiResponse(code = 201, message = "Successfully created new product line"),
          @ApiResponse(code = 400, message = "You have made an invalid request"),
          @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
          @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
          @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
  })
  @PostMapping("/addPL")
  public final ResponseEntity<Object> addProductLine(@RequestBody(required = false) @Valid final AddPLRequestDTO addPlRequestDTO, final HttpServletRequest request){
    if (addPlRequestDTO == null) {
      return new ResponseEntity<>(ApiResponseDTO.builder()
              .status(HttpStatus.BAD_REQUEST.value())
              .message("Required request body is missing ")
              .timestamp(LocalDateTime.now())
              .build(), HttpStatus.BAD_REQUEST);
    }
    return new ResponseEntity<>(dgfService.addPL(addPlRequestDTO, request), HttpStatus.CREATED);
  }

  @ApiOperation(value = "To update an existing product line", response = Iterable.class)
  @ApiResponses(value = {
          @ApiResponse(code = 200, message = "Successfully updated the product line"),
          @ApiResponse(code = 400, message = "You have made an invalid request"),
          @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
          @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
          @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
  })
  @PutMapping("/updatePL/{productLineId}")
  public final ResponseEntity<Object> updateProductLine(@RequestBody(required = false) @Valid UpdatePLRequestDTO updatePLRequestDTO, @PathVariable ("productLineId") int productLineId, HttpServletRequest request){
    if (updatePLRequestDTO == null) {
      return new ResponseEntity<>(ApiResponseDTO.builder()
              .status(HttpStatus.BAD_REQUEST.value())
              .message("Required request body is missing ")
              .timestamp(LocalDateTime.now())
              .build(), HttpStatus.BAD_REQUEST);
    }
    return new ResponseEntity<>(dgfService.updatePL(updatePLRequestDTO, productLineId, request), HttpStatus.OK);
  }
}
