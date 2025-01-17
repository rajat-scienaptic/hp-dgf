package com.hp.dgf.controller;

import com.hp.dgf.constants.Variables;
import com.hp.dgf.dto.request.AddPLRequestDTO;
import com.hp.dgf.dto.request.UpdatePLRequestDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.exception.CustomException;
import com.hp.dgf.service.DGFHeaderDataService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import java.math.BigDecimal;
import java.time.LocalDateTime;

//@CrossOrigin(origins = "*")
@RequestMapping("/api/v1")
@RestController
public class DGFHeaderController {

    @Autowired
    private DGFHeaderDataService dgfHeaderDataService;

    @ApiOperation(value = "Returns header data for all business categories", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully returned current date and time"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("/getHeaderData/{businessCategoryId}")
    public final ResponseEntity<Object> getHeaderData(@PathVariable("businessCategoryId") final int businessCategoryId) {
        return new ResponseEntity<>(dgfHeaderDataService.getHeaderData(businessCategoryId), HttpStatus.OK);
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
    public final ResponseEntity<Object> addProductLine(@RequestBody(required = false) @Valid final AddPLRequestDTO addPlRequestDTO,
                                                       final HttpServletRequest request,
                                                       @RequestHeader(value = "Cookie", required = false) final String cookie) {
        if (addPlRequestDTO == null) {
            return new ResponseEntity<>(ApiResponseDTO.builder()
                    .status(HttpStatus.BAD_REQUEST.value())
                    .message(Variables.REQUEST_BODY_ERROR)
                    .timestamp(LocalDateTime.now())
                    .build(), HttpStatus.BAD_REQUEST);
        }
        if (addPlRequestDTO.getBaseRate().compareTo(BigDecimal.ZERO) < 0) {
            throw new CustomException("PL base rate cannot be negative !", HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<>(dgfHeaderDataService.addPL(addPlRequestDTO, request, cookie), HttpStatus.CREATED);
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
    public final ResponseEntity<Object> updateProductLine(@RequestBody(required = false) @Valid UpdatePLRequestDTO updatePLRequestDTO,
                                                          @PathVariable("productLineId") final int productLineId,
                                                          final HttpServletRequest request,
                                                          @RequestHeader(value = "Cookie", required = false) final String cookie) {
        if (updatePLRequestDTO == null) {
            return new ResponseEntity<>(ApiResponseDTO.builder()
                    .status(HttpStatus.BAD_REQUEST.value())
                    .message(Variables.REQUEST_BODY_ERROR)
                    .timestamp(LocalDateTime.now())
                    .build(), HttpStatus.BAD_REQUEST);
        }

        if (updatePLRequestDTO.getBaseRate() != null && updatePLRequestDTO.getBaseRate().compareTo(BigDecimal.ZERO) < 0) {
            throw new CustomException("PL base rate cannot be negative !", HttpStatus.BAD_REQUEST);
        }

        return new ResponseEntity<>(dgfHeaderDataService.updatePL(updatePLRequestDTO, productLineId, request, cookie), HttpStatus.OK);
    }

    @ApiOperation(value = "To delete a product line by id", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted product line"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("/deletePL/{productLineId}")
    public final ResponseEntity<Object> deletePL(@PathVariable("productLineId") final int productLineId,
                                                 final HttpServletRequest request,
                                                 @RequestHeader(value = "Cookie", required = false) final String cookie) {
        return new ResponseEntity<>(dgfHeaderDataService.deletePL(productLineId, request, cookie), HttpStatus.OK);
    }

}
