package com.hp.dgf.controller;

import com.hp.dgf.dto.request.AddDgfRateEntryDTO;
import com.hp.dgf.dto.request.AddPLRequestDTO;
import com.hp.dgf.dto.request.UpdateDGFRateEntryDTO;
import com.hp.dgf.dto.request.UpdatePLRequestDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.service.DGFGroupDataService;
import com.hp.dgf.service.DGFHeaderDataService;
import com.hp.dgf.service.DGFRateEntryService;
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
    private DGFGroupDataService dgfGroupDataService;

    @Autowired
    private DGFHeaderDataService dgfHeaderDataService;

    @Autowired
    private DGFRateEntryService dgfRateEntryService;

    @ApiOperation(value = "Returns all dgf groups data corresponding to a business category", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully returned current date and time"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("/getDgfGroupsData/{businessCategoryId}")
    public final ResponseEntity<Object> getDgfGroupsDataByBusinessId(@PathVariable("businessCategoryId") final int businessCategoryId) {
        return new ResponseEntity<>(dgfGroupDataService.getDgfGroups(businessCategoryId), HttpStatus.OK);
    }

    @ApiOperation(value = "Returns header data for all business categories", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully returned current date and time"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("/getHeaderData")
    public final ResponseEntity<Object> getHeaderData() {
        return new ResponseEntity<>(dgfHeaderDataService.getHeaderData(), HttpStatus.OK);
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
    public final ResponseEntity<Object> addProductLine(@RequestBody(required = false) @Valid final AddPLRequestDTO addPlRequestDTO, final HttpServletRequest request) {
        if (addPlRequestDTO == null) {
            return new ResponseEntity<>(ApiResponseDTO.builder()
                    .status(HttpStatus.BAD_REQUEST.value())
                    .message("Required request body is missing ")
                    .timestamp(LocalDateTime.now())
                    .build(), HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<>(dgfHeaderDataService.addPL(addPlRequestDTO, request), HttpStatus.CREATED);
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
    public final ResponseEntity<Object> updateProductLine(@RequestBody(required = false) @Valid UpdatePLRequestDTO updatePLRequestDTO, @PathVariable("productLineId") int productLineId, HttpServletRequest request) {
        if (updatePLRequestDTO == null) {
            return new ResponseEntity<>(ApiResponseDTO.builder()
                    .status(HttpStatus.BAD_REQUEST.value())
                    .message("Required request body is missing ")
                    .timestamp(LocalDateTime.now())
                    .build(), HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<>(dgfHeaderDataService.updatePL(updatePLRequestDTO, productLineId, request), HttpStatus.OK);
    }

    @ApiOperation(value = "To add a new dgf rate entry", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Successfully added new dgf rate entry"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("/addDgfRateEntry")
    public final ResponseEntity<Object> addNewDGFRateEntry(@RequestBody(required = false) @Valid final AddDgfRateEntryDTO addDgfRateEntryDTO, final HttpServletRequest request) {
        if (addDgfRateEntryDTO == null) {
            return new ResponseEntity<>(ApiResponseDTO.builder()
                    .status(HttpStatus.BAD_REQUEST.value())
                    .message("Required request body is missing ")
                    .timestamp(LocalDateTime.now())
                    .build(), HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<>(dgfRateEntryService.addDGFRateEntry(addDgfRateEntryDTO, request), HttpStatus.CREATED);
    }

    @ApiOperation(value = "To update an existing dgf rate entry", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated the dgf rate entry"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })

    @PutMapping("/updateDgfRateEntry/{dgfRateEntryId}")
    public final ResponseEntity<Object> updateDGFRateEntry(@RequestBody(required = false) @Valid UpdateDGFRateEntryDTO updateDGFRateEntryDTO, @PathVariable("dgfRateEntryId") int dgfRateEntryId, HttpServletRequest request) {
        if (updateDGFRateEntryDTO == null) {
            return new ResponseEntity<>(ApiResponseDTO.builder()
                    .status(HttpStatus.BAD_REQUEST.value())
                    .message("Required request body is missing ")
                    .timestamp(LocalDateTime.now())
                    .build(), HttpStatus.BAD_REQUEST);
        }
        return new ResponseEntity<>(dgfRateEntryService.updateDGFRateEntry(updateDGFRateEntryDTO, dgfRateEntryId, request), HttpStatus.OK);
    }
}
