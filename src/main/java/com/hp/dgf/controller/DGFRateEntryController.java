package com.hp.dgf.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.constants.Variables;
import com.hp.dgf.dto.request.AddDgfRateEntryDTO;
import com.hp.dgf.dto.request.UpdateDGFRateEntryDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.service.DGFRateEntryService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.time.LocalDateTime;

@CrossOrigin(origins = "*", maxAge = 3600)
@RequestMapping("/api/v1")
@RestController
public class DGFRateEntryController {

    @Autowired
    private DGFRateEntryService dgfRateEntryService;

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
                    .message(Variables.REQUEST_BODY_ERROR)
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
    @PutMapping(value = "/updateDgfRateEntry/{dgfRateEntryId}", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public final ResponseEntity<Object> updateDGFRateEntry(@RequestParam ("jsonData") String jsonData,
                                                           @PathVariable("dgfRateEntryId") final int dgfRateEntryId,
                                                           final HttpServletRequest request,
                                                           @RequestParam ("file") @Valid @NotNull @NotBlank MultipartFile file) throws IOException {
        if (jsonData == null || jsonData.isEmpty()) {
            return new ResponseEntity<>(ApiResponseDTO.builder()
                    .status(HttpStatus.BAD_REQUEST.value())
                    .message(Variables.REQUEST_BODY_ERROR)
                    .timestamp(LocalDateTime.now())
                    .build(), HttpStatus.BAD_REQUEST);
        }
        ObjectMapper mapper = new ObjectMapper();
        UpdateDGFRateEntryDTO updateDGFRateEntryDTO = mapper.readValue(jsonData, UpdateDGFRateEntryDTO.class);
        return new ResponseEntity<>(dgfRateEntryService.updateDGFRateEntry(updateDGFRateEntryDTO, dgfRateEntryId, request, file), HttpStatus.OK);
    }

    @ApiOperation(value = "To get dgf entry data for specific dgfEntryId", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully returned dgf entry data"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @GetMapping("/getDgfRateEntryData/{dgfRateEntryId}")
    public final ResponseEntity<Object> getDgfRateEntryData(@PathVariable ("dgfRateEntryId") final int dgfRateEntryId){
        return new ResponseEntity<>(dgfRateEntryService.getDgfRateEntryDataById(dgfRateEntryId), HttpStatus.OK);
    }

}
