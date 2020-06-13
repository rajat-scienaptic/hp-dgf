package com.hp.dgf.controller;

import com.hp.dgf.dto.request.DGFGroupDTO;
import com.hp.dgf.service.DGFGroupDataService;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

@RequestMapping("/api/v1")
@RestController
public final class DGFGroupsController {

    @Autowired
    private DGFGroupDataService dgfGroupDataService;

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

    @ApiOperation(value = "To add a new dgf group", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Successfully added new dgf group"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("/addDgfGroup")
    public final ResponseEntity<Object> addDgfGroup(@RequestBody @Valid final DGFGroupDTO dgfGroupDTO, HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.addDgfGroup(dgfGroupDTO, request), HttpStatus.CREATED);
    }

    @ApiOperation(value = "To add a new dgf sub group level 1", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Successfully added new dgf sub group level 1"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("/addDgfSubGroupLevel1")
    public final ResponseEntity<Object> addDgfSubGroupLevel1(@RequestBody @Valid final DGFGroupDTO dgfGroupDTO, final HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.addDgfSubGroupLevel1(dgfGroupDTO, request), HttpStatus.CREATED);
    }

    @ApiOperation(value = "To add a new dgf sub group level 2", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Successfully added new dgf sub group level 2"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("/addDgfSubGroupLevel2")
    public final ResponseEntity<Object> addDgfSubGroupLevel2(@RequestBody @Valid final DGFGroupDTO dgfGroupDTO, final HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.addDgfSubGroupLevel2(dgfGroupDTO, request), HttpStatus.CREATED);
    }

    @ApiOperation(value = "To add a new dgf sub group level 3", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Successfully added new dgf sub group level 3"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PostMapping("/addDgfSubGroupLevel3")
    public final ResponseEntity<Object> addDgfSubGroupLevel3(@RequestBody @Valid final DGFGroupDTO dgfGroupDTO, final HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.addDgfSubGroupLevel3(dgfGroupDTO, request), HttpStatus.CREATED);
    }

    @ApiOperation(value = "To delete a dgf group by id", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted dgf group"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("/deleteDgFGroup/{dgfGroupId}")
    public final ResponseEntity<Object> deleteDgFGroup(@PathVariable ("dgfGroupId") final int dgfGroupId, final HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.deleteDgfGroup(dgfGroupId, request), HttpStatus.OK);
    }

    @ApiOperation(value = "To delete a dgf sub group level 1 by id", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted dgf sub group level 1"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("/deleteDgfSubGroupLevel1/{dgfSubGroupLevel1Id}")
    public final ResponseEntity<Object> deleteDgfSubGroupLevel1(@PathVariable ("dgfSubGroupLevel1Id") final int dgfSubGroupLevel1Id, final HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.deleteDgfSubGroupLevel1(dgfSubGroupLevel1Id, request), HttpStatus.OK);
    }

    @ApiOperation(value = "To delete a dgf sub group level 2 by id", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted dgf sub group level 2"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("/deleteDgfSubGroupLevel2/{dgfSubGroupLevel2Id}")
    public final ResponseEntity<Object> deleteDgfSubGroupLevel2(@PathVariable ("dgfSubGroupLevel2Id") final int dgfSubGroupLevel2Id, final HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.deleteDgfSubGroupLevel2(dgfSubGroupLevel2Id, request), HttpStatus.OK);
    }


    @ApiOperation(value = "To delete a dgf sub group level 3 by id", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully deleted dgf sub group level 3"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @DeleteMapping("/deleteDgfSubGroupLevel3/{dgfSubGroupLevel3Id}")
    public final ResponseEntity<Object> deleteDgfSubGroupLevel3(@PathVariable ("dgfSubGroupLevel3Id") final int dgfSubGroupLevel3Id, final HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.deleteDgfSubGroupLevel3(dgfSubGroupLevel3Id, request), HttpStatus.OK);
    }

    @ApiOperation(value = "To update an existing DGF Group", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated the DGF Group"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("/updateDgfGroup/{dgfGroupsId}")
    public final ResponseEntity<Object> updateDgfGroup(@RequestBody @Valid final DGFGroupDTO dgfGroupDTO, @PathVariable ("dgfGroupsId") final int dgfGroupsId, final HttpServletRequest request){
      return new ResponseEntity<>(dgfGroupDataService.updateDgfGroup(dgfGroupDTO, dgfGroupsId, request), HttpStatus.OK);
    }

    @ApiOperation(value = "To update an existing DGF Sub Group Level 1", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated the DGF Sub Group Level 1"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("/updateDgfSubGroupLevel1/{dgfSubGroupLevel1Id}")
    public final ResponseEntity<Object> updateDgfSubGroupLevel1(@RequestBody @Valid final DGFGroupDTO dgfGroupDTO, @PathVariable ("dgfGroupsId") final int dgfGroupsId, final HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.updateDgfSubGroupLevel1(dgfGroupDTO, dgfGroupsId, request), HttpStatus.OK);
    }

    @ApiOperation(value = "To update an existing DGF Sub Group Level 2", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated the DGF Sub Group Level 2"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("/updateDgfSubGroupLevel2/{dgfSubGroupLevel2Id}")
    public final ResponseEntity<Object> updateDgfSubGroupLevel2(@RequestBody @Valid final DGFGroupDTO dgfGroupDTO, @PathVariable ("dgfGroupsId") final int dgfGroupsId, final HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.updateDgfSubGroupLevel2(dgfGroupDTO, dgfGroupsId, request), HttpStatus.OK);
    }

    @ApiOperation(value = "To update an existing DGF Sub Group Level 3", response = Iterable.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully updated the DGF Sub Group Level 3"),
            @ApiResponse(code = 400, message = "You have made an invalid request"),
            @ApiResponse(code = 401, message = "You are not authorized to view the resource"),
            @ApiResponse(code = 403, message = "Accessing the resource you were trying to reach is forbidden"),
            @ApiResponse(code = 404, message = "The resource you were trying to reach is not found")
    })
    @PutMapping("/updateDgfGroup/{dgfSubGroupLevel3Id}")
    public final ResponseEntity<Object> updateDgfSubGroupLevel3(@RequestBody @Valid final DGFGroupDTO dgfGroupDTO, @PathVariable ("dgfGroupsId") final int dgfGroupsId, final HttpServletRequest request){
        return new ResponseEntity<>(dgfGroupDataService.updateDgfSubGroupLevel3(dgfGroupDTO, dgfGroupsId, request), HttpStatus.OK);
    }
}