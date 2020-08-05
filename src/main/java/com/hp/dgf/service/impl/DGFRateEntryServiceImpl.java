package com.hp.dgf.service.impl;

import com.hp.dgf.constants.Variables;
import com.hp.dgf.dto.request.AddDgfRateEntryDTO;
import com.hp.dgf.dto.request.UpdateDGFRateEntryDTO;
import com.hp.dgf.dto.response.ApiResponseDTO;
import com.hp.dgf.dto.response.DGFRateChangeLogDTO;
import com.hp.dgf.dto.response.DGFRateEntryResponseDTO;
import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.*;
import com.hp.dgf.repository.*;
import com.hp.dgf.service.AttachmentService;
import com.hp.dgf.service.DGFRateEntryService;
import com.hp.dgf.service.UserValidationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;

@Service
public class DGFRateEntryServiceImpl implements DGFRateEntryService {
    @Autowired
    DGFRateEntryRepository dgfRateEntryRepository;
    @Autowired
    DGFLogRepository dgfLogRepository;
    @Autowired
    DGFSubGroupLevel1Repository dgfSubGroupLevel1Repository;
    @Autowired
    DGFSubGroupLevel2Repository dgfSubGroupLevel2Repository;
    @Autowired
    DGFSubGroupLevel3Repository dgfSubGroupLevel3Repository;
    @Autowired
    ProductLineRepository productLineRepository;
    @Autowired
    BusinessSubCategoryRepository businessSubCategoryRepository;
    @Autowired
    BusinessCategoryRepository businessCategoryRepository;
    @Autowired
    DGFRateChangeLogRepository dgfRateChangeLogRepository;
    @Autowired
    AttachmentRepository attachmentRepository;
    @Autowired
    AttachmentService attachmentService;
    @Autowired
    UserValidationService userValidationService;

    @Override
    public ApiResponseDTO addDGFRateEntry(final AddDgfRateEntryDTO addDgfRateEntryDTO,
                                          final HttpServletRequest request,
                                          final String cookie) {
        try {

            ProductLine productLine = productLineRepository.checkIfPlExists(addDgfRateEntryDTO.getProductLineName());

            if(productLine == null){
                throw new CustomException("Product Line with name : "+addDgfRateEntryDTO.getProductLineName()
                        + " not found !", HttpStatus.NOT_FOUND);
            }

            DGFRateEntry dgfRateEntry = DGFRateEntry.builder()
                    .createdBy(userValidationService.getUserNameFromCookie(cookie))
                    .createdOn(LocalDateTime.now())
                    .dgfRate(addDgfRateEntryDTO.getDgfRate())
                    .productLineId(productLine.getId())
                    .build();

            if(addDgfRateEntryDTO.getDgfSubGroupLevel3Id() != null){
                checkIfDgfRateEntryForSubGroupLevel3Exists(addDgfRateEntryDTO.getDgfSubGroupLevel3Id(), productLine.getId());
                checkIfDgfRateForSubGroupLevel3IsNotBiggerThanThatOfSubGroupLevel2(addDgfRateEntryDTO.getDgfSubGroupLevel3Id(), productLine.getId(), addDgfRateEntryDTO.getDgfRate());
                dgfRateEntry.setDgfSubGroupLevel3Id(addDgfRateEntryDTO.getDgfSubGroupLevel3Id());
            }

            if(addDgfRateEntryDTO.getDgfSubGroupLevel2Id() != null){
                checkIfDgfRateEntryForSubGroupLevel2Exists(addDgfRateEntryDTO.getDgfSubGroupLevel2Id(), productLine.getId());
                dgfRateEntry.setDgfSubGroupLevel2Id(addDgfRateEntryDTO.getDgfSubGroupLevel2Id());
            }

            int dgfRateEntryId = dgfRateEntryRepository.save(dgfRateEntry).getId();

            dgfRateChangeLogRepository.save(DGFRateChangeLog.builder()
                    .attachmentId(1)
                    .createdBy(userValidationService.getUserNameFromCookie(cookie))
                    .createdOn(LocalDateTime.now())
                    .dgfRate(addDgfRateEntryDTO.getDgfRate())
                    .oldDgfRate(addDgfRateEntryDTO.getDgfRate())
                    .dgfRateEntryId(dgfRateEntryId)
                    .build());

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Rate Entry with id : " + dgfRateEntryId + " has been successfully created !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.CREATED.value())
                    .message("DGF Rate Entry with id : " + dgfRateEntryId + " has been successfully created !")
                    .build();

        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO updateDGFRateEntry(final UpdateDGFRateEntryDTO updateDGFRateEntryDTO,
                                             final int dgfRateEntryId,
                                             final HttpServletRequest request,
                                             final MultipartFile file,
                                             final String cookie) {
        try{
            final DGFRateEntry dgfRateEntry = dgfRateEntryRepository.findById(dgfRateEntryId)
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("DGF Rate Entry with id : "+dgfRateEntryId+" does not exist !", HttpStatus.NOT_FOUND);
                    });

            BigDecimal currentDgfRate = dgfRateEntry.getDgfRate();
            BigDecimal newDgfRate = updateDGFRateEntryDTO.getDgfRate();

            if(currentDgfRate.compareTo(newDgfRate) == 0){
                throw new CustomException("New Dgf Rate cannot be same as the current Dgf Rate !", HttpStatus.BAD_REQUEST);
            }

            if(dgfRateEntry.getDgfSubGroupLevel3Id() != null){
                checkIfDgfRateForSubGroupLevel3IsNotBiggerThanThatOfSubGroupLevel2(dgfRateEntry.getDgfSubGroupLevel3Id(), dgfRateEntry.getProductLineId(), updateDGFRateEntryDTO.getDgfRate());
            }

            if(dgfRateEntry.getDgfSubGroupLevel2Id() != null){
                checkIfDgfRateForSubGroupLevel2IsNotSmallerThanThatOfSubGroupLevel3(dgfRateEntry.getDgfSubGroupLevel2Id(), dgfRateEntry.getProductLineId(), updateDGFRateEntryDTO.getDgfRate());
            }

            final int attachmentId = attachmentRepository.save(Attachment.builder()
                    .attachmentPath(saveAttachment(file))
                    .build()).getId();

            dgfRateEntry.setAttachmentId(attachmentId);
            dgfRateEntry.setNote(updateDGFRateEntryDTO.getNote());
            dgfRateEntry.setDgfRate(updateDGFRateEntryDTO.getDgfRate());

            dgfRateEntryRepository.save(dgfRateEntry);

            dgfRateChangeLogRepository.save(DGFRateChangeLog.builder()
                    .attachmentId(attachmentId)
                    .createdBy(userValidationService.getUserNameFromCookie(cookie))
                    .createdOn(LocalDateTime.now())
                    .oldDgfRate(currentDgfRate)
                    .dgfRate(newDgfRate)
                    .dgfRateEntryId(dgfRateEntryId)
                    .note(updateDGFRateEntryDTO.getNote())
                    .build());

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message("DGF Rate Entry with id : " + dgfRateEntryId + " has been successfully updated !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.CREATED.value())
                    .message("DGF Rate Entry with id : " + dgfRateEntryId + " has been successfully updated !")
                    .body(getDgfRateEntryDataById(dgfRateEntryId))
                    .build();
        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .username(userValidationService.getUserNameFromCookie(cookie))
                        .message(e.getMessage())
                        .createTime(LocalDateTime.now())
                        .build());
            }
            if(e.getClass().equals(CustomException.class)){
                throw new CustomException(e.getMessage(), ((CustomException) e).getBody(), HttpStatus.BAD_REQUEST);
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public Object getDgfRateEntryDataById(int dgfRateEntryId) {
        String seller = null;
        String dgfGroup = null;

        final DGFRateEntry dgfRateEntry = dgfRateEntryRepository.findById(dgfRateEntryId)
                .<CustomException>orElseThrow(() -> {
                    throw new CustomException("DGF Rate Entry not found for id : "+dgfRateEntryId, HttpStatus.NOT_FOUND);
                });

        if(dgfRateEntry.getDgfSubGroupLevel2Id() != null && dgfRateEntry.getDgfSubGroupLevel3Id() == null){
            final DGFSubGroupLevel2 dgfSubGroupLevel2 = dgfSubGroupLevel2Repository.findById(dgfRateEntry.getDgfSubGroupLevel2Id())
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 2 not found for id : "+dgfRateEntry.getDgfSubGroupLevel2Id(), HttpStatus.NOT_FOUND);
                    });
            seller = dgfSubGroupLevel2.getBaseRate();

            final DGFSubGroupLevel1 dgfSubGroupLevel1 = dgfSubGroupLevel1Repository.findById(dgfSubGroupLevel2.getDgfSubGroupLevel1Id())
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 1 not found for id : "
                                +dgfSubGroupLevel2.getDgfSubGroupLevel1Id(), HttpStatus.NOT_FOUND);
                    });

            dgfGroup = dgfSubGroupLevel1.getBaseRate();

        }else if(dgfRateEntry.getDgfSubGroupLevel2Id() == null && dgfRateEntry.getDgfSubGroupLevel3Id() != null){
            final DGFSubGroupLevel3 dgfSubGroupLevel3 = dgfSubGroupLevel3Repository.findById(dgfRateEntry.getDgfSubGroupLevel3Id())
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 3 not found for id : "+dgfRateEntry.getDgfSubGroupLevel3Id(), HttpStatus.NOT_FOUND);
                    });

            seller = dgfSubGroupLevel3.getBaseRate();

            final DGFSubGroupLevel2 dgfSubGroupLevel2 = dgfSubGroupLevel2Repository.
                    findById(dgfSubGroupLevel3.getDgfSubGroupLevel2Id())
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 2 not found for id : "
                                +dgfSubGroupLevel3.getDgfSubGroupLevel2Id(), HttpStatus.NOT_FOUND);
                    });

            final DGFSubGroupLevel1 dgfSubGroupLevel1 = dgfSubGroupLevel1Repository
                    .findById(dgfSubGroupLevel2.getDgfSubGroupLevel1Id())
                    .<CustomException>orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 1 not found for id : "
                                +dgfSubGroupLevel2.getDgfSubGroupLevel1Id(), HttpStatus.NOT_FOUND);
                    });

            dgfGroup = dgfSubGroupLevel1.getBaseRate();
        }

        final ProductLine productLineObject = productLineRepository.findById(dgfRateEntry.getProductLineId())
                .<CustomException>orElseThrow(() -> {
                    throw new CustomException("Product Line not found for id : "+dgfRateEntry.getProductLineId(), HttpStatus.NOT_FOUND);
                });

        final String productLine = productLineObject.getCode();

        final BusinessSubCategory businessSubCategory = businessSubCategoryRepository.findById(productLineObject.getBusinessSubCategoryId())
                .<CustomException>orElseThrow(() -> {
                    throw new CustomException("Business Sub Category not found for id : "+productLineObject.getBusinessSubCategoryId(), HttpStatus.NOT_FOUND);
                });

        final BusinessCategory businessCategory = businessCategoryRepository.findById(businessSubCategory.getBusinessCategoryId())
                .<CustomException>orElseThrow(() -> {
                    throw new CustomException("Business Category not found for id : "+businessSubCategory.getBusinessCategoryId(), HttpStatus.NOT_FOUND);
                });

        final String category = businessCategory.getName();

        final List<DGFRateChangeLog> dgfRateChangeLogList = dgfRateChangeLogRepository.getDGFRateChangeLogByRateEntryId(dgfRateEntryId);
        final List<DGFRateChangeLogDTO> dgfRateChangeHistoryList = new LinkedList<>();

        dgfRateChangeLogList.forEach(dgfRateChangeLog -> {
            Attachment attachment = attachmentRepository.findById(dgfRateChangeLog.getAttachmentId())
                    .<CustomException>orElseThrow(() -> {
                       throw new CustomException("No Attachment was " +
                               "found for id :"+dgfRateChangeLog.getAttachmentId(), HttpStatus.NOT_FOUND);
                    });
            dgfRateChangeHistoryList.add(DGFRateChangeLogDTO.builder()
                    .attachment(attachment.getAttachmentPath())
                    .modifiedBy(dgfRateChangeLog.getCreatedBy())
                    .notes(dgfRateChangeLog.getNote())
                    .date(dgfRateChangeLog.getCreatedOn())
                    .baseRate(dgfRateChangeLog.getOldDgfRate())
                    .build());
        });

        dgfRateChangeHistoryList.sort(Comparator.comparing(DGFRateChangeLogDTO::getDate).reversed());

        return DGFRateEntryResponseDTO.builder()
                .category(category)
                .seller(seller)
                .dgfGroup(dgfGroup)
                .productLine(productLine)
                .dgfRate(dgfRateEntry.getDgfRate())
                .dgfRateChangeLogs(dgfRateChangeHistoryList)
                .build();
    }

    private String saveAttachment(final MultipartFile file){
        return attachmentService.saveAttachment(file);
    }

    private void checkIfDgfRateForSubGroupLevel3IsNotBiggerThanThatOfSubGroupLevel2(int dgfSubGroupLevel3Id, int productLineId, BigDecimal dgfRateOfSubGroupLevel3){
        final int dgfSubGroupLevel2Id = dgfSubGroupLevel3Repository.getDgfSubGroupLevel2Id(dgfSubGroupLevel3Id);
        final BigDecimal dgfRateOfSubGroupLevel2 = dgfRateEntryRepository.getDgfRateOfSubGroupLevel2(dgfSubGroupLevel2Id, productLineId);
        final List<Integer> dgfSubGroupLevel3IdList = dgfSubGroupLevel3Repository.getDgfSubGroupLevel3IdList(dgfSubGroupLevel2Id);

        if(dgfSubGroupLevel3IdList.isEmpty()){
            if(dgfRateOfSubGroupLevel3.compareTo(dgfRateOfSubGroupLevel2) > 0){
                throw new CustomException("Dgf Rate of Sub Group Level 3 cannot be bigger than the Dgf Rate of Sub Group Level 2", HttpStatus.BAD_REQUEST);
            }
        }else {
            final List<DGFRateEntry> dgfRateEntryList = new LinkedList<>();
            for(int id : dgfSubGroupLevel3IdList){
                final DGFRateEntry dgfRateEntry = dgfRateEntryRepository.findByDgfSubGroupLevel3IdAndProductLineId(id, productLineId);
                if(dgfRateEntry != null){
                    dgfRateEntryList.add(dgfRateEntry);
                }
                if(id == dgfSubGroupLevel3Id){
                    dgfRateEntryList.remove(dgfRateEntry);
                }
            }
            final BigDecimal existingTotal = dgfRateEntryList.stream()
                    .map(DGFRateEntry::getDgfRate)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            final BigDecimal newTotal = existingTotal.add(dgfRateOfSubGroupLevel3);
            if(newTotal.compareTo(dgfRateOfSubGroupLevel2) > 0){
                final BigDecimal maxValueAllowed = dgfRateOfSubGroupLevel2.subtract(existingTotal);
                final Map<String, Object> errorMap = new LinkedHashMap<>();
                String message = "Sum of Existing Sub Group Level 3 members Dgf Rates cannot be bigger than the Dgf Rate of Sub Group Level 2.";
                errorMap.put("existingTotal", existingTotal);
                errorMap.put("newTotal", newTotal);
                errorMap.put("maxValueAllowed", maxValueAllowed);
                throw new CustomException(message, errorMap, HttpStatus.BAD_REQUEST);
            }
        }
    }

    private void checkIfDgfRateForSubGroupLevel2IsNotSmallerThanThatOfSubGroupLevel3(int dgfSubGroupLevel2Id, int productLineId, BigDecimal dgfRateOfSubGroupLevel2){
        final List<Integer> dgfSubGroupLevel3IdList = dgfSubGroupLevel3Repository.getDgfSubGroupLevel3IdList(dgfSubGroupLevel2Id);

        if(!dgfSubGroupLevel3IdList.isEmpty()){
            final List<DGFRateEntry> dgfRateEntryList = new LinkedList<>();
            for(int id : dgfSubGroupLevel3IdList){
                final DGFRateEntry dgfRateEntry = dgfRateEntryRepository.findByDgfSubGroupLevel3IdAndProductLineId(id, productLineId);
                if(dgfRateEntry != null){
                    dgfRateEntryList.add(dgfRateEntry);
                }
            }
            final BigDecimal existingTotal = dgfRateEntryList.stream()
                    .map(DGFRateEntry::getDgfRate)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
            if(existingTotal.compareTo(dgfRateOfSubGroupLevel2) > 0){
                throw new CustomException("Dgf Rate Value of Sub Group Level 2 cannot be smaller than the sum of it's children Dgf Rate Values !", HttpStatus.BAD_REQUEST);
            }
        }
    }

    private void checkIfDgfRateEntryForSubGroupLevel3Exists(int dgfSubGroupLevel3Id, int productLineId){
        DGFRateEntry dgfRateEntry = dgfRateEntryRepository.findByDgfSubGroupLevel3IdAndProductLineId(dgfSubGroupLevel3Id, productLineId);
        if(dgfRateEntry != null){
            throw new CustomException("Dgf Rate Entry already exists for DgfSubGroupLevel3 with id : "+dgfSubGroupLevel3Id, HttpStatus.BAD_REQUEST);
        }
    }

    private void checkIfDgfRateEntryForSubGroupLevel2Exists(int dgfSubGroupLevel2Id, int productLineId){
        DGFRateEntry dgfRateEntry = dgfRateEntryRepository.findByDgfSubGroupLevel2IdAndProductLineId(dgfSubGroupLevel2Id, productLineId);
        if(dgfRateEntry != null){
            throw new CustomException("Dgf Rate Entry already exists for DgfSubGroupLevel2 with id : "+dgfSubGroupLevel2Id, HttpStatus.BAD_REQUEST);
        }
    }

}
