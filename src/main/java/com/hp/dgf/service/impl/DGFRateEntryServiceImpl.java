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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

@Service
public class DGFRateEntryServiceImpl implements DGFRateEntryService {
    @Autowired
    private DGFRateEntryRepository dgfRateEntryRepository;
    @Autowired
    private DGFLogRepository dgfLogRepository;
    @Autowired
    private DGFSubGroupLevel1Repository dgfSubGroupLevel1Repository;
    @Autowired
    private DGFSubGroupLevel2Repository dgfSubGroupLevel2Repository;
    @Autowired
    private DGFSubGroupLevel3Repository dgfSubGroupLevel3Repository;
    @Autowired
    private ProductLineRepository productLineRepository;
    @Autowired
    private BusinessSubCategoryRepository businessSubCategoryRepository;
    @Autowired
    private BusinessCategoryRepository businessCategoryRepository;
    @Autowired
    private DGFRateChangeLogRepository dgfRateChangeLogRepository;
    @Autowired
    private AttachmentRepository attachmentRepository;
    @Autowired
    private AttachmentService attachmentService;

    @Override
    public ApiResponseDTO addDGFRateEntry(AddDgfRateEntryDTO addDgfRateEntryDTO, HttpServletRequest request) {
        try {

            ProductLine productLine = productLineRepository.checkIfPlExists(addDgfRateEntryDTO.getProductLineName());

            if(productLine == null){
                throw new CustomException("Product Line with name : "+addDgfRateEntryDTO.getProductLineName()
                        + " not found !", HttpStatus.NOT_FOUND);
            }

            DGFRateEntry dgfRateEntry = DGFRateEntry.builder()
                    .createdBy(addDgfRateEntryDTO.getCreatedBy())
                    .createdOn(LocalDateTime.now())
                    .dgfRate(addDgfRateEntryDTO.getDgfRate())
                    .productLineId(productLine.getId())
                    .build();

            if(addDgfRateEntryDTO.getDgfSubGroupLevel3Id() != null){
                dgfRateEntry.setDgfSubGroupLevel3Id(addDgfRateEntryDTO.getDgfSubGroupLevel3Id());
            }

            if(addDgfRateEntryDTO.getDgfSubGroupLevel2Id() != null){
                dgfRateEntry.setDgfSubGroupLevel2Id(addDgfRateEntryDTO.getDgfSubGroupLevel2Id());
            }

            int dgfRateEntryId = dgfRateEntryRepository.save(dgfRateEntry).getId();

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
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
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public ApiResponseDTO updateDGFRateEntry(UpdateDGFRateEntryDTO updateDGFRateEntryDTO, int dgfRateEntryId, HttpServletRequest request, MultipartFile file) {
        try{
            final DGFRateEntry dgfRateEntry = dgfRateEntryRepository.findById(dgfRateEntryId)
                    .orElseThrow(() -> {
                        throw new CustomException("DGF Rate Entry with id : "+dgfRateEntryId+" does not exist !", HttpStatus.NOT_FOUND);
                    });

            final int attachmentId = attachmentRepository.save(Attachment.builder()
                    .attachmentPath(saveAttachment(file))
                    .build()).getId();

            final BigDecimal previousBaseRate = dgfRateEntry.getDgfRate();

            dgfRateEntry.setAttachmentId(attachmentId);
            dgfRateEntry.setNote(updateDGFRateEntryDTO.getNote());
            dgfRateEntry.setDgfRate(updateDGFRateEntryDTO.getDgfRate());

            dgfRateEntryRepository.save(dgfRateEntry);

            dgfRateChangeLogRepository.save(DGFRateChangeLog.builder()
                    .attachmentId(attachmentId)
                    .createdBy(updateDGFRateEntryDTO.getCreatedBy())
                    .createdOn(LocalDateTime.now())
                    .dgfRate(previousBaseRate)
                    .dgfRateEntryId(dgfRateEntryId)
                    .note(updateDGFRateEntryDTO.getNote())
                    .build());

            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.SUCCESS)
                        .message("DGF Rate Entry with id : " + dgfRateEntryId + " has been successfully updated !")
                        .createTime(LocalDateTime.now())
                        .build());
            }

            return ApiResponseDTO.builder()
                    .timestamp(LocalDateTime.now())
                    .status(HttpStatus.CREATED.value())
                    .message("DGF Rate Entry with id : " + dgfRateEntryId + " has been successfully updated !")
                    .build();
        } catch (Exception e) {
            if (request != null) {
                dgfLogRepository.save(DGFLogs.builder()
                        .ip(request.getRemoteAddr())
                        .endpoint(request.getRequestURI())
                        .type(request.getMethod())
                        .status(Variables.FAILURE)
                        .message(e.getMessage())
                        .createTime(LocalDateTime.now())
                        .build());
            }
            throw new CustomException(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @Override
    public Object getDgfRateEntryDataById(int dgfRateEntryId) {
        String seller = null;
        String dgfGroup = null;

        final DGFRateEntry dgfRateEntry = dgfRateEntryRepository.findById(dgfRateEntryId)
                .orElseThrow(() -> {
                    throw new CustomException("DGF Rate Entry not found for id : "+dgfRateEntryId, HttpStatus.NOT_FOUND);
                });

        if(dgfRateEntry.getDgfSubGroupLevel2Id() != null && dgfRateEntry.getDgfSubGroupLevel3Id() == null){
            final DGFSubGroupLevel2 dgfSubGroupLevel2 = dgfSubGroupLevel2Repository.findById(dgfRateEntry.getDgfSubGroupLevel2Id())
                    .orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 2 not found for id : "+dgfRateEntry.getDgfSubGroupLevel2Id(), HttpStatus.NOT_FOUND);
                    });
            seller = dgfSubGroupLevel2.getBaseRate();

            final DGFSubGroupLevel1 dgfSubGroupLevel1 = dgfSubGroupLevel1Repository.findById(dgfSubGroupLevel2.getDgfSubGroupLevel1Id())
                    .orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 1 not found for id : "
                                +dgfSubGroupLevel2.getDgfSubGroupLevel1Id(), HttpStatus.NOT_FOUND);
                    });

            dgfGroup = dgfSubGroupLevel1.getBaseRate();

        }else if(dgfRateEntry.getDgfSubGroupLevel2Id() == null && dgfRateEntry.getDgfSubGroupLevel3Id() != null){
            final DGFSubGroupLevel3 dgfSubGroupLevel3 = dgfSubGroupLevel3Repository.findById(dgfRateEntry.getDgfSubGroupLevel3Id())
                    .orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 3 not found for id : "+dgfRateEntry.getDgfSubGroupLevel3Id(), HttpStatus.NOT_FOUND);
                    });

            seller = dgfSubGroupLevel3.getBaseRate();

            final DGFSubGroupLevel2 dgfSubGroupLevel2 = dgfSubGroupLevel2Repository.
                    findById(dgfSubGroupLevel3.getDgfSubGroupLevel2Id())
                    .orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 2 not found for id : "
                                +dgfSubGroupLevel3.getDgfSubGroupLevel2Id(), HttpStatus.NOT_FOUND);
                    });

            final DGFSubGroupLevel1 dgfSubGroupLevel1 = dgfSubGroupLevel1Repository
                    .findById(dgfSubGroupLevel2.getDgfSubGroupLevel1Id())
                    .orElseThrow(() -> {
                        throw new CustomException("DGF Sub Group Level 1 not found for id : "
                                +dgfSubGroupLevel2.getDgfSubGroupLevel1Id(), HttpStatus.NOT_FOUND);
                    });

            dgfGroup = dgfSubGroupLevel1.getBaseRate();
        }

        final ProductLine productLineObject = productLineRepository.findById(dgfRateEntry.getProductLineId())
                .orElseThrow(() -> {
                    throw new CustomException("Product Line not found for id : "+dgfRateEntry.getProductLineId(), HttpStatus.NOT_FOUND);
                });

        final String productLine = productLineObject.getCode();

        final BusinessSubCategory businessSubCategory = businessSubCategoryRepository.findById(productLineObject.getBusinessSubCategoryId())
                .orElseThrow(() -> {
                    throw new CustomException("Business Sub Category not found for id : "+productLineObject.getBusinessSubCategoryId(), HttpStatus.NOT_FOUND);
                });

        final BusinessCategory businessCategory = businessCategoryRepository.findById(businessSubCategory.getBusinessCategoryId())
                .orElseThrow(() -> {
                    throw new CustomException("Business Category not found for id : "+businessSubCategory.getBusinessCategoryId(), HttpStatus.NOT_FOUND);
                });

        final String category = businessCategory.getName();

        final List<DGFRateChangeLog> dgfRateChangeLogList = dgfRateChangeLogRepository.getDGFRateChangeLogByRateEntryId(dgfRateEntryId);
        final List<DGFRateChangeLogDTO> dgfRateChangeHistoryList = new LinkedList<>();

        dgfRateChangeLogList.forEach(dgfRateChangeLog -> {
            Attachment attachment = attachmentRepository.findById(dgfRateChangeLog.getAttachmentId())
                    .orElseThrow(() -> {
                       throw new CustomException("No Attachment was " +
                               "found for id :"+dgfRateChangeLog.getAttachmentId(), HttpStatus.NOT_FOUND);
                    });
            dgfRateChangeHistoryList.add(DGFRateChangeLogDTO.builder()
                    .attachment(attachment.getAttachmentPath())
                    .modifiedBy(dgfRateChangeLog.getCreatedBy())
                    .notes(dgfRateChangeLog.getNote())
                    .date(dgfRateChangeLog.getCreatedOn())
                    .baseRate(dgfRateChangeLog.getDgfRate())
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
        return attachmentService.save(file);
    }
}
