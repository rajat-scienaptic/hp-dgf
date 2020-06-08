package com.hp.dgf.helper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.constants.Variables;
import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.BusinessCategory;
import com.hp.dgf.model.DGFRateEntry;
import com.hp.dgf.repository.BusinessCategoryRepository;
import com.hp.dgf.repository.BusinessSubCategoryRepository;
import com.hp.dgf.repository.DGFRateEntryRepository;
import com.hp.dgf.repository.ProductLineRepository;
import com.hp.dgf.utils.MonthService;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@Service
public class ExcelHelper {
    @Autowired
    private BusinessCategoryRepository businessCategoryRepository;
    @Autowired
    private ProductLineRepository productLineRepository;
    @Autowired
    private BusinessSubCategoryRepository businessSubCategoryRepository;
    @Autowired
    private MonthService monthService;
    @Autowired
    private DGFRateEntryRepository dgfRateEntryRepository;

    private ExcelHelper() {
    }

    private static final String SHEET = "DGF Tool";

    public ByteArrayInputStream generateReport(LocalDateTime createdOn) {
        final List<BusinessCategory> headerBusinessCategory = businessCategoryRepository.findAll();

        //Initialized Object Mapper to Map Json Object
        final ObjectMapper mapper = new ObjectMapper();

        JsonNode businessCategoryNode = mapper.convertValue(headerBusinessCategory, JsonNode.class);

        try (XSSFWorkbook workbook = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Sheet sheet = workbook.createSheet("DGF Sheet");
            sheet.autoSizeColumn(50);
            createBcRow(businessCategoryNode, workbook, sheet, createdOn);
            createBscRow(businessCategoryNode, sheet);
            createPlRow(businessCategoryNode, sheet);
            workbook.write(out);
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            throw new CustomException("Failed to export data to Excel file: " + e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    private void createBcRow(JsonNode businessCategoryNode, Workbook workbook, Sheet sheet, LocalDateTime createdOn) {
        Row row1 = sheet.createRow(0);
        int columnCount = 1;
        int rowCount = 5;
        for (int i = 0; i < businessCategoryNode.size(); i++) {
            String name = businessCategoryNode.get(i).get("name").toString().replaceAll("\"", "");
            row1.createCell(columnCount).setCellValue(name);
            columnCount = plCountPerBC(businessCategoryNode.get(i), workbook, sheet) + columnCount;
            createDGFDataRows(businessCategoryNode, workbook, sheet, rowCount, createdOn);
        }
    }

    private void createBscRow(JsonNode businessCategoryNode, Sheet sheet) {
        Row row2 = sheet.createRow(1);
        int columnCount = 1;
        for (int i = 0; i < businessCategoryNode.size(); i++) {
            JsonNode bscNode = businessCategoryNode.get(i).get(Variables.CHILDREN);
            for (int j = 0; j < bscNode.size(); j++) {
                String name = bscNode.get(j).get("name").toString().replaceAll("\"", "");
                row2.createCell(columnCount).setCellValue(name);
                columnCount = plCountPerBSC(bscNode.get(j)) + columnCount;
            }
        }
    }

    private void createPlRow(JsonNode businessCategoryNode, Sheet sheet) {
        final LocalDate currentDate = LocalDate.now();
        final String year = String.valueOf(currentDate.getYear());
        final String fyYear = year.substring(year.length() - 2);

        final int quarter = monthService.getQuarter();
        final String headerBaseRateTitle = "BASE RATES FY" + fyYear;

        Row row3 = sheet.createRow(2);
        row3.createCell(0).setCellValue("PLs");
        Row row4 = sheet.createRow(3);
        row4.createCell(0).setCellValue(headerBaseRateTitle);

        int columnCount = 1;
        for (int i = 0; i < businessCategoryNode.size(); i++) {
            JsonNode bscNode = businessCategoryNode.get(i).get(Variables.CHILDREN);
            for (int j = 0; j < bscNode.size(); j++) {
                JsonNode productLine = bscNode.get(j).get(Variables.COLUMNS);
                for (int k = 0; k < productLine.size(); k++) {
                    String code = productLine.get(k).get("code").toString().replaceAll("\"", "");
                    String baseRate = productLine.get(k).get("baseRate").toString().replaceAll("\"", "");
                    row3.createCell(columnCount).setCellValue(code);
                    row4.createCell(columnCount).setCellValue(baseRate);
                    columnCount++;
                }
            }
        }

//        final String effectiveFirstQuarterTitle = Variables.EFFECTIVE + " " + monthService.getMonthRange(quarter) + " " +
//                monthService.getYear() + " " + "(" + monthService.getQuarterName(quarter) + ")";
//        final String effectiveSecondQuarterTitle = Variables.EFFECTIVE + " " + monthService.getMonthRange(quarter + 1) + " " +
//                year + " " + "(" + monthService.getQuarterName(quarter + 1) + ")";
//        final String effectiveThirdQuarterTitle = Variables.EFFECTIVE + " " + monthService.getMonthRange(quarter + 2) + " " +
//                year + " " + "(" + monthService.getQuarterName(quarter + 2) + ")";
//        final String effectiveFourthQuarterTitle = Variables.EFFECTIVE + " " + monthService.getMonthRange(quarter + 3) + " " +
//                year + " " + "(" + monthService.getQuarterName(quarter + 3) + ")";
//
//        Row row5 = sheet.createRow(4);
//        row5.createCell(0).setCellValue(effectiveFirstQuarterTitle);
//        Row row6 = sheet.createRow(5);
//        row6.createCell(0).setCellValue(effectiveSecondQuarterTitle);
//        Row row7 = sheet.createRow(6);
//        row7.createCell(0).setCellValue(effectiveThirdQuarterTitle);
//        Row row8 = sheet.createRow(7);
//        row8.createCell(0).setCellValue(effectiveFourthQuarterTitle);
    }

    private void createDGFDataRows(JsonNode businessCategoryNode, Workbook workbook, Sheet sheet, int rowCount, LocalDateTime createdOn) {
        for (int i = 0; i < businessCategoryNode.size(); i++) {
            JsonNode dgfGroupSet = businessCategoryNode.get(i).get("dgfGroups");
            if (!dgfGroupSet.isEmpty()) {
                rowCount = createDGFGroupsRows(dgfGroupSet, workbook, sheet, rowCount, createdOn);
            }
        }
    }

    private int createDGFGroupsRows(JsonNode dgfGroupSet, Workbook workbook, Sheet sheet, int rowCount, LocalDateTime createdOn) {
        CellStyle style = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setColor(HSSFColor.HSSFColorPredefined.WHITE.getIndex());
        font.setFontHeightInPoints((short) 9);
        font.setBold(true);
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.BLUE.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        for (int j = 0; j < dgfGroupSet.size(); j++) {
            Row dgfRows = sheet.createRow(rowCount);
            JsonNode dgfSubGroupLevel1 = dgfGroupSet.get(j).get(Variables.CHILDREN);
            String dgfGroupName = dgfGroupSet.get(j).get(Variables.BASE_RATE)
                    .toString().replaceAll("\"", "");
            dgfRows.createCell(0).setCellValue(dgfGroupName);
            dgfRows.getCell(0).setCellStyle(style);
            rowCount++;
            rowCount = createSubGroupLevel1Rows(dgfSubGroupLevel1, workbook, sheet, rowCount, createdOn);
        }
        return rowCount;
    }

    private int createSubGroupLevel1Rows(JsonNode dgfSubGroupLevel1, Workbook workbook, Sheet sheet, int rowCount, LocalDateTime createdOn) {
        for (int k = 0; k < dgfSubGroupLevel1.size(); k++) {
            Row dgfSubGroupLevel1Rows = sheet.createRow(rowCount);
            JsonNode dgfSubGroupLevel2 = dgfSubGroupLevel1.get(k).get(Variables.CHILDREN);
            String dgfSubGroupLevel1Name = dgfSubGroupLevel1.get(k).get(Variables.BASE_RATE)
                    .toString().replaceAll("\"", "");
            dgfSubGroupLevel1Rows.createCell(0).setCellValue(dgfSubGroupLevel1Name);
            rowCount++;
            rowCount = createSubGroupLevel2Rows(dgfSubGroupLevel2, workbook, sheet, rowCount, createdOn);
        }
        return rowCount;
    }

    private int createSubGroupLevel2Rows(JsonNode dgfSubGroupLevel2, Workbook workbook, Sheet sheet, int rowCount, LocalDateTime createdOn) {
        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        for (int k = 0; k < dgfSubGroupLevel2.size(); k++) {
            JsonNode dgfSubGroupLevel3 = dgfSubGroupLevel2.get(k).get(Variables.CHILDREN);
            Row dgfSubGroupLevel2Rows = sheet.createRow(rowCount);
            String dgfSubGroupLevel2Name = dgfSubGroupLevel2.get(k).get(Variables.BASE_RATE)
                    .toString().replaceAll("\"", "");
            dgfSubGroupLevel2Rows.createCell(0).setCellValue(dgfSubGroupLevel2Name);
            dgfSubGroupLevel2Rows.getCell(0).setCellStyle(style);
            rowCount++;
            rowCount = createSubGroupLevel3Rows(dgfSubGroupLevel3, workbook, sheet, rowCount, createdOn);
            int id = Integer.parseInt(dgfSubGroupLevel2.get(k).get("id").toString());
            List<DGFRateEntry> dgfRateEntryList = dgfRateEntryRepository.
                    findByDgfSubGroupLevel2IdAndCreatedOnBetween(id, createdOn, LocalDateTime.now());
            for (int m = 1; m < dgfRateEntryList.size(); m++) {
                String dgfRate = dgfRateEntryList.get(m - 1).getDgfRate().toString().replaceAll("\"", "");
                dgfSubGroupLevel2Rows.createCell(m).setCellValue(dgfRate);
            }
        }
        return rowCount;
    }

    private int createSubGroupLevel3Rows(JsonNode dgfSubGroupLevel3, Workbook workbook, Sheet sheet, int rowCount, LocalDateTime createdOn) {
        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.RIGHT);
        for (int k = 0; k < dgfSubGroupLevel3.size(); k++) {
            Row dgfSubGroupLevel3Rows = sheet.createRow(rowCount);
            String dgfSubGroupLevel3Name = dgfSubGroupLevel3.get(k).get(Variables.BASE_RATE)
                    .toString().replaceAll("\"", "");
            dgfSubGroupLevel3Rows.createCell(0).setCellValue(dgfSubGroupLevel3Name);
            dgfSubGroupLevel3Rows.getCell(0).setCellStyle(style);
            int id = Integer.parseInt(dgfSubGroupLevel3.get(k).get("id").toString());
            List<DGFRateEntry> dgfRateEntryList = dgfRateEntryRepository.
                    findByDgfSubGroupLevel3IdAndCreatedOnBetween(id, createdOn, LocalDateTime.now());
            for (int m = 1; m < dgfRateEntryList.size(); m++) {
                String dgfRate = dgfRateEntryList.get(m - 1).getDgfRate().toString().replaceAll("\"", "");
                dgfSubGroupLevel3Rows.createCell(m).setCellValue(dgfRate);
            }
            rowCount++;
        }
        return rowCount;
    }

    public static List<DGFRateEntry> excelToTDgfRateEntry(InputStream is) {
        try {
            Workbook workbook = new XSSFWorkbook(is);

            Sheet sheet = workbook.getSheet(SHEET);
            Iterator<Row> rows = sheet.iterator();

            List<DGFRateEntry> dgfRateEntryList = new LinkedList<>();

            int rowNumber = 0;
            while (rows.hasNext()) {
                Row currentRow = rows.next();

                // skip header
                if (rowNumber == 0) {
                    rowNumber++;
                    continue;
                }

                Iterator<Cell> cellsInRow = currentRow.iterator();

                DGFRateEntry dgfRateEntry = new DGFRateEntry();

                int cellIdx = 0;
                while (cellsInRow.hasNext()) {
                    Cell currentCell = cellsInRow.next();

                    switch (cellIdx) {
                        case 0:
                            dgfRateEntry.setId((int) currentCell.getNumericCellValue());
                            break;

                        case 1:
                            dgfRateEntry.setCreatedOn(LocalDateTime.parse(currentCell.getStringCellValue()));
                            break;

                        case 2:
                            dgfRateEntry.setCreatedBy(currentCell.getStringCellValue());
                            break;

                        case 3:
                            dgfRateEntry.setNote(currentCell.getStringCellValue());
                            break;

                        case 4:
                            dgfRateEntry.setDgfSubGroupLevel2Id((int) currentCell.getNumericCellValue());
                            break;

                        case 5:
                            dgfRateEntry.setDgfSubGroupLevel3Id((int) currentCell.getNumericCellValue());
                            break;

                        case 6:
                            dgfRateEntry.setProductLineId((int) currentCell.getNumericCellValue());
                            break;

                        case 7:
                            dgfRateEntry.setDgfRate(BigDecimal.valueOf(currentCell.getNumericCellValue()));
                            break;

                        case 8:
                            dgfRateEntry.setDgfSubGroupLevel2Id((int) currentCell.getNumericCellValue());
                            break;

                        default:
                            break;
                    }

                    cellIdx++;
                }

                dgfRateEntryList.add(dgfRateEntry);
            }

            workbook.close();

            return dgfRateEntryList;
        } catch (IOException e) {
            throw new CustomException("fail to parse Excel file: " + e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    private int plCountPerBC(JsonNode businessSubCategoryNode, Workbook workbook, Sheet sheet) {
        int count = 0;
        JsonNode bscNode = businessSubCategoryNode.get(Variables.CHILDREN);
        for (int j = 0; j < bscNode.size(); j++) {
            count = bscNode.get(j).get(Variables.COLUMNS).size() + count;
        }
        return count;
    }

    private int plCountPerBSC(JsonNode businessSubCategoryNode) {
        int count = 0;
        count = businessSubCategoryNode.get(Variables.COLUMNS).size() + count;
        return count;
    }

}