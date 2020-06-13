package com.hp.dgf.helper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hp.dgf.constants.Variables;
import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.BusinessCategory;
import com.hp.dgf.model.DGFRateEntry;
import com.hp.dgf.repository.BusinessCategoryRepository;
import com.hp.dgf.repository.DGFRateChangeLogRepository;
import com.hp.dgf.repository.DGFRateEntryRepository;
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
import java.util.*;

@Service
public class ExcelHelper {
    @Autowired
    BusinessCategoryRepository businessCategoryRepository;
    @Autowired
    DGFRateEntryRepository dgfRateEntryRepository;
    @Autowired
    DGFRateChangeLogRepository dgfRateChangeLogRepository;

    private ExcelHelper() {
    }

    private static final String SHEET = "DGF Tool";

    Map<Integer, Integer> subGroupLevel2IdAndRowNumberMap = new LinkedHashMap<>();
    Map<Integer, Integer> subGroupLevel3IdAndRowNumberMap = new LinkedHashMap<>();
    Map<Integer, Integer> productLineAndColumnNumberMap = new LinkedHashMap<>();

    public ByteArrayInputStream generateReport(LocalDateTime createdOn) {
        final List<BusinessCategory> headerBusinessCategory = businessCategoryRepository.findAll();

        //Initialized Object Mapper to Map Json Object
        final ObjectMapper mapper = new ObjectMapper();

        JsonNode businessCategoryNode = mapper.convertValue(headerBusinessCategory, JsonNode.class);

        try (XSSFWorkbook workbook = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Sheet sheet = workbook.createSheet("DGF Sheet");
            sheet.setColumnWidth(0, 8000);
            createBcRow(businessCategoryNode, workbook, sheet, createdOn);
            createBscRow(businessCategoryNode, workbook, sheet);
            createPlRow(businessCategoryNode, workbook, sheet);
            fillDgfSubGroupLevel2And3Rows(productLineAndColumnNumberMap, sheet, createdOn, workbook);
            workbook.write(out);
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            throw new CustomException("Failed to export data to Excel file: " + e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    private void createBcRow(JsonNode businessCategoryNode, Workbook workbook, Sheet sheet, LocalDateTime createdOn) {
        Row row1 = sheet.createRow(0);
        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        style.setFillForegroundColor(IndexedColors.LIGHT_BLUE.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderLeft(BorderStyle.THICK);
        style.setRightBorderColor(IndexedColors.BLACK.getIndex());

        Font font = workbook.createFont();
        font.setColor(HSSFColor.HSSFColorPredefined.WHITE.getIndex());
        font.setFontHeightInPoints((short) 9);
        font.setBold(true);
        style.setFont(font);

        int columnCount = 1;
        int rowCount = 5;
        int first = 1;
        int end = 0;

        for (int i = 0; i < businessCategoryNode.size(); i++) {
            String name = businessCategoryNode.get(i).get("name").toString().replaceAll("\"", "");
            row1.createCell(columnCount).setCellValue(name);
            row1.getCell(columnCount).setCellStyle(style);
            columnCount = plCountPerBC(businessCategoryNode.get(i)) + columnCount;
            int last = plCountPerBC(businessCategoryNode.get(i));
            end = first - 1 + last;
            CellRangeAddress cellRangeAddress = new CellRangeAddress(0, 0, first, end);
            sheet.addMergedRegion(cellRangeAddress);
            first = last + first;
            createDGFDataRows(businessCategoryNode, workbook, sheet, rowCount, createdOn);
        }
    }

    private void createBscRow(JsonNode businessCategoryNode, Workbook workbook, Sheet sheet) {
        Row row2 = sheet.createRow(1);
        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        style.setFillForegroundColor(IndexedColors.CORNFLOWER_BLUE.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderLeft(BorderStyle.THICK);
        style.setRightBorderColor(IndexedColors.BLACK.getIndex());

        Font font = workbook.createFont();
        font.setColor(HSSFColor.HSSFColorPredefined.WHITE.getIndex());
        font.setFontHeightInPoints((short) 9);
        font.setBold(true);
        style.setFont(font);

        int columnCount = 1;
        int first = 1;
        int end = 0;
        for (int i = 0; i < businessCategoryNode.size(); i++) {
            JsonNode bscNode = businessCategoryNode.get(i).get(Variables.CHILDREN);
            for (int j = 0; j < bscNode.size(); j++) {
                String name = bscNode.get(j).get("name").toString().replaceAll("\"", "");
                row2.createCell(columnCount).setCellValue(name);
                row2.getCell(columnCount).setCellStyle(style);
                columnCount = plCountPerBSC(bscNode.get(j)) + columnCount;
                int last = plCountPerBSC(bscNode.get(j));
                end = first - 1 + last;
                CellRangeAddress cellRangeAddress = new CellRangeAddress(1, 1, first, end);
                sheet.addMergedRegion(cellRangeAddress);
                first = last + first;
            }
        }
    }

    private void createPlRow(JsonNode businessCategoryNode, Workbook workbook, Sheet sheet) {
        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        style.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style.setBorderLeft(BorderStyle.THICK);
        style.setRightBorderColor(IndexedColors.BLACK.getIndex());
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(IndexedColors.BLACK.getIndex());

        CellStyle style1 = workbook.createCellStyle();
        style1.setFillForegroundColor(IndexedColors.LIGHT_BLUE.getIndex());
        style1.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style1.setAlignment(HorizontalAlignment.CENTER);
        style1.setVerticalAlignment(VerticalAlignment.CENTER);

        CellStyle style2 = workbook.createCellStyle();
        style2.setFillForegroundColor(IndexedColors.CORNFLOWER_BLUE.getIndex());
        style2.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        style2.setAlignment(HorizontalAlignment.CENTER);
        style2.setVerticalAlignment(VerticalAlignment.CENTER);

        Font font = workbook.createFont();
        font.setColor(HSSFColor.HSSFColorPredefined.WHITE.getIndex());
        font.setFontHeightInPoints((short) 9);
        font.setBold(true);

        Font font1 = workbook.createFont();
        font1.setColor(HSSFColor.HSSFColorPredefined.BLACK.getIndex());
        font1.setFontHeightInPoints((short) 9);
        font1.setBold(true);

        style.setFont(font1);
        style1.setFont(font);
        style2.setFont(font);

        final LocalDate currentDate = LocalDate.now();
        final String year = String.valueOf(currentDate.getYear());
        final String fyYear = year.substring(year.length() - 2);

        final String headerBaseRateTitle = "BASE RATES FY" + fyYear;

        Row row3 = sheet.createRow(2);
        row3.createCell(0).setCellValue("PLs");
        row3.getCell(0).setCellStyle(style1);
        row3.getCell(0).setCellStyle(style1);

        Row row4 = sheet.createRow(3);
        row4.createCell(0).setCellValue(headerBaseRateTitle);
        row4.getCell(0).setCellStyle(style2);
        row4.getCell(0).setCellStyle(style2);

        int columnCount = 1;

        for (int i = 0; i < businessCategoryNode.size(); i++) {
            JsonNode bscNode = businessCategoryNode.get(i).get(Variables.CHILDREN);
            for (int j = 0; j < bscNode.size(); j++) {
                JsonNode productLine = bscNode.get(j).get(Variables.COLUMNS);
                for (int k = 0; k < productLine.size(); k++) {
                    String code = productLine.get(k).get("code").toString().replaceAll("\"", "");
                    String baseRate = productLine.get(k).get("baseRate").toString().replaceAll("\"", "");
                    int productLineId = Integer.parseInt(productLine.get(k).get("id").toString());

                    row3.createCell(columnCount).setCellValue(code);
                    row3.getCell(columnCount).setCellStyle(style);

                    productLineAndColumnNumberMap.put(productLineId, columnCount);

                    row4.createCell(columnCount).setCellValue(baseRate);
                    row4.getCell(columnCount).setCellStyle(style);
                    columnCount++;
                }
            }
        }
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
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        Font font = workbook.createFont();
        font.setColor(HSSFColor.HSSFColorPredefined.WHITE.getIndex());
        font.setFontHeightInPoints((short) 9);
        font.setBold(true);
        style.setFont(font);
        style.setFillForegroundColor(IndexedColors.LIGHT_BLUE.getIndex());
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
        CellStyle style = workbook.createCellStyle();
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        style.setFillForegroundColor(IndexedColors.CORNFLOWER_BLUE.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        Font font = workbook.createFont();
        font.setColor(HSSFColor.HSSFColorPredefined.WHITE.getIndex());
        font.setFontHeightInPoints((short) 9);
        font.setBold(true);
        style.setFont(font);
        for (int k = 0; k < dgfSubGroupLevel1.size(); k++) {
            Row dgfSubGroupLevel1Rows = sheet.createRow(rowCount);
            JsonNode dgfSubGroupLevel2 = dgfSubGroupLevel1.get(k).get(Variables.CHILDREN);
            String dgfSubGroupLevel1Name = dgfSubGroupLevel1.get(k).get(Variables.BASE_RATE)
                    .toString().replaceAll("\"", "");
            dgfSubGroupLevel1Rows.createCell(0).setCellValue(dgfSubGroupLevel1Name);
            dgfSubGroupLevel1Rows.getCell(0).setCellStyle(style);
            rowCount++;
            rowCount = createSubGroupLevel2Rows(dgfSubGroupLevel2, workbook, sheet, rowCount, createdOn);
        }
        return rowCount;
    }

    private int createSubGroupLevel2Rows(JsonNode dgfSubGroupLevel2, Workbook workbook, Sheet sheet, int rowCount, LocalDateTime createdOn) {
        CellStyle style = workbook.createCellStyle();
        style.setVerticalAlignment(VerticalAlignment.CENTER);

        Font font = workbook.createFont();
        font.setFontHeightInPoints((short) 9);
        font.setColor(HSSFColor.HSSFColorPredefined.MAROON.getIndex());
        font.setBold(true);
        style.setFont(font);

        Font font1 = workbook.createFont();
        font1.setFontHeightInPoints((short) 9);
        font1.setColor(HSSFColor.HSSFColorPredefined.BLACK.getIndex());
        font1.setBold(true);

        CellStyle style1 = workbook.createCellStyle();
        style1.setAlignment(HorizontalAlignment.CENTER);
        style1.setVerticalAlignment(VerticalAlignment.CENTER);
        style1.setFont(font1);

        for (int k = 0; k < dgfSubGroupLevel2.size(); k++) {
            JsonNode dgfSubGroupLevel3 = dgfSubGroupLevel2.get(k).get(Variables.CHILDREN);
            int dgfSubGroupLevel2Id = Integer.parseInt(dgfSubGroupLevel2.get(k).get("id").toString());
            Row dgfSubGroupLevel2Rows = sheet.createRow(rowCount);
            subGroupLevel2IdAndRowNumberMap.put(rowCount, dgfSubGroupLevel2Id);
            String dgfSubGroupLevel2Name = dgfSubGroupLevel2.get(k).get(Variables.BASE_RATE)
                    .toString().replaceAll("\"", "");
            dgfSubGroupLevel2Rows.createCell(0).setCellValue(dgfSubGroupLevel2Name);
            dgfSubGroupLevel2Rows.getCell(0).setCellStyle(style);
            rowCount++;
            rowCount = createSubGroupLevel3Rows(dgfSubGroupLevel3, workbook, sheet, rowCount, createdOn);
        }

        return rowCount;
    }

    private int createSubGroupLevel3Rows(JsonNode dgfSubGroupLevel3, Workbook workbook, Sheet sheet, int rowCount, LocalDateTime createdOn) {
        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);

        Font font = workbook.createFont();
        font.setFontHeightInPoints((short) 9);
        font.setColor(HSSFColor.HSSFColorPredefined.BLACK.getIndex());
        font.setBold(true);
        style.setFont(font);

        for (int k = 0; k < dgfSubGroupLevel3.size(); k++) {
            Row dgfSubGroupLevel3Rows = sheet.createRow(rowCount);
            String dgfSubGroupLevel3Name = dgfSubGroupLevel3.get(k).get(Variables.BASE_RATE)
                    .toString().replaceAll("\"", "");

            int dgfSubGroupLevel3Id = Integer.parseInt(dgfSubGroupLevel3.get(k).get("id").toString());
            subGroupLevel3IdAndRowNumberMap.put(rowCount, dgfSubGroupLevel3Id);

            dgfSubGroupLevel3Rows.createCell(0).setCellValue(dgfSubGroupLevel3Name);
            dgfSubGroupLevel3Rows.getCell(0).setCellStyle(style);

            rowCount++;
        }
        return rowCount;
    }

    private int plCountPerBC(JsonNode businessSubCategoryNode) {
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

    private void fillDgfSubGroupLevel2And3Rows(Map<Integer, Integer> productLineAndColumnNumberMap, Sheet sheet, LocalDateTime createdOn, Workbook workbook) {

        Font font = workbook.createFont();
        font.setFontHeightInPoints((short) 9);
        font.setColor(HSSFColor.HSSFColorPredefined.BLACK.getIndex());
        font.setBold(true);

        CellStyle style = workbook.createCellStyle();
        style.setAlignment(HorizontalAlignment.CENTER);
        style.setVerticalAlignment(VerticalAlignment.CENTER);
        style.setFont(font);

        productLineAndColumnNumberMap.forEach((k,v) -> {
            subGroupLevel2IdAndRowNumberMap.forEach((x, y) -> {
                DGFRateEntry dgfRateEntry = dgfRateEntryRepository.findByDgfSubGroupLevel2IdAndProductLineId(y, k);
                if (dgfRateEntry != null) {
                    String dgfRate = "";
                    if (dgfRateChangeLogRepository.getLatestData(createdOn, dgfRateEntry.getId()) != null) {
                        dgfRate = dgfRateChangeLogRepository.getLatestData(createdOn, dgfRateEntry.getId()).toString().replaceAll("\"", "");
                    }
                    sheet.getRow(x).createCell(v).setCellValue(dgfRate);
                    sheet.getRow(x).getCell(v).setCellStyle(style);
                }
            });

            subGroupLevel3IdAndRowNumberMap.forEach((x, y) -> {
                DGFRateEntry dgfRateEntry = dgfRateEntryRepository.findByDgfSubGroupLevel3IdAndProductLineId(y, k);
                if (dgfRateEntry != null) {
                    String dgfRate = "";
                    if (dgfRateChangeLogRepository.getLatestData(createdOn, dgfRateEntry.getId()) != null) {
                        dgfRate = dgfRateChangeLogRepository.getLatestData(createdOn, dgfRateEntry.getId()).toString().replaceAll("\"", "");
                    }
                    sheet.getRow(x).createCell(v).setCellValue(dgfRate);
                    sheet.getRow(x).getCell(v).setCellStyle(style);
                }
            });
        });
    }

    private static List<DGFRateEntry> excelToTDgfRateEntry(InputStream is) {
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

}