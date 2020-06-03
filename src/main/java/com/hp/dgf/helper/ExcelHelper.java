package com.hp.dgf.helper;

import com.hp.dgf.exception.CustomException;
import com.hp.dgf.model.BusinessCategory;
import com.hp.dgf.model.BusinessSubCategory;
import com.hp.dgf.model.DGFRateEntry;
import com.hp.dgf.repository.BusinessCategoryRepository;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

@Service
public class ExcelHelper {
    @Autowired
    private BusinessCategoryRepository businessCategoryRepository;

    private ExcelHelper() {
    }

    private static final String TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    private static final String SHEET = "DGF Tool";


    public ByteArrayInputStream dgfRateEntryToExcel() {

        final List<BusinessCategory> headerBusinessCategory = businessCategoryRepository.findAll();
        final List<String> businessCategories = new LinkedList<>();
        final List<Set<BusinessSubCategory>> businessSubCategoryList = new LinkedList<>();

        headerBusinessCategory.forEach(businessCategory -> {
            businessCategories.add(businessCategory.getName());
            businessSubCategoryList.add(businessCategory.getChildren());
        });

        try (XSSFWorkbook workbook = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Sheet sheet = workbook.createSheet("DGF Sheet");
            sheet.setColumnWidth(0, 2560);
            sheet.setColumnWidth(1, 2560);
            Row row = sheet.createRow(0);
            row.createCell(0).setCellValue("Hello World");
            workbook.write(out);
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            throw new CustomException("fail to import data to Excel file: " + e.getMessage(), HttpStatus.BAD_REQUEST);
        }
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
}