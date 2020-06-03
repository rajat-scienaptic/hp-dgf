package com.hp.dgf.utils;

import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.Locale;

@Service
public class MonthService {
    final Calendar c = Calendar.getInstance(Locale.US);

    public int getQuarter(){
        int month = c.get(Calendar.MONTH);
        return month <= Calendar.MARCH ? 1 : month <= Calendar.JUNE ? 2 : month <= Calendar.SEPTEMBER ? 3 : 4;
    }

    public String getMonthRange(int quarter){
        String monthRange = "";
        switch (quarter){
            case 1:
                monthRange = "JAN-MAR";
                break;
            case 2:
                monthRange = "APR-JUN";
                break;
            case 3:
                monthRange = "JUL-SEP";
                break;
            case 4:
                monthRange = "OCT-DEC";
                break;
            default:
                monthRange = "JAN-MAR";
        }
        return monthRange;
    }

    public String getQuarterName(int quarter){
        String quarterName = "";
        switch (quarter){
            case 1:
                quarterName = "Q1";
                break;
            case 2:
                quarterName = "Q2";
                break;
            case 3:
                quarterName = "Q3";
                break;
            case 4:
                quarterName = "Q4";
                break;
            default:
                quarterName = "JAN-MAR";
        }
        return quarterName;
    }

    public int getYear(){
        return c.get(Calendar.YEAR);
    }
}
