package com.scienaptic.jobs.bean

import java.sql.Date

case class RetailHoliday (Week_End_Date : Date, USLaborDay: Integer, USMemorialDay: Integer, USPresidentsDay: Integer, USThanksgivingDay: Integer, USChristmasDay: Integer )

case class RetailHolidayTranspose (Week_End_Date : Date, USMemorialDay: Integer, USPresidentsDay: Integer, USThanksgivingDay: Integer, holidays: String, holiday_dummy: Integer)
