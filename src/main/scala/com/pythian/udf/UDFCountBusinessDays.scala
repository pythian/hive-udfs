/* Collection of Hive UDFs to calculate the number of weekdays, Saturdays and Sundays
   in a given time interval
*/
package com.pythian.udf

import java.util.{GregorianCalendar, Calendar, Date}

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.IntWritable

object DateUtils {
  // Constant Sets for different types of days
  val BusinessDays = Set(Calendar.MONDAY, Calendar.TUESDAY, Calendar.WEDNESDAY,
    Calendar.THURSDAY, Calendar.FRIDAY)
  val Saturday = Set(Calendar.SATURDAY)
  val Sunday = Set(Calendar.SUNDAY)

  // convert a Unix time into Java Calendar
  def getCalendar(timestamp: Long): Calendar = {
    val calendarDate = Calendar.getInstance()
    calendarDate.setTimeInMillis(timestamp * 1000)
    calendarDate
  }

  def sameDay(a: Calendar, b: Calendar): Boolean =
    if (a.get(Calendar.YEAR) == b.get(Calendar.YEAR) &&
        a.get(Calendar.MONTH) == b.get(Calendar.MONTH) &&
        a.get(Calendar.DAY_OF_MONTH) == b.get(Calendar.DAY_OF_MONTH)) true
    else false

  /* Recursive function that build a list of dates between two given dates. Both endpoints are excluded*/
  def datesBetween(startDate: Calendar, endDate: Calendar): List[Date] = {

    /* this helper function is required to keep calls tail-recursive
       and avoid additional list argument in datesBetween
     */
    def datesBetweenRecursive(sDate: Calendar, eDate: Calendar, list: List[Date]): List[Date] = {
      sDate.add(Calendar.DAY_OF_MONTH, 1)
      if (sameDay(sDate, eDate)) list
      else datesBetweenRecursive(sDate, eDate, sDate.getTime :: list)
    }

    datesBetweenRecursive(startDate, endDate, List())
  }

  /*def datesBetween(startDate: Calendar, endDate:Calendar): List[Date] = {
    var datesList: List[Date] = Nil
    // don't include start and end dates
    startDate.add(Calendar.DAY_OF_MONTH, 1)
    endDate.add(Calendar.DAY_OF_MONTH, -1)
    while (startDate.before(endDate)) {
      datesList =  startDate.getTime :: datesList
      startDate.add(Calendar.DAY_OF_MONTH, 1)
    }
    datesList
  }*/

  // check if a given day belongs to a given days set
  def isDay(date: Date, daysOfWeek: Set[Int]): Boolean = {
    val cal = new GregorianCalendar()
    cal.setTime(date)
    daysOfWeek.contains(cal.get(Calendar.DAY_OF_WEEK))
  }

  def isBusinessDay(date: Date): Boolean = isDay(date, BusinessDays)

  def isSaturday(date: Date): Boolean = isDay(date, Saturday)

  def isSunday(date: Date): Boolean = isDay(date, Sunday)

  /* This function counts actual number of given days types in an interval.
     Interval is provided as Unix time and is of IntWritable type.
     counter is a function that takes Date as input and returns True or False
   */
  def countDays(startTimestamp: IntWritable, endTimestamp: IntWritable, counter: Date => Boolean): Int = {
    if (startTimestamp == null || endTimestamp == null) return 0
    val startDate = DateUtils.getCalendar(startTimestamp.get())
    val endDate = DateUtils.getCalendar(endTimestamp.get())

    DateUtils.datesBetween(startDate, endDate).count(d => counter(d))
  }

}

/* Actual UDF class definitions. Notice that the only difference is the counter function we pass to countDays */
class CountBusinessDays extends UDF {
  def evaluate(startTimestamp: IntWritable, endTimestamp: IntWritable): Int = {
    DateUtils.countDays(startTimestamp, endTimestamp, DateUtils.isBusinessDay)
  }
}

class CountSaturdays extends UDF {
  def evaluate(startTimestamp: IntWritable, endTimestamp: IntWritable): Int = {
    DateUtils.countDays(startTimestamp, endTimestamp, DateUtils.isSaturday)
  }
}

class CountSundays extends UDF {
  def evaluate(startTimestamp: IntWritable, endTimestamp: IntWritable): Int = {
    DateUtils.countDays(startTimestamp, endTimestamp, DateUtils.isSunday)
  }
}
