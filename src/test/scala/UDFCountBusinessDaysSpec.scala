
import java.text.SimpleDateFormat

import com.pythian.udf.{CountBusinessDays, CountSaturdays, CountSundays}
import org.apache.hadoop.io.LongWritable
import com.pythian.udf.DateUtils
import org.scalatest._

class UDFCountBusinessDaysSpec extends FlatSpec with Matchers {
  val simpleDf = new SimpleDateFormat("yyyy/MM/dd")

  "CountBusinessDays" should "execute correctly when end date <= start date" in {
    val startTimestamp = new LongWritable(1389194565)
    val endTimestamp = new LongWritable(1389187740)

    val counter = new CountBusinessDays
    counter.evaluate(startTimestamp, endTimestamp) should be (0)
  }

  "CountBusinessDays" should "return a number of full business days between two dates" in {
    val counter = new CountBusinessDays

    // 1416999600 is 11 am, November 26, 2014
    // 1417280996 is noon , November 29, 2014
    val startTimestamp = new LongWritable(1416999600)
    val endTimestamp = new LongWritable(1417280996)

    counter.evaluate(startTimestamp, endTimestamp) should be (2)
  }

  "CountBusinessDays" should "return 14 days between Jan 01, 2009 and Jan 23, 2009" in {
   // 1230786846 - 2009/01/01
   // 1232732202  - 2009/01/23
    val counter = new CountBusinessDays
    val startTimestamp = new LongWritable(1230786846)
    val endTimestamp = new LongWritable(1232732202)

    counter.evaluate(startTimestamp, endTimestamp) should be (15)
  }

  "CountSaturdays" should "return 3 between Jan 01, 2009 and Jan 23, 2009" in {
    // 1230786846 - 2009/01/01
    // 1232732202  - 2009/01/23
    val counter = new CountSaturdays

    val startTimestamp = new LongWritable(1230786846)
    val endTimestamp = new LongWritable(1232732202)

    counter.evaluate(startTimestamp, endTimestamp) should be (3)
  }

  "CountSundays" should "return 3 between Jan 01, 2009 and Jan 23, 2009" in {
    // 1230786846 - 2009/01/01
    // 1232732202  - 2009/01/23
    val counter = new CountSundays

    val startTimestamp = new LongWritable(1230786846)
    val endTimestamp = new LongWritable(1232732202)

    counter.evaluate(startTimestamp, endTimestamp) should be (3)
  }
  "DateUtil.getCalendar" should "return a java.util.Calendar from unix timestamp" in {

    simpleDf.format(DateUtils.getCalendar(1416999600).getTime) should be ("2014/11/26")
  }

  "DateUtil.datesBetween" should "return a list of dates between startDate and endDate, exclusive" in {
    val startDate = DateUtils.getCalendar(1416999600)
    val endDate = DateUtils.getCalendar(1417280996)
    val dates = DateUtils.datesBetween(startDate, endDate)
    dates.map(d => simpleDf.format(d)) should be (List("2014/11/28", "2014/11/27"))
  }

}

