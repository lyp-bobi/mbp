package com.mbp.Feature
import java.text.SimpleDateFormat
class timeDivision(var startTime:Long = 0,
        var endTime:Long = 0,
        var period:Long = 3600000) {
  def getPeriod(time:Long):Tuple2[Long,Long]={
    val n=math.floor((time-startTime)*1.0/period).toInt
    return (startTime+n*period, startTime+(n+1)*period)
  }
}
object timeDivision{
  def parseTime(s:String): Long ={
    val fm=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    fm.parse(s).getTime
  }
}
