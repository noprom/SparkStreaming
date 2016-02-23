package com.huntdreams.streaming.ch4

import java.util.regex.Pattern
import java.util.regex.Matcher

/**
 * ScalaLogAnalyzer
 *
 * @author tyee.noprom@qq.com
 * @time 2/23/16 11:27 AM.
 */
class ScalaLogAnalyzer extends Serializable {

  /**
   * Transform the Apache log files and convert them into a Map of
   * Key/Value pair
   *
   * @param logLine logLine
   * @return
   */
  def transformLogData(logLine: String): Map[String, String] = {
    //Pattern which will extract the relevant data from Apache Access Log Files
    val LOG_ENTRY_PATTERN =
      """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)"""
    val PATTERN = Pattern.compile(LOG_ENTRY_PATTERN)
    val matcher = PATTERN.matcher(logLine)
    //Matching the pattern for the each line of the Apache access Log file
    if (!matcher.find()) {
      System.out.println("Cannot parse logline" + logLine)
    }
    //Finally create a Key/Value pair of extracted data and return to calling program
    createDataMap(matcher);
  }

  /**
   * Create a scala map
   *
   * @param m matcher
   * @return
   */
  def createDataMap(m: Matcher): Map[String, String] = {
    return Map[String, String](
         ("IP" -> m.group(1)),
         ("client" -> m.group(2)),
         ("user" -> m.group(3)),
         ("date" -> m.group(4)),
         ("method" -> m.group(5)),
         ("request" -> m.group(6)),
         ("protocol" -> m.group(7)),
         ("respCode" -> m.group(8)),
         ("size" -> m.group(9)))
  }
}
