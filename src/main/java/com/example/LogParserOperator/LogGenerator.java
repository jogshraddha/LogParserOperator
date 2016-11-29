package com.example.LogParserOperator;


import javax.validation.constraints.Min;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;

/**
 * Created by synerzip on 18/11/16.
 */
public class LogGenerator extends BaseOperator implements InputOperator
{
  private static final Logger logger = LoggerFactory.getLogger(LogGenerator.class);

  @Min(1)
  private int numTuples = 20;
  private transient int count = 0;
  public static Random rand = new Random();

  public final transient DefaultOutputPort<byte[]> out = new DefaultOutputPort<byte[]>();

  public static String[] host = {"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4"};
  public static String rfc931 = "-";
  public static String[] username = {"John", "Peter", "Ron", "Kelvin"};
  public static String[] datetime = {"10/Oct/1999:21:15:05 +0500", "22/Jan/2013:11:11:11 +0500", "04/Oct/2015:13:04:05 +0500", "31/Dec/2011:12:14:15 +0500"};
  public static String request = "GET /index.html HTTP/1.0";
  public static String[] statusCode = {"200", "400", "500", "404"};
  public static String[] bytes = {"2326", "4050", "4336", "5050"};

  private int sleepTime;

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  private static String getNext(int num)
  {

    StringBuilder logString  = new StringBuilder();
    try {
      //Extended log
      logString.append("2014-06-03 05:14:00 10.0.1.3 127.0.0.3 80 200 304 0 344 433 http://abc.prw.com");

      //common log
//      logString.append(host[num % host.length]).append(" ");
//      logString.append(rfc931).append(" ");
//      logString.append(username[num % username.length]).append(" ");
//      logString.append("[" + datetime[num % datetime.length] + "]").append(" ");
//      logString.append("\"" + request + "\"").append(" ");
//      logString.append(statusCode[num % statusCode.length]).append(" ");
//      logString.append(bytes[num % bytes.length]);
    } catch (Exception e) {
      return null;
    }
    logger.info("Generated String {}", logString.toString());
    return logString.toString();
  }

  @Override
  public void emitTuples()
  {
    if (count++ < numTuples) {
//      out.emit(getNext().getBytes());
      out.emit(getNext(rand.nextInt(numTuples) + 1).getBytes());
    } else {
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        logger.info("Sleep interrupted");
      }
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }
}
