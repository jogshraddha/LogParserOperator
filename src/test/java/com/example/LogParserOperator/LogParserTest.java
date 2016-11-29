package com.example.LogParserOperator;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.codehaus.jettison.json.JSONException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.testbench.CollectorTestSink;
import defaultlogs.pojo.CombinedLog;
import defaultlogs.pojo.CommonLog;
import defaultlogs.pojo.ExtendedLog;

public class LogParserTest
{
  private static final String filename = "logSchema.json";

  LogParser logParser = new LogParser();

  private CollectorTestSink<Object> error = new CollectorTestSink<Object>();

  private CollectorTestSink<Object> pojoPort = new CollectorTestSink<Object>();

  @Rule
  public Watcher watcher = new Watcher();

  public class Watcher extends TestWatcher
  {
    @Override
    protected void starting(Description description)
    {
      super.starting(description);
      logParser.err.setSink(error);
      logParser.parsedOutput.setSink(pojoPort);
      logParser.setLogFileFormat("common");
      logParser.setup(null);
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      error.clear();
      pojoPort.clear();
      pojoPort.clear();
      logParser.teardown();
    }
  }

  @Test
  public void TestValidCommonLogInputCase() throws JSONException
  {
    logParser.setLogFileFormat("common");
    logParser.setupLog();
    logParser.beginWindow(0);
    String log = "125.125.125.125 - dsmith [10/Oct/1999:21:15:05 +0500] \"GET /index.html HTTP/1.0\" 200 1043";
    logParser.in.process(log.getBytes());
    logParser.endWindow();
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(CommonLog.class, obj.getClass());
    CommonLog pojo = (CommonLog)obj;
    Assert.assertNotNull(obj);
    Assert.assertEquals("125.125.125.125", pojo.getHost());
    Assert.assertEquals("dsmith", pojo.getUsername());
    Assert.assertEquals("10/Oct/1999:21:15:05 +0500", pojo.getDatetime());
    Assert.assertEquals("GET /index.html HTTP/1.0", pojo.getRequest());
    Assert.assertEquals("200", pojo.getStatusCode());
    Assert.assertEquals("1043", pojo.getBytes());
  }

  @Test
  public void TestInvalidCommonLogInput()
  {
    logParser.setLogFileFormat("common");
    logParser.setupLog();
    String tuple = "127.0.0.1 - dsmith 10/Oct/1999:21:15:05] \"GET /index.html HTTP/1.0\" 200 1043";
    logParser.beginWindow(0);
    logParser.in.process(tuple.getBytes());
    logParser.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestValidCombinedLogInputCase() throws JSONException
  {
    logParser.setLogFileFormat("combined");
    logParser.setupLog();
    logParser.beginWindow(0);
    String log = "125.125.125.125 - dsmith [10/Oct/1999:21:15:05 +0500] \"GET /index.html HTTP/1.0\" 200 1043 \"http://www.ibm.com/\" \"Mozilla/4.05 [en] (WinNT; I)\" \"USERID=CustomerA;IMPID=01234\"";
    logParser.in.process(log.getBytes());
    logParser.endWindow();
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(CombinedLog.class, obj.getClass());
    CombinedLog pojo = (CombinedLog)obj;
    Assert.assertNotNull(obj);
    Assert.assertEquals("125.125.125.125", pojo.getHost());
    Assert.assertEquals("dsmith", pojo.getUserName());
    Assert.assertEquals("10/Oct/1999:21:15:05 +0500", pojo.getDatetime());
    Assert.assertEquals("GET /index.html HTTP/1.0", pojo.getRequest());
    Assert.assertEquals("200", pojo.getStatusCode());
    Assert.assertEquals("1043", pojo.getBytes());
    Assert.assertEquals("http://www.ibm.com/", pojo.getReferrer());
    Assert.assertEquals("Mozilla/4.05 [en] (WinNT; I)", pojo.getUser_agent());
    Assert.assertEquals("USERID=CustomerA;IMPID=01234", pojo.getCookie());
  }

  @Test
  public void TestInvalidCombinedLogInput()
  {
    logParser.setLogFileFormat("combined");
    logParser.setupLog();
    String tuple = "127.0.0.1 - dsmith 10/Oct/1999:21:15:05] GET /index.html HTTP/1.0\" 200 1043 \"http://www.ibm.com/\" \"Mozilla/4.05 [en] (WinNT; I)\" \"USERID=CustomerA;IMPID=01234\"";
    logParser.beginWindow(0);
    logParser.in.process(tuple.getBytes());
    logParser.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestValidExtendedLogInputCase() throws JSONException
  {
    logParser.setLogFileFormat("extended");
    logParser.setExtendedFieldsSeq("date time c-ip s-ip s-port sc-status sc-substatus sc-win32-status sc-bytes cs-bytes cs(Referrer)");
    logParser.setupLog();
    logParser.beginWindow(0);
    String log = "2014-06-03 05:14:00 10.0.1.3 127.0.0.3 80 200 304 0 344 433 https://abb.pqr.com";
    logParser.in.process(log.getBytes());
    logParser.endWindow();
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    Assert.assertEquals(ExtendedLog.class, obj.getClass());
    ExtendedLog pojo = (ExtendedLog)obj;
    Assert.assertNotNull(obj);
    Assert.assertEquals("2014-06-03", pojo.getDate());
    Assert.assertEquals("05:14:00", pojo.getTime());
    Assert.assertEquals("10.0.1.3", pojo.getClientIP());
    Assert.assertEquals("127.0.0.3", pojo.getServerIP());
    Assert.assertEquals("200", pojo.getStatus());
    Assert.assertEquals("304", pojo.getSubStatus());
    Assert.assertEquals("0", pojo.getWin32Status());
    Assert.assertEquals("344", pojo.getBytesSent());
    Assert.assertEquals("433", pojo.getBytesReceived());
    Assert.assertEquals("https://abb.pqr.com", pojo.getReferrer());
  }


  @Test
  public void TestEmptyInput()
  {
    String tuple = "";
    logParser.beginWindow(0);
    logParser.in.process(tuple.getBytes());
    logParser.endWindow();
    Assert.assertEquals(0, pojoPort.collectedTuples.size());
    Assert.assertEquals(1, error.collectedTuples.size());
  }

  @Test
  public void TestSchemaInput() throws JSONException, java.io.IOException
  {
    logParser.setClazz(LogSchema.class);
    logParser.setLogFileFormat(SchemaUtils.jarResourceFileToString(filename));
    logParser.setLogSchemaDetails(new LogSchemaDetails(logParser.geLogFileFormat()));
    String log = "125.125.125.125 - dsmith [10/Oct/1999:21:15:05 +0500] \"GET /index.html HTTP/1.0\" 200 1043";
    logParser.beginWindow(0);
    logParser.in.process(log.getBytes());
    logParser.endWindow();
    Assert.assertEquals(1, pojoPort.collectedTuples.size());
    Assert.assertEquals(0, error.collectedTuples.size());
    Object obj = pojoPort.collectedTuples.get(0);
    Assert.assertNotNull(obj);
    LogSchema pojo = (LogSchema) obj;
    Assert.assertEquals("125.125.125.125", pojo.getHost());
    Assert.assertEquals("dsmith", pojo.getUserName());
    Assert.assertEquals("10/Oct/1999:21:15:05 +0500", pojo.getDatetime());
    Assert.assertEquals("GET /index.html HTTP/1.0", pojo.getRequest());
    Assert.assertEquals("200", pojo.getStatusCode());
    //Assert.assertEquals("1043", pojo.getBytes());
  }

  public static class LogSchema {
    private String host;
    private String rfc931;
    private String userName;
    private String datetime;
    private String request;
    private String statusCode;
    private String bytes;

    public String getHost() {
      return host;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public String getRfc931() {
      return rfc931;
    }

    public void setRfc931(String rfc931) {
      this.rfc931 = rfc931;
    }

    public String getUserName() {
      return userName;
    }

    public void setUserName(String username) {
      this.userName = username;
    }

    public String getDatetime() {
      return datetime;
    }

    public void setDatetime(String datetime) {
      this.datetime = datetime;
    }

    public String getRequest() {
      return request;
    }

    public void setRequest(String request) {
      this.request = request;
    }

    public String getStatusCode() {
      return statusCode;
    }

    public void setStatusCode(String statusCode) {
      this.statusCode = statusCode;
    }

    public String getBytes() {
      return bytes;
    }

    public void setBytes(String bytes) {
      this.bytes = bytes;
    }

    @Override
    public String toString()
    {
      return "LogSchema [host=" + host + ", rfc931=" + rfc931 + ", userName=" + userName
        + ", datetime=" + datetime + ", request=" + request + ", statusCode=" + statusCode + ", bytes=" + bytes + "]";
    }
  }
}

