package com.example.LogParserOperator.schema;

/**
 * Created by synerzip on 16/11/16.
 */
public class LogSchema {
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
