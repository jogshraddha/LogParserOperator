package defaultlogs.pojo;

import sun.security.pkcs.ParsingException;

import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by synerzip on 10/11/16.
 */
public class CommonLog implements Log {

    private String host;
    private String rfc931;
    private String username;
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

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
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
    public Log getPojo(String log) throws Exception {
        String pattern = "^([0-9.]+) ([w. -]+) (.*?) \\[(.*?)\\] \"((?:[^\"]|\")+)\" (\\d{3}) (\\d+|-)";
        Pattern compile = Pattern.compile(pattern);
        Matcher m = compile.matcher(log);

        if (m.find()) {
            this.setHost(m.group(1));
            this.setRfc931(m.group(2));
            this.setUsername(m.group(3));
            this.setDatetime(m.group(4));
            this.setRequest(m.group(5));
            this.setStatusCode(m.group(6));
            this.setBytes(m.group(7));
        } else {
            throw new Exception("No match found");
        }
        return this;
    }

    @Override
    public String toString() {
        return "CommonLog [ Host : " + this.getHost() +
            ", rfc931 : " + this.getRfc931() +
            ", userName : " + this.getUsername() +
            ", dateTime : " + this.getDatetime() +
            ", request : " + this.getRequest() +
            ", statusCode : " + this.getStatusCode() +
            ", bytes : " + this.getBytes() + " ]";
    }
}


