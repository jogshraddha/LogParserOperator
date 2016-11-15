package defaultlogs.pojo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by synerzip on 10/11/16.
 */
public class CombinedLog implements Log{
    String logFormatExample="172.16.0.3 - frank [25/Sep/2002:14:04:19 +0200] \"GET / HTTP/1.1\" 401 - \"\" \"Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.1) Gecko/20020827\"";
    String pattern="([(\\d\\.)]+) - (.*?) \\[(.*?)\\] \"(.*?)\" (\\d+) - \"(.*?)\" \"(.*?)\"";

    String host;
    String rfc931;
    String userName;
    String datetime;
    String request;
    String statusCode;
    String bytes;
    String referrer;
    String user_agent;
    String cookie;

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

    public void setUserName(String userName) {
        this.userName = userName;
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

    public String getReferrer() {
        return referrer;
    }

    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    public String getUser_agent() {
        return user_agent;
    }

    public void setUser_agent(String user_agent) {
        this.user_agent = user_agent;
    }

    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }

    @Override
    public Log getPojo(String log){
        String pattern="^([0-9.]+) ([w. -]+) (.*?) \\[(.*?)\\] \"((?:[^\"]|\")+)\" (\\d{3}) (\\d+|-) \"((?:[^\"]|\")+)\"(.*?)\"";
        Pattern compile = Pattern.compile(pattern);
        Matcher m = compile.matcher(log);

        if (m.find()){
            this.setHost(m.group(1));
            this.setRfc931(m.group(2));
            this.setUserName(m.group(3));
            this.setDatetime(m.group(4));
            this.setRequest(m.group(5));
            this.setStatusCode(m.group(6));
            this.setBytes(m.group(7));
            this.setReferrer(m.group(8));
            this.setUser_agent(m.group(9));
        }
        return this;
    }

    @Override
    public String toString(){
        return "[ Host : " + this.getHost() +
            " rfc931 : " + this.getRfc931() +
            " userName : " + this.getUserName() +
            " dateTime : " + this.getDatetime() +
            " request : " + this.getRequest() +
            " statusCode : " + this.getStatusCode() +
            " bytes : " + this.getBytes() +
            " referrer : " + this.getReferrer() +
            " user_agent : " + this.getUser_agent() +" ]";
    }
}
