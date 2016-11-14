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
    String username;
    String datetime;
    String request;
    String statuscode;
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

    public String getStatuscode() {
        return statuscode;
    }

    public void setStatuscode(String statuscode) {
        this.statuscode = statuscode;
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
        String pattern="([(\\d\\.)]+) - (.*?) \\[(.*?)\\] \"(.*?)\" (\\d+) - \"(.*?)\" \"(.*?)\"";
        Pattern compile = Pattern.compile(pattern);
        Matcher m = compile.matcher(log);

        if (m.find()){
            this.setHost(m.group(1));
            this.setUsername(m.group(2));
            this.setDatetime(m.group(3));
            this.setRequest(m.group(4));
            this.setStatuscode(m.group(5));
            this.setReferrer(m.group(7));
        }
        return this;
    }


}
