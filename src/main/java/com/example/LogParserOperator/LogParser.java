package com.example.LogParserOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.fasterxml.jackson.databind.ObjectMapper;
import defaultlogs.pojo.DefaultLogs;
import defaultlogs.pojo.Log;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by synerzip on 14/11/16.
 */
public class LogParser extends BaseOperator {

    private static final Logger logger = LoggerFactory.getLogger(LogParser.class);

    private transient Class<?> clazz;

    private String logFileFormat;

    long errorTupleCount;

    long parsedObjectCount;

    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort() {
        @Override
        public void setup(Context.PortContext context) {
            LogParser.this.setClazz(context.getAttributes().get(Context.PortContext.TUPLE_CLASS));
        }
    };

    public final transient DefaultOutputPort<Object> errorPort = new DefaultOutputPort<>();

    private LogSchemaDetails logSchemaDetails;

    Log log;

    public void beginWindow(long windowId) {
        this.errorTupleCount = 0L;
        this.parsedObjectCount = 0L;
    }

    @Override
    public void setup(Context.OperatorContext context) {
        //define log regex and pojo class
        logger.info("Received logFileFormat as : " + logFileFormat);
        if(DefaultLogs.logTypes.containsKey(logFileFormat)) {
            logger.info("Parsing logs from default log formats");
            log = DefaultLogs.logTypes.get(logFileFormat);
        } else {
            logger.info("Parsing logs from custom log formats");
            try {
                //parse the schema in logFileFormat string
                logSchemaDetails = new LogSchemaDetails(logFileFormat);
            } catch (Exception e) {
                logger.error("Error while initializing the custom format " + e.getMessage());
            }
        }
    }
    // Now create matcher object.

    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
        @Override
        public void process(String bite) {
            try {
                if(logSchemaDetails != null) {
                    logger.info("Parsing with CUSTOM log format has been started");
                    String pattern = createPattern();
                    ObjectMapper objMapper = new ObjectMapper();
                    output.emit(objMapper.readValue(createJsonFromLog(bite, pattern).toString().getBytes(), LogParser.this.getClazz()));
                    parsedObjectCount++;
                } else {
                    logger.info("Parsing with DEFAULT log format has been started");
                    Log parsedLog = log.getPojo(bite);
                    if(log != null) {
                        output.emit(parsedLog.toString());
                        parsedObjectCount++;
                    } else {
                        throw new NullPointerException("Could not parse the log");
                    }
                }
            } catch (Exception e) {
                logger.error("Error while parsing the logs " + e.getMessage());
                errorPort.emit(e.getMessage());
                errorTupleCount++;
            }
        }
    };

    public String createPattern() {
        String pattern = "";
        for(LogSchemaDetails.Field field: logSchemaDetails.getFields()){
            pattern = pattern + field.getRegex() + " ";
        }
        return pattern.trim();
    }

    public JSONObject createJsonFromLog(String log, String pattern) throws Exception {
        Pattern compile = Pattern.compile(pattern);
        Matcher m = compile.matcher(log);
        int count = m.groupCount();
        int i = 1;
        JSONObject jsonObject = new JSONObject();
        if (m.find()) {
            for(String field: logSchemaDetails.getFieldNames()){
                if(i == count){
                    break;
                }
                jsonObject.put(field, m.group(i));
                i++;
            }
        } else {
            throw new Exception("No match found");
        }
        return jsonObject;
    }

    public void setLogFileFormat(String logFileFormat) {
        this.logFileFormat = logFileFormat;

    }

    public String geLogFileFormat() {
        return logFileFormat;
    }

    public Class<?> getClazz() {
        return this.clazz;
    }

    public void setClazz(Class<?> clazz) {
        this.clazz = clazz;
    }
}

