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

public class LogParser extends BaseOperator
{

    private transient Class<?> clazz;

    private String logFileFormat;

    long errorTupleCount;

    long parsedObjectCount;

    private LogSchemaDetails logSchemaDetails;

    Log log;

    public final transient DefaultOutputPort<Object> errorPort = new DefaultOutputPort<>();

    public final transient DefaultOutputPort<Object> output = new DefaultOutputPort()
    {
        @Override
        public void setup(Context.PortContext context) {
            clazz = context.getAttributes().get(Context.PortContext.TUPLE_CLASS);
        }
    };

    public void beginWindow(long windowId)
    {
        this.errorTupleCount = 0L;
        this.parsedObjectCount = 0L;
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
        //define logFileFormat and pojo class
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

    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
        @Override
        public void process(String bite) {
            try {
                if(logSchemaDetails != null) {
                    logger.info("Parsing with CUSTOM log format has been started");
                    String pattern = createPattern();
                    ObjectMapper objMapper = new ObjectMapper();
                    output.emit(objMapper.readValue(createJsonFromLog(bite, pattern).toString().getBytes(), clazz));
                    parsedObjectCount++;
                } else {
                    logger.info("Parsing with DEFAULT log format " + logFileFormat);
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

    /**
     * Combines the given regex and forms a pattern string for parsing the logs
     * @return pattern
     */
    public String createPattern()
    {
        String pattern = "";
        for(LogSchemaDetails.Field field: logSchemaDetails.getFields()) {
            pattern = pattern + field.getRegex() + " ";
        }
        return pattern.trim();
    }

    /**
     * Creates json object by parsing the log with custom logFileFormat
     * @param log
     * @param pattern
     * @return logObject
     * @throws Exception
     */
    public JSONObject createJsonFromLog(String log, String pattern) throws Exception
    {
        Pattern compile = Pattern.compile(pattern);
        Matcher m = compile.matcher(log);
        int count = m.groupCount();
        int i = 1;
        JSONObject logObject = new JSONObject();
        if(m.find()) {
            for(String field: logSchemaDetails.getFieldNames()) {
                if(i == count) {
                    break;
                }
                logObject.put(field, m.group(i));
                i++;
            }
        } else {
            throw new Exception("No match found for log : " + log);
        }
        return logObject;
    }

    public void setLogFileFormat(String logFileFormat)
    {
        this.logFileFormat = logFileFormat;

    }

    public String geLogFileFormat()
    {
        return logFileFormat;
    }

    public Class<?> getClazz()
    {
        return this.clazz;
    }

    public void setClazz(Class<?> clazz)
    {
        this.clazz = clazz;
    }

    private static final Logger logger = LoggerFactory.getLogger(LogParser.class);
}

