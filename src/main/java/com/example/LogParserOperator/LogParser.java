package com.example.LogParserOperator;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.parser.Parser;
import com.datatorrent.lib.util.KeyValPair;
import com.fasterxml.jackson.databind.ObjectMapper;
import defaultlogs.pojo.DefaultLogs;
import defaultlogs.pojo.Log;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser extends Parser<byte[], KeyValPair<String, String>>
{

    private String logFileFormat;

    private LogSchemaDetails logSchemaDetails;

    Log log;

    private transient ObjectMapper objMapper;

    @Override
    public Object convert(byte[] tuple)
    {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public KeyValPair<String, String> processErrorTuple(byte[] bytes)
    {
        return null;
    }

    /**
     * output port to emit validate records as JSONObject
     */
    public transient DefaultOutputPort<Object> parsedOutput = new DefaultOutputPort<Object>();

    /**
     * metric to keep count of number of tuples emitted on {@link #parsedOutput}
     * port
     */
    @AutoMetric
    long parsedOutputCount;

    @Override
    public void beginWindow(long windowId)
    {
        super.beginWindow(windowId);
        parsedOutputCount = 0;
    }

    @Override
    public void setup(Context.OperatorContext context)
    {
        objMapper = new ObjectMapper();
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

    @Override
    public void processTuple(byte[] inputTuple)
    {
        if (inputTuple == null) {
            if (err.isConnected()) {
                err.emit(new KeyValPair<String, String>(null, "null tuple"));
            }
            errorTupleCount++;
            return;
        }

        String incomingString = new String(inputTuple);
        logger.info("Input string {} ", incomingString);

        try {
            if(logSchemaDetails != null) {
                logger.info("Parsing with CUSTOM log format has been started");
                String pattern = createPattern();
                if (parsedOutput.isConnected()) {
                    parsedOutput.emit(objMapper.readValue(createJsonFromLog(incomingString, pattern).toString().getBytes(), clazz));
                    parsedOutputCount++;
                }
            } else {
                logger.info("Parsing with DEFAULT log format " + logFileFormat);
                Log parsedLog = log.getPojo(incomingString);
                if(parsedLog != null) {
                    logger.info("Emitting parsed object ");
                    parsedOutput.emit(parsedLog.toString());
                    parsedOutputCount++;
                } else {
                    throw new NullPointerException("Could not parse the log");
                }
            }
        } catch (Exception e) {
            logger.error("Error while parsing the logs " + e.getMessage());
            errorTupleCount++;
            if (err.isConnected()) {
                err.emit(new KeyValPair<String, String>(incomingString, e.getMessage()));
            }
        }
    }

    public String createPattern()
    {
        String pattern = "";
        for(LogSchemaDetails.Field field: logSchemaDetails.getFields()) {
            pattern = pattern + field.getRegex() + " ";
        }
        return pattern.trim();
    }

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

    private static final Logger logger = LoggerFactory.getLogger(LogParser.class);
}

