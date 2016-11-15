package com.example.LogParserOperator;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import defaultlogs.pojo.DefaultLogs;
import defaultlogs.pojo.Log;

/**
 * Created by synerzip on 14/11/16.
 */
public class LogParser extends BaseOperator {



    private String logFileFormat;

    final transient DefaultOutputPort<Object> outputPort= new DefaultOutputPort<>();

    Log log;

    @Override
    public void setup(Context.OperatorContext context) {
        //define log regex and pojo class

        if(DefaultLogs.logTypes.containsKey(logFileFormat)){
            log = DefaultLogs.logTypes.get(logFileFormat);
        }else{
            //parse the schema in logformat string
            LogSchemaDetails logSchemaDetails = new LogSchemaDetails(logFileFormat);

        }

        //get json schema and set the fields and regex
    }
    // Now create matcher object.

    final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>(){

        @Override
        public void process(String bite) {
            try {
                Log parsedLog = log.getPojo(bite);
                outputPort.emit(parsedLog.toString());
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    };

    private String createJsonFromLog(String tuple) {
        //get the regex of log then create json for that log.


        //we need log regex, schema
        //if it is default the use existing log schema,

        return null;
    }


    public void setLogFileFormat(String logFileFormat) {
        this.logFileFormat = logFileFormat;
    }
    public String geLogFileFormat() {
        return logFileFormat;
    }
}

