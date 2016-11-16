package com.example.LogParserOperator;

import com.datatorrent.contrib.parser.DelimitedSchema;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by synerzip on 11/11/16.
 */
public class LogSchemaDetails {

    private static final Logger logger = LoggerFactory.getLogger(LogSchemaDetails.class);

    private List<String> fieldNames = new LinkedList();

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<Field> getFields() {
        return fields;
    }

    private List<Field> fields = new LinkedList();

    public LogSchemaDetails(String json) throws JSONException, IOException{
        logger.info("initializing lohSchema...");
        initialize(json);

    }

    private void initialize(String json) throws JSONException, IOException {
        JSONObject jo = new JSONObject(json);
        JSONArray fieldArray = jo.getJSONArray("fields");

        for(int i = 0; i < fieldArray.length(); ++i) {
            JSONObject obj = fieldArray.getJSONObject(i);
            Field field = new Field(obj.getString("field"), obj.getString("regex"));
            this.fields.add(field);
            this.fieldNames.add(field.name);
        }
    }

    public class Field
    {

        String name;
        String regex;


        public Field(String name, String regex)
        {
            this.name = name;
            this.regex = regex;
        }

        public String getName()
        {
            return name;
        }

        public void setName(String name)
        {
            this.name = name;
        }

        public String getRegex()
        {
            return regex;
        }

        public void setRegex(String regex)
        {
            this.regex = regex;
        }

        @Override
        public String toString()
        {
            return "Fields [name=" + name + ", regex=" + regex +"]";
        }
    }
}
