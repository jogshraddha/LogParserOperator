package com.example.LogParserOperator;

import com.datatorrent.contrib.parser.DelimitedSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by synerzip on 11/11/16.
 */
public class LogSchemaDetails {

    private static final String FIELDS = "fields";

    private static final String NAME = "name";
    private static final String REGEX = "regex";

    private List<String> fieldNames = new LinkedList<String>();
    private List<DelimitedSchema.Field> fields = new LinkedList<DelimitedSchema.Field>();

    public LogSchemaDetails(String json) {

        initialize(json);

    }

    private void initialize(String json) {

    }

    public class Field
    {

        String name;
        String regex;


        public Field(String name, String type)
        {
            this.name = name;
            this.regex = String.valueOf(type.toUpperCase());
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

    private static final Logger logger = LoggerFactory.getLogger(LogSchemaDetails.class);


}
