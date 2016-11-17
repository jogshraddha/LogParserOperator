/**
 * Put your copyright and license info here.
 */
package com.example.LogParserOperator;

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="LogParser")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    LineByLineFileInputOperator lineByLineFileInputOperator = dag.addOperator("lineReader", new LineByLineFileInputOperator());

    lineByLineFileInputOperator.setDirectory("/tmp/test/common_log1.txt");

    LogParser parser = dag.addOperator("parser", new LogParser());

    ConsoleOutputOperator cons = dag.addOperator("parsedLog", new ConsoleOutputOperator());

    ConsoleOutputOperator error = dag.addOperator("Error", ConsoleOutputOperator.class);

    dag.addStream("log", lineByLineFileInputOperator.output, parser.input);

    dag.addStream("parser", parser.output, cons.input);

    dag.addStream("error", parser.errorPort, error.input);
  }
}
