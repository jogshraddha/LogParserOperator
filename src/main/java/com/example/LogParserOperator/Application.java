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
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    LineByLineFileInputOperator lineByLineFileInputOperator = dag.addOperator("lineReader", new LineByLineFileInputOperator());

    lineByLineFileInputOperator.setDirectory("/tmp/test/common_log1.txt");

    LogParser parser = dag.addOperator("parser", new LogParser());

    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());

    ConsoleOutputOperator errorConsole = dag.addOperator("errorConsole", new ConsoleOutputOperator());

    dag.addStream("log", lineByLineFileInputOperator.output, parser.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    dag.addStream("parser", parser.outputPort, cons.input).setLocality(Locality.CONTAINER_LOCAL);

    dag.addStream("error", parser.errorPort, errorConsole.input).setLocality(Locality.CONTAINER_LOCAL);
  }
}
