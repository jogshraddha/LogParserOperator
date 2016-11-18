/**
 * Put your copyright and license info here.
 */
package com.example.LogParserOperator;

import org.apache.hadoop.conf.Configuration;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="LogParser")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    LogGenerator generator = dag.addOperator("generator", new LogGenerator());

    LogParser parser = dag.addOperator("parser", new LogParser());

    ConsoleOutputOperator cons = dag.addOperator("parsedLog", new ConsoleOutputOperator());

    ConsoleOutputOperator error = dag.addOperator("Error", ConsoleOutputOperator.class);

    dag.addStream("log", generator.out, parser.in);

    dag.addStream("parser", parser.parsedOutput, cons.input);

    dag.addStream("error", parser.err, error.input);
  }
}
