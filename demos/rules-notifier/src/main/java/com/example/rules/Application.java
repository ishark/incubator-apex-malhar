/**
 * Put your copyright and license info here.
 */
package com.example.rules;

import org.apache.hadoop.conf.Configuration;

import com.example.rules.RuleExpressionEvaluator.MatchRules;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "Rule Notifier application")
public class Application implements StreamingApplication
{
  public static class AscendingNumbers implements InputOperator
  {

    @Override
    public void beginWindow(long arg0)
    {

    }

    @Override
    public void endWindow()
    {

    }

    @Override
    public void setup(OperatorContext arg0)
    {

    }

    @Override
    public void teardown()
    {

    }

    @Override
    public void emitTuples()
    {
      output.emit(count++);
    }

    private int count = 0;
    public transient final DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();

  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Sample DAG with 2 operators
    // Replace this code with the DAG you want to build

    AscendingNumbers randomGenerator = dag.addOperator("randomGenerator", AscendingNumbers.class);

    RuleExpressionEvaluator eval = dag.addOperator("rule", new RuleExpressionEvaluator());

    eval.addParameterNameType("input", Integer.class);

    eval.addExpression("by10", "input% 10==0");
    eval.addExpression("by5", "input% 5==0");
    eval.setMatchRules(MatchRules.ANY);
    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("randomData", randomGenerator.output, eval.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("stream", eval.output, cons.input).setLocality(Locality.CONTAINER_LOCAL);
  }
}
