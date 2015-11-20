package com.example.rules;

import com.example.rules.RuleExpressionEvaluator.MatchRules;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;

@ApplicationAnnotation(name = "Rule Notifier for JSON application")
public class EvalJsonApplication extends Application
{
  @Override
  public RuleExpressionEvaluator<String> addExpressionEvaluator(DAG dag, KafkaSinglePortStringInputOperator randomGenerator)
  { 
    RuleExpressionEvaluator<String> eval = dag.addOperator("rule", new RuleExpressionEvaluator<String>());
    eval.setTupleType(PojoObject.class);
    eval.addParameterNameType("input", PojoObject.class);
    eval.addExpression("by10", "input.obj.counter% 100==0");
    eval.addExpression("by5", "input.obj.counter% 50==0");
    eval.setMatchRules(MatchRules.ANY);
    
//    eval.addParameterNameType("input", JSONObject.class);
//    eval.addExpression("jsonCheck", "input.getJSONObject(\"obj\").getInt(\"counter\") %1000 ==0");

    dag.addStream("randomData", randomGenerator.outputPort, eval.inputJson).setLocality(Locality.CONTAINER_LOCAL);
    return eval;
  }
}
