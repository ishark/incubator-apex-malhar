package com.example.rules;

import org.codehaus.jettison.json.JSONObject;

import com.example.rules.RuleExpressionEvaluator.MatchRules;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;

public class EvalJsonApplication extends Application
{
  @Override
  public RuleExpressionEvaluator<PojoObject> addExpressionEvaluator(DAG dag, AscendingNumbers randomGenerator)
  { 
    RuleExpressionEvaluator<PojoObject> eval = dag.addOperator("rule", new RuleExpressionEvaluator<PojoObject>());
    eval.addParameterNameType("input", JSONObject.class);
    eval.addExpression("jsonCheck", "input.getJSONObject(\"obj\").getInt(\"counter\") %10 ==0");
    eval.setMatchRules(MatchRules.ANY);
    dag.addStream("randomData", randomGenerator.outputJson, eval.inputJson).setLocality(Locality.CONTAINER_LOCAL);
    return eval;
  }

}
