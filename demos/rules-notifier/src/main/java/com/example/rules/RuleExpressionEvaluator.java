package com.example.rules;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;

public class RuleExpressionEvaluator extends BaseOperator
{
  public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();
  private transient ExpressionEvaluator ee;
  private String expression;

  private Map<String, Class> parametersNameTypes = new HashMap<>();
  private Map<String, String> expressions = new HashMap<>();

  private MatchRules matchRules;

  public static enum MatchRules
  {
    ALL, ANY
  }

  @Override
  public void setup(OperatorContext context)
  {
    createExpression();
  }

  private void createExpression()
  {
    try {
      ee = new ExpressionEvaluator(expression, // expression
          boolean.class, // expressionType
          getParamNames(), // parameterNames
          getParameterTypes() // parameterTypes
      );
    } catch (CompileException ex) {
      logger.error("uh oh!! ", ex);
      DTThrowable.rethrow(ex);
    }
  }

  private Class[] getParameterTypes()
  {
    Class[] paramTypes = new Class[1];
    parametersNameTypes.values().toArray(paramTypes);
    return paramTypes;
  }

  private String[] getParamNames()
  {
    String[] paramNames = new String[1];
    parametersNameTypes.keySet().toArray(paramNames);
    return paramNames;
  }

  public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      try {
        boolean result = (Boolean)ee.evaluate(new Object[] { tuple });
        if (result) {
          output.emit(tuple);
        }
      } catch (InvocationTargetException ex) {
        logger.error("uh oh! while process:", ex);
      }
    }
  };

  public String getExpression()
  {
    return expression;
  }

  public void setSingleExpression(String expression)
  {
    this.expression = expression;
    createExpression();
  }

  public Map<String, Class> getParametersNameTypes()
  {
    return parametersNameTypes;
  }

  public void setParametersNameTypes(Map<String, Class> parametersNameTypes)
  {
    this.parametersNameTypes = parametersNameTypes;
  }

  public void addParameterNameType(String name, Class type)
  {
    this.parametersNameTypes.put(name, type);
  }

  public Map<String, String> getExpressions()
  {
    return expressions;
  }

  private void setExpression()
  {
    StringBuilder builder = new StringBuilder();
    boolean firstExpression = true;
    for (String expression : this.expressions.values()) {
      if (!firstExpression) {
        builder.append(matchRules == MatchRules.ALL ? "&&" : "||");
      } else {
        firstExpression = false;
      }
      builder.append('(');
      builder.append(expression);
      builder.append(')');
    }

    this.expression = builder.toString();
  }

  public void setExpressions(Map<String, String> expressions)
  {
    this.expressions = expressions;
    setExpression();
    createExpression();
  }

  public void addExpression(String name, String expression)
  {
    this.expressions.put(name, expression);
    setExpression();
    createExpression();
  }

  public void removeExpression(String name)
  {
    this.expressions.remove(name);
    setExpression();
    createExpression();
  }

  public MatchRules getMatchRules()
  {
    return matchRules;
  }

  public void setMatchRules(MatchRules matchRules)
  {
    if (this.matchRules != matchRules) {
      this.matchRules = matchRules;
      setExpression();
      createExpression();
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(RuleExpressionEvaluator.class);
}
