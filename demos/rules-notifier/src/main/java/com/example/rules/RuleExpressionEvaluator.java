package com.example.rules;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.DTThrowable;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class RuleExpressionEvaluator<T> extends BaseOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  private transient ExpressionEvaluator ee;
  private String expression;

  private Map<String, Class> parametersNameTypes = new HashMap<>();
  private Map<String, String> expressions = new HashMap<>();

  private String addRule;
  private String removeRule;

  private Class tupleType;

  private MatchRules matchRules;

  private String combineRulesWith;
  
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
    if ((expression != null) && !expression.equals("")) {
      try {
        ee = new ExpressionEvaluator(expression, // expression
                boolean.class, // expressionType
                getParamNames(), // parameterNames
                getParameterTypes(), // parameterTypes
                new Class[]{JSONException.class}, null);
      } catch (Exception ex) {
        logger.error("uh oh!! ", ex);
        DTThrowable.rethrow(ex);
      }
    } else {
      ee = null;
    }
  }

  private Class[] getParameterTypes()
  {
    if (parametersNameTypes.size() == 0) {
      return new Class[0];
    }
    Class[] paramTypes = new Class[1];
    parametersNameTypes.values().toArray(paramTypes);
    return paramTypes;
  }

  private String[] getParamNames()
  {
    if (parametersNameTypes.size() == 0) {
      return new String[0];
    }
    String[] paramNames = new String[1];
    parametersNameTypes.keySet().toArray(paramNames);
    return paramNames;
  }

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      try {
        boolean result = (Boolean)ee.evaluate(new Object[] { tuple });
        if (result) {
          output.emit(String.valueOf(tuple));
        }
      } catch (InvocationTargetException ex) {
        logger.error("uh oh! while process:", ex);
      }
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<String> inputJson = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple) {
      if (ee != null) {
        try {
          ObjectMapper mapper = new ObjectMapper();

          boolean result = (Boolean) ee.evaluate(new Object[]{mapper.readValue(tuple, tupleType)});
          if (result) {
            output.emit(String.valueOf(tuple));
          }
        } catch (InvocationTargetException ex) {
          logger.error("uh oh! while process:", ex);
        } catch (JsonParseException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (JsonMappingException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
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

  public void setAddRule(String addRule)
  {
    String[] parts = addRule.split(":");
    if (parts.length >= 2) {
      addExpression(parts[0], parts[1]);
    }
  }

  public void setRemoveRule(String removeRule)
  {
    removeExpression(removeRule);
  }

  public Class getTupleType()
  {
    return tupleType;
  }

  public void setTupleType(Class tupleType)
  {
    this.tupleType = tupleType;
  }

  public String getCombineRulesWith()
  {
    return combineRulesWith;
  }

  public void setCombineRulesWith(String combineRulesWith)
  {
    if (this.combineRulesWith.toLowerCase().equals(combineRulesWith)) {
      return;
    }
    this.combineRulesWith = combineRulesWith;
    if(this.combineRulesWith.toLowerCase().equals("or")) {
      setMatchRules(MatchRules.ANY);
    } else if(this.combineRulesWith.toLowerCase().equals("and")) {
      setMatchRules(MatchRules.ALL);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(RuleExpressionEvaluator.class);
}
