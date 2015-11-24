/**
 * Put your copyright and license info here.
 */
package com.example.rules;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.example.rules.RuleExpressionEvaluator.MatchRules;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.contrib.kafka.KafkaConsumer;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;

@ApplicationAnnotation(name = "Rule Notifier application")
public abstract class Application implements StreamingApplication
{
  public static class PojoObject
  {
    public String uuid;
    public NestedObject obj;

    public PojoObject()
    {

    }

    public PojoObject(int count)
    {
      uuid = UUID.randomUUID().toString();
      obj = new NestedObject(count);
    }

    @Override
    public String toString()
    {
      return "{uuid = " + uuid + ", nestedObject = " + obj + "}";
    }

  }

  public static class NestedObject
  {
    public int counter;

    public NestedObject(int count)
    {
      counter = count;
    }

    public NestedObject()
    {

    }

    @Override
    public String toString()
    {
      return "{counter = " + counter + "}";
    }
  }

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
      if (output.isConnected()) {
        output.emit(count++);
      } else if (outputJson.isConnected()) {
        PojoObject obj = new PojoObject(count++);
        ObjectMapper mapper = new ObjectMapper();
        try {
          outputJson.emit(new JSONObject(mapper.writeValueAsString(obj)));
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (JSONException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } else if (outputPojo.isConnected()) {
        outputPojo.emit(new PojoObject(count++));
      }
    }

    private int count = 0;
    public transient final DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();

    public transient final DefaultOutputPort<JSONObject> outputJson = new DefaultOutputPort<JSONObject>();

    public transient final DefaultOutputPort<PojoObject> outputPojo = new DefaultOutputPort<PojoObject>();

  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //AscendingNumbers randomGenerator = dag.addOperator("randomGenerator", AscendingNumbers.class);
    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());

    // Kafka input
    KafkaSinglePortStringInputOperator randomGenerator = dag.addOperator("kafkaInput",
        new KafkaSinglePortStringInputOperator());
    KafkaConsumer consumer = new SimpleKafkaConsumer();
    consumer.setTopic("input");
    consumer.setInitialOffset("earliest");
    consumer.setZookeeper("node17.morado.com:2181");
    //    consumer.setZookeeper("localhost:2181");
    randomGenerator.setConsumer(consumer);

    // Rule eval
    RuleExpressionEvaluator<String> eval = addExpressionEvaluator(dag, randomGenerator);

    // Kafka
    KafkaSinglePortOutputOperator<String, String> kafkaOut = dag.addOperator("kafka",
        new KafkaSinglePortOutputOperator<String, String>());
    kafkaOut.setTopic("Alerts");
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.setProperty("key.serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", "node17.morado.com:9092");
    //props.put("metadata.broker.list", "localhost:9092");
    props.setProperty("producer.type", "async");
    kafkaOut.setConfigProperties(props);
    conf.set("dt.operator.KafkaMessageProducer.prop.configProperties(metadata.broker.list)", "localhost:9092");

    // Web Socket
    PubSubWebSocketOutputOperator<String> socketOut = dag.addOperator("socket",
        new PubSubWebSocketOutputOperator<String>());
    socketOut.setTopic("Alerts");
    String gatewayAddress = "node0.morado.com:9099";
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
    socketOut.setUri(uri);

    dag.addStream("stream", eval.output, cons.input, socketOut.input, kafkaOut.inputPort);//.setLocality(Locality.CONTAINER_LOCAL);

  }

  abstract public RuleExpressionEvaluator<String> addExpressionEvaluator(DAG dag, KafkaSinglePortStringInputOperator randomGenerator);

//  {
//    RuleExpressionEvaluator<String> eval = dag.addOperator("rule", new RuleExpressionEvaluator<String>());
//    eval.addParameterNameType("input", PojoObject.class);
//    eval.addExpression("by10", "input.obj.counter% 100==0");
//    eval.addExpression("by5", "input.obj.counter% 50==0");
//    eval.setMatchRules(MatchRules.ANY);
//    dag.addStream("randomData", randomGenerator.outputPort, eval.input);//.setLocality(Locality.CONTAINER_LOCAL);
//    return eval;
//  }
}
