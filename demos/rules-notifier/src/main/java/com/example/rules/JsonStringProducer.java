package com.example.rules;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Created by pramod on 11/24/15.
 */
public class JsonStringProducer {

    public static class PojoObject
    {
        public String uuid;
        public NestedObject readings;

        public PojoObject()
        {

        }

        public PojoObject(int count)
        {
            uuid = UUID.randomUUID().toString();
            readings = new NestedObject(count);
        }

        public PojoObject(int sensor1, int sensor2)
        {
            uuid = UUID.randomUUID().toString();
            readings = new NestedObject(sensor1, sensor2);
        }

        @Override
        public String toString()
        {
            return "{uuid = " + uuid + ", nestedObject = " + readings + "}";
        }

    }

    public static class NestedObject
    {
        public int sensor1;
        public int sensor2;

        public NestedObject(int count)
        {
            this.sensor1 = count;
        }

        public NestedObject(int sensor1, int sensor2)
        {
            this.sensor1 = sensor1;
            this.sensor2 = sensor2;
        }

        public NestedObject()
        {

        }

        @Override
        public String toString()
        {
            return "{sensor1 = " + sensor1 + ", sensor2 = " + sensor2 + "}";
        }
    }

    public void generate() throws IOException {
        Properties props = new Properties();
        props.put("metadata.broker.list", "node17.morado.com:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);
        Random random = new Random();
        ObjectMapper mapper = new ObjectMapper();
        try {
            while (true) {
                int sensor1 = random.nextInt(1000000);
                int sensor2 = random.nextInt(1000000);
                PojoObject pojoObject = new PojoObject(sensor1, sensor2);
                String pojoJson = mapper.writeValueAsString(pojoObject);
                KeyedMessage<String, String> message = new KeyedMessage<String, String>("RulesInput", pojoJson);
                producer.send(message);
            }
        } finally {
            producer.close();
        }
    }

    public static void main(String[] args) throws IOException {
        JsonStringProducer producer = new JsonStringProducer();
        producer.generate();
    }

}
