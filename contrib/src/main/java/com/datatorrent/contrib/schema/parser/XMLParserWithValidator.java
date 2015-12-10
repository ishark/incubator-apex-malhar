package com.datatorrent.contrib.schema.parser;

import java.io.IOException;
import java.io.StringReader;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.netlet.util.DTThrowable;

public class XMLParserWithValidator extends Parser<String>
{
  private String schemaXSDFile;
  private transient Unmarshaller unmarshaller;
  private transient Validator validator;

  public transient DefaultOutputPort<Document> parsedOutput = new DefaultOutputPort<Document>();

  @Override
  public Object convert(String tuple)
  {
    // This method is not invoked for XML parser
    return null;
  }

  @Override
  public void processTuple(String inputTuple)
  {
    try {
      if (out.isConnected()) {
        StringReader reader = new StringReader(inputTuple);
        JAXBElement<?> output = unmarshaller.unmarshal(new StreamSource(reader), getClazz());
        LOG.debug(output.getValue().toString());
        out.emit(output.getValue());
      } else {
        validator.validate(new StreamSource(inputTuple));
      }
      if (validatedOutput.isConnected()) {
        validatedOutput.emit(inputTuple);
      }
      if (parsedOutput.isConnected()) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        try {
          builder = factory.newDocumentBuilder();
          Document doc = builder.parse(inputTuple);
          parsedOutput.emit(doc);

        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      LOG.debug("Failed to parse xml tuple {}, Exception = {} ", inputTuple, e);
      if (err.isConnected()) {
        err.emit(inputTuple);
      }
    }
  }

  @Override
  public void setup(com.datatorrent.api.Context.OperatorContext context)
  {
    try {
      JAXBContext ctx = JAXBContext.newInstance(getClazz());
      unmarshaller = ctx.createUnmarshaller();
      if (schemaXSDFile != null) {
        Path filePath = new Path(schemaXSDFile);
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.newInstance(filePath.toUri(), configuration);
        FSDataInputStream inputStream = fs.open(filePath);

        SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema schema = factory.newSchema(new StreamSource(inputStream));
        unmarshaller.setSchema(schema);
        validator = schema.newValidator();
      }
    } catch (SAXException e) {
      DTThrowable.wrapIfChecked(e);
    } catch (JAXBException e) {
      DTThrowable.wrapIfChecked(e);
    } catch (IOException e) {
      DTThrowable.wrapIfChecked(e);
    }
  }

  public String getSchemaFile()
  {
    return schemaXSDFile;
  }

  public void setSchemaFile(String schemaFile)
  {
    this.schemaXSDFile = schemaFile;
  }

  public static Logger LOG = LoggerFactory.getLogger(Parser.class);
}
