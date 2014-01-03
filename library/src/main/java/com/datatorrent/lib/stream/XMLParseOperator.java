/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.stream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.validation.constraints.NotNull;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * Takes xml byte array and picks values of specific nodes and attributes using xpath and emits them as a map of key values
 * <b>input</b>: expects xml byte array<br>
 * <b>outputMap</b>: emits HashMap&lt;String,String&gt;<br>
 * key: user supplied key
 * value: value of the key from xml
 *
 * @Since 0.9.3
 */
public class XMLParseOperator extends BaseOperator
{
  private transient DocumentBuilderFactory builderFactory;
  private transient DocumentBuilder builder;
  private transient XPath xPath;
  /*
   * user defined map for xpath lookup
   * key: name of the element
   * value: element xpath to lookup
   */
  @NotNull
  private Map<String, String> elementKeys;
  private HashMap<String, String> elementMap;
  private static final Logger logger = LoggerFactory.getLogger(XMLParseOperator.class);

  @Override
  public void setup(OperatorContext context)
  {
    try {
      builderFactory = DocumentBuilderFactory.newInstance();
      builder = builderFactory.newDocumentBuilder();
      xPath = XPathFactory.newInstance().newXPath();
      elementKeys = new HashMap<String, String>();
      elementMap = new HashMap<String, String>();
    }
    catch (ParserConfigurationException ex) {
      logger.error("setup exception", ex);
    }
  }

  /*
   * xml input as a byte array
   */
  @InputPortFieldAnnotation(name = "byteArrayXMLInput")
  public final transient DefaultInputPort<byte[]> byteArrayXMLInput = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] t)
    {
      processTuple(t);
      outputMap.emit(elementMap);
    }

  };

  /**
   * Gets the values of required elements from xml and emits them
   *
   * @param xmlBytes
   */
  private void processTuple(byte[] xmlBytes)
  {
    Document xmlDocument = toXmlDocument(xmlBytes);
    for (Map.Entry<String, String> entry : elementKeys.entrySet()) {

      String element = entry.getValue();
      String key = entry.getKey();
      try {
        if (element.contains("@")) {
          // attribute value
          String attr = (String)xPath.compile(element).evaluate(xmlDocument, XPathConstants.STRING);
          elementMap.put(key, attr);
        }
        else {
          // node value
          Node node = (Node)xPath.compile(element).evaluate(xmlDocument, XPathConstants.NODE);
          elementMap.put(key, node.getTextContent());
        }
      }
      catch (XPathExpressionException ex) {
        logger.error("error in xpath", ex);
      }
    }

  }

  /**
   * converts the given xml to document
   *
   * @param xmlBytes input xml byte array
   * @return Document
   */
  private Document toXmlDocument(byte[] xmlBytes)
  {
    try {
      Document xmlDocument;
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(xmlBytes);
      xmlDocument = builder.parse(byteArrayInputStream);

      return xmlDocument;

    }
    catch (SAXException ex) {
      logger.error("exception during converting xml string to document", ex);
    }
    catch (IOException ex) {
      logger.error("exception during converting xml string to document", ex);
    }
    return null;
  }

  /**
   * keys to extract from xml using xpath
   *
   * @param element xpath key
   * @param name name to use for key
   */
  public void addElementKeys(String element, String name)
  {
    elementKeys.put(element, name);
  }

  /**
   * Output hash map port.
   */
  @OutputPortFieldAnnotation(name = "map")
  public final transient DefaultOutputPort<HashMap<String, String>> outputMap = new DefaultOutputPort<HashMap<String, String>>();
}
