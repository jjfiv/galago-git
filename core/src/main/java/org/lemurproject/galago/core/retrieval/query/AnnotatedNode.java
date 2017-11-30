/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.query;

import org.lemurproject.galago.utility.Parameters;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author sjh
 */
public class AnnotatedNode implements Serializable {

  private static final long serialVersionUID = 7684494262194438451L;
  public String type;
  public String className;
  public String parameters;
  public long document;
  public boolean atCandidate;
  public String returnValue;
  public String extraInfo;
  public List<AnnotatedNode> children;

  // useful sometimes
  public AnnotatedNode() {
  }

  public AnnotatedNode(String type,
          String className,
          String parameters,
          long document,
          boolean atCandidate,
          String returnValue,
          List<AnnotatedNode> children) {
    this.type = type;
    this.className = className;
    this.parameters = parameters;
    this.document = document;
    this.atCandidate = atCandidate;
    this.returnValue = returnValue;
    this.extraInfo = "";
    this.children = children;
  }

  public AnnotatedNode(String type,
          String className,
          String parameters,
          long document,
          boolean atCandidate,
          String returnValue,
          String extraInfo,
          List<AnnotatedNode> children) {
    this.type = type;
    this.className = className;
    this.parameters = parameters;
    this.document = document;
    this.atCandidate = atCandidate;
    this.returnValue = returnValue;
    this.extraInfo = extraInfo;
    this.children = children;
  }

  @Override
  public String toString() {
    return toPrettyString("");
  }

  private String toPrettyString(String prefix) {
    StringBuilder sb = new StringBuilder();
    sb.append(prefix);
    sb.append(type).append("\t").append(className).append("\t");
    sb.append(document).append("\t").append(atCandidate).append("\t");
    sb.append(returnValue).append("\t").append(parameters).append("\t");
    sb.append(extraInfo).append("\n");

    for (AnnotatedNode child : children) {
      sb.append(child.toPrettyString(prefix + "  "));
    }

    return sb.toString();
  }

  public Parameters toJSON() {
    Parameters out = Parameters.create();
    out.put("type", type);
    out.put("className", className);
    out.put("document", document);
    out.put("atCandidate", atCandidate);
    out.put("returnValue", returnValue);
    out.put("extraInfo", extraInfo);
    List<Parameters> cjson = new ArrayList<>();
    for (AnnotatedNode child : children) {
      cjson.add(child.toJSON());
    }
    out.put("children", cjson);
    return out;
  }
}
