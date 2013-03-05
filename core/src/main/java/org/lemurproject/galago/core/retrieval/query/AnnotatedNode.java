/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.query;

import java.util.List;

/**
 *
 * @author sjh
 */
public class AnnotatedNode {

  public String type;
  public String className;
  public String parameters;
  public int document;
  public boolean atCandidate;
  public String returnValue;
  public String extraInfo;
  public List<AnnotatedNode> children;

  public AnnotatedNode(String type,
          String className,
          String parameters,
          int document,
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
          int document,
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
    
    for(AnnotatedNode child : children){
      sb.append(child.toPrettyString(prefix + "  "));
    }

    return sb.toString();
  }
}
