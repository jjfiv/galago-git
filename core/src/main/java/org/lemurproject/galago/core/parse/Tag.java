// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.Serializable;
import java.util.Map.Entry;
import java.util.Map;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * This class represents a tag in a XML/HTML document.
 * 
 * A tag has a tagName, an optional set of attributes, a beginning position and an
 * end position.  The positions are in terms of tokens, so if begin = 5, that means
 * the open tag is between token 5 and token 6.
 * 
 * @author trevor
 */
public class Tag implements Comparable<Tag>, Serializable {

  /**
   * Constructs a tag.
   *
   * @param tagName The tagName of the tag.
   * @param attributes Attributes of the tag.
   * @param begin Location of the start tag within the document, in tokens.
   * @param end Location of the end tag within the document, in tokens.
   */
  public Tag(String name, Map<String, String> attributes, int begin, int end) {
    this.name = name;
    this.attributes = attributes;
    this.begin = begin;
    this.end = end;
  }

  /**
   * Compares two tags together.  Tags are ordered by the location of
   * the open tag.  If we find two tags opening at the same location, the tie
   * is broken by the location of the closing tag.
   *
   * @param other
   * @return
   */
  @Override
  public int compareTo(Tag other) {
    int deltaBegin = begin - other.begin;
    if (deltaBegin == 0) {
      return end - other.end;
    }
    return deltaBegin;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append("<");
    builder.append(name);

    for (Entry<String, String> entry : attributes.entrySet()) {
      builder.append(' ');
      builder.append(entry.getKey());
      builder.append('=');
      builder.append('"');
      builder.append(entry.getValue());
      builder.append('"');
    }

    builder.append('>');
    return builder.toString();
  }
  public String name;
  public Map<String, String> attributes;
  public int begin;
  public int end;
}
