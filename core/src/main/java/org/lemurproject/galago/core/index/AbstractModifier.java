// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
public abstract class AbstractModifier implements IndexPartModifier {

  protected BTreeReader reader;
  protected String source;
  protected String name;

  public AbstractModifier(BTreeReader reader) {
    this.reader = reader;
    Parameters p = reader.getManifest();
    source = p.get("part", "unknown");
    name = p.get("name", "unknown");
  }

  public AbstractModifier(String filename) throws FileNotFoundException, IOException {
    this(BTreeFactory.getBTreeReader(filename));
  }

  public void close() throws IOException {
    this.reader.close();
  }

  public String getSourcePart() {
    return source;
  }

  public String getModifierName() {
    return name;
  }

  public static String getModifierName(String dir, String part, String name) {
      return String.format("%s/%s.%s", dir, part, name);
  }

  public Parameters getManifest() {
    return reader.getManifest();
  }

  public boolean isEligible(Node node) {
    NodeParameters p = node.getNodeParameters();
    return (p.get("part", "none").equals(source) &&
            p.get("mod", "none").equals(name));
  }
}