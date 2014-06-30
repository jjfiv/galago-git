// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.utility.Parameters;

/*
 * window operator
 * 
 * rewrites the window meta-operator :
 *  #window:part=n3-w1-ordered-h2( term term term )
 *  or
 *  #window:width=1:ordered=true:h=2:usedocfreq=false:stemming=false( term term term )
 * 
 * to an operator that can be run on an n-gram index :
 * 
 *  #extents:part=n3-w2-ordered-h2:term~term~term()
 * 
 * @author sjh
 * 
 */
public class WindowRewriteTraversal extends Traversal {

  Parameters availiableParts;

  public WindowRewriteTraversal(Retrieval retrieval) throws IOException {
    this.availiableParts = retrieval.getAvailableParts();
  }

  /*
   * before node checks that an ngram operator is possible
   * 
   */
  @Override
  public void beforeNode(Node node, Parameters qp) throws Exception {
    if (node.getOperator().equals("window")) {
      for (Node child : node.getInternalNodes()) {
        if (!child.getOperator().equals("text")) {
          throw new Exception("Arguments of an window operator need to be text nodes.\n"
                  + "Problem argument : \n" + child.toString());
        }
      }

      int n = node.getInternalNodes().size();
      if (!possibleIndexExists(n)) {
        throw new Exception("No indexes found for " + n + "-windows. Aborting.\n" + node.toString());
      }
    }
  }

  /*
   * after node creates the extent node that accesses the correct ngram index
   * 
   */
  @Override
  public Node afterNode(Node original, Parameters qp) throws Exception {

    if (original.getOperator().equals("window")) {

      NodeParameters p = original.getNodeParameters();
      String part;
      if (p.containsKey("part")) {
        part = p.getString("part");
      } else {
        int n = original.getInternalNodes().size();
        part = getnGramPartName(n, p);
        p.set("part", part);
      }

      String window = createWindow(original.getInternalNodes());
      p.set("default", window);
      return new Node("extents", p, new ArrayList<Node>(), original.getPosition());
    }
    return original;
  }

  // minimum work to ensure that an n-gram index exists
  private boolean possibleIndexExists(int n) {
    for (String part : availiableParts.getKeys()) {
      if (part.startsWith("n" + Integer.toString(n))) {
        return true;
      }
    }
    return false;
  }

  /* pick index part matching the parameters
   * 
   *  window indexes look like: nX-wX-[un]ordered-hX[-docfreq][-stemmed]
   */
  private String getnGramPartName(int n, NodeParameters p) {
    String selectedPart = null;
    int currentThreshold = Integer.MAX_VALUE;


    // first build a set of requirements
    HashSet<String> desiredPartAttributes = new HashSet();
    desiredPartAttributes.add("n" + Integer.toString(n));
    if (p.containsKey("width")) {
      desiredPartAttributes.add(".w" + p.getLong("width"));
    }
    if (p.containsKey("threshold")) {
      desiredPartAttributes.add(".h" + p.getLong("threshold"));
    }
    if (p.containsKey("ordered")) {
      if (p.getBoolean("ordered")) {
        desiredPartAttributes.add("od.");
      } else {
        desiredPartAttributes.add("uw.");
      }
    }
    if (p.containsKey("stemming")) {
      if (p.getBoolean("stemming")) {
        // this won't work.
        desiredPartAttributes.add(".porter");
      }
    }
    if (p.containsKey("usedocfreq")) {
      if (p.getBoolean("usedocfreq")) {
        desiredPartAttributes.add(".df");
      }
    }

    List<String> parts = new ArrayList<String>(availiableParts.getKeys());
    Collections.sort(parts);
    for (String part : parts) {
      boolean flag = true;
      for (String attr : desiredPartAttributes) {
        if (!part.contains(attr)) {
          flag = false;
        }
      }
      // if all attributes match
      if (flag) {
        String hValue = part.split(".")[3].replace("h", "");
        int h = Integer.parseInt(hValue);
        if (h <= currentThreshold) {
          currentThreshold = h;
          selectedPart = part;
        }
      }
    }
    return selectedPart;
  }

  private String createWindow(List<Node> children) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    String text;

    for (Node child : children) {
      if (!first) {
        sb.append("~");
      }
      first = false;
      sb.append(child.getDefaultParameter());
    }
    return sb.toString();
  }
}
