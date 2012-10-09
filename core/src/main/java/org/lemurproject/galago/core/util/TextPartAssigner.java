// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

import java.util.Set;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.tartarus.snowball.ext.englishStemmer;

/**
 *
 * @author irmarc
 */
public class TextPartAssigner {

  public static Node assignPart(Node original, Parameters parts, Parameters globalParameters) {
    Set<String> available = parts.getKeys();
    
    Node transformed = original;
    if (globalParameters != null 
            && globalParameters.isBoolean("stemming") 
            && (globalParameters.getBoolean("stemming") == false)) {
        // we have a parameter to turn off stemming
        
        if (available.contains("postings")) {
            transformed = transformedNode(original, "postings", "extents");
        }
       
    } else {
        // default is to use porter stemmer
        if (available.contains("postings.porter")) {
            transformed = transformedNode(original, "postings.porter", "extents");
        } else if (available.contains("postings.krovetz")) {
            transformed = transformedNode(original, "postings.krovetz", "extents");
        } else if (available.contains("postings")) {
            return transformedNode(original, "postings", "extents");
        }
    }
    return transformed;
  }

  public static Node transformedNode(Node original,
          String indexName, String operatorName) {
    NodeParameters parameters = original.getNodeParameters();
    parameters.set("part", indexName);
    return new Node(operatorName, parameters, Node.cloneNodeList(original.getInternalNodes()), original.getPosition());
  }
}
