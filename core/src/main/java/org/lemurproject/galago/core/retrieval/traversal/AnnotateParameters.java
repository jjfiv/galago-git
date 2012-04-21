// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.HashSet;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.retrieval.GroupRetrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Class ensures that iterators are annotated with required parameters. -
 *
 * @author sjh
 */
public class AnnotateParameters extends Traversal {

  Parameters queryParameters;
  Parameters globalParameters;
  Retrieval retrieval;

  // featurefactory is necessary to get the correct class
  public AnnotateParameters(Retrieval retrieval, Parameters queryParams) throws IOException {
    this.globalParameters = retrieval.getGlobalParameters();
    this.queryParameters = queryParams;
    this.retrieval = retrieval;

  }

  @Override
  public void beforeNode(Node node) {
  }

  @Override
  public Node afterNode(Node node) throws Exception {
    // need to get list of required statistics
    RequiredParameters required = null;
    Class c = retrieval.getNodeType(node).getIteratorClass();

    // need to cascade down super classes to find sub-annotations.
    while (c != null && StructuredIterator.class.isAssignableFrom(c)) {
      Class<? extends StructuredIterator> d = c;
      required = d.getAnnotation(RequiredParameters.class);

      // then annotate the node with any of:
      // -- nodeFreq, nodeDocCount, collLen, docCount, collProb
      if (required != null) {
        for (String p : required.parameters()) {
          if (!node.getNodeParameters().containsKey(p)
                  && (queryParameters.containsKey(p) || globalParameters.containsKey(p))) {
            if (queryParameters.isBoolean(p) || globalParameters.isBoolean(p)) {
              node.getNodeParameters().set(p, queryParameters.get(p, globalParameters.getBoolean(p)));
            } else if (queryParameters.isDouble(p) || globalParameters.isDouble(p)) {
              node.getNodeParameters().set(p, queryParameters.get(p, globalParameters.getDouble(p)));
            } else if (queryParameters.isLong(p) || globalParameters.isLong(p)) {
              node.getNodeParameters().set(p, queryParameters.get(p, globalParameters.getLong(p)));
            } else if (queryParameters.isString(p) || globalParameters.isString(p)) {
              node.getNodeParameters().set(p, queryParameters.get(p, globalParameters.getString(p)));
            } else {
              throw new RuntimeException("Parameter " + p + " was specified for this query or globally"
                      + "\nbut it could not be annotated into node: " + node.toString()
                      + "\nPlease ensure the parameter is specified as a simple type : {String,boolean,long,double} "
                      + "in the query parameters or global parameters.");
            }
          } else {
            // debugging code.
            //Logger.getLogger(this.getClass().getName()).info("Parameter " + p + " not found - using default value.");
          }
        }
      }

      // recurse up to the next superclass
      c = c.getSuperclass();
    }
    return node;
  }
}