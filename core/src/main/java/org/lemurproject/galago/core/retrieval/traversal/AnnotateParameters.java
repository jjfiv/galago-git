// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.structured.RequiredParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Class ensures that iterators are annotated with required parameters. -
 *
 * @author sjh
 */
public class AnnotateParameters extends Traversal {

  Parameters globalParameters;
  Retrieval retrieval;

  // featurefactory is necessary to get the correct class
  public AnnotateParameters(Retrieval retrieval) throws IOException {
    this.globalParameters = retrieval.getGlobalParameters();
    this.retrieval = retrieval;
  }

  @Override
  public void beforeNode(Node node, Parameters qp) {
  }

  @Override
  public Node afterNode(Node node, Parameters queryParameters) throws Exception {
    // need to get list of required statistics
    RequiredParameters required = null;
    Class c = retrieval.getNodeType(node).getIteratorClass();

    // need to cascade down super classes to find sub-annotations.
    while (c != null && MovableIterator.class.isAssignableFrom(c)) {
      Class<? extends MovableIterator> d = c;
      required = d.getAnnotation(RequiredParameters.class);

      // then annotate the node with any of:
      // -- nodeFreq, nodeDocCount, collLen, docCount, collProb
      if (required != null) {
        for (String p : required.parameters()) {
          if (!node.getNodeParameters().containsKey(p)
                  && (queryParameters.containsKey(p))) {
            if (queryParameters.isBoolean(p)) {
              node.getNodeParameters().set(p, queryParameters.getBoolean(p));
            } else if (queryParameters.isDouble(p)) {
              node.getNodeParameters().set(p, queryParameters.getDouble(p));
            } else if (queryParameters.isLong(p)) {
              node.getNodeParameters().set(p, queryParameters.getLong(p));
            } else if (queryParameters.isString(p)) {
              node.getNodeParameters().set(p, queryParameters.getString(p));
            } else {
              throw new RuntimeException("Parameter " + p + " was specified for this query"
                      + "\nbut it could not be annotated into node: " + node.toString()
                      + "\nPlease ensure the parameter is specified as a simple type : {String,boolean,long,double} "
                      + "in the query parameters or global parameters.");
            }
          } else if (!node.getNodeParameters().containsKey(p)
                  && (globalParameters.containsKey(p))) {
            if (globalParameters.isBoolean(p)) {
              node.getNodeParameters().set(p, globalParameters.getBoolean(p));
            } else if (globalParameters.isDouble(p)) {
              node.getNodeParameters().set(p, globalParameters.getDouble(p));
            } else if (globalParameters.isLong(p)) {
              node.getNodeParameters().set(p, globalParameters.getLong(p));
            } else if (globalParameters.isString(p)) {
              node.getNodeParameters().set(p, globalParameters.getString(p));
            } else {
              throw new RuntimeException("Parameter " + p + " was specified for globally"
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

      // default up to the next superclass
      c = c.getSuperclass();
    }
    return node;
  }
}
