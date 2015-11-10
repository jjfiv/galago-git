/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.PassageFilterIterator;
import org.lemurproject.galago.core.retrieval.iterator.PassageLengthIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.utility.Parameters;

/**
 * Inserts global passage restriction (length/extent) iterators in a node tree.
 * 
 * Inserts at the the appropriate layer of the iterator tree;
 *   just below scoring iterators, top of extent/length iterators.
 * 
 *  -- All other nodes are ignored --
 * 
 * @author sjh
 */
public class PassageRestrictionTraversal extends Traversal {

  private Retrieval retrieval;
  private Parameters globalParams;
  private boolean defaultPassageQuery;
  private boolean defaultWrapLengths;
  private boolean defaultWrapExtents;

  public PassageRestrictionTraversal(Retrieval retrieval) {
    this.retrieval = retrieval;
    this.globalParams = retrieval.getGlobalParameters();
    this.defaultPassageQuery = this.globalParams.get("passageQuery", false) || this.globalParams.get("extentQuery", false);

    // if a field index is being used, it might be important to ignore this wrapper.
    this.defaultWrapLengths = this.globalParams.get("wrapPassageLengths", true);

    // if some special index is being used, it might be important to ignore this wrapper.
    this.defaultWrapExtents = this.globalParams.get("wrapPassageLengths", true);
  }

  @Override
  public void beforeNode(Node object, Parameters queryParameters) throws Exception {
    // do nothing
  }

  @Override
  public Node afterNode(Node original, Parameters queryParameters) throws Exception {
    boolean passageQuery = queryParameters.get("passageQuery", false) || queryParameters.get("extentQuery", false);
    if (!passageQuery) {
      return original;
    }
    // if a field index is being used, it might be important to ignore this wrapper.
    boolean wrapLengths = queryParameters.get("wrapPassageLengths", true);
    // if some special index is being used, it might be important to ignore this wrapper.
    boolean wrapExtents = queryParameters.get("wrapPassageLengths", true);
    
    // check if the node returns extents or lengths
    NodeType nodeType = retrieval.getNodeType(original);

    // check for a lengths node, that is not already a passagelengths node
    if (wrapLengths && nodeType != null && LengthsIterator.class.isAssignableFrom(nodeType.getIteratorClass())
            && !PassageLengthIterator.class.isAssignableFrom(nodeType.getIteratorClass())) {
      Node parent = original.getParent();
      // check if parent node is a neither extents nor lengths node (e.g. scoring or other), even null (original == root), do nothing (?)
      NodeType parType = (parent != null) ? retrieval.getNodeType(parent) : null;
      if (parType != null && !LengthsIterator.class.isAssignableFrom(parType.getIteratorClass())) {
        // if so : wrap in passage restriction 
        Node replacement = new Node("passagelengths");
        replacement.addChild(original);
        return replacement;
      }
    }

    // check for an extents node that is not already a restriction node
    if (wrapExtents && nodeType != null
            && ExtentIterator.class.isAssignableFrom(nodeType.getIteratorClass())
            && !PassageFilterIterator.class.isAssignableFrom(nodeType.getIteratorClass())) {

      Node parent = original.getParent();

      // check if parent node is a neither extents nor lengths node (e.g. scoring or other), if null (original == root), do nothing (?)
      NodeType parType = (parent != null) ? retrieval.getNodeType(parent) : null;
      if (parType != null && !ExtentIterator.class.isAssignableFrom(parType.getIteratorClass())) {
        // if so : wrap in passage restriction 
        Node replacement = new Node("passagefilter");
        replacement.addChild(original);
        return replacement;
      }
    }

     // TODO: check for a counts node, if found, replace it with an extents node
     // However, keep in mind that not all indexes have extents so this may fail later
     // That said, this is a best-effort approach and if you don't have extents then passage retrieval
     // doesn't make any sense anyway.

    return original;
  }
}
