// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.traversal.IndriWeightConversionTraversal;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;

/**
 *
 * @author trevor
 */
public class WeightConversionTraversalTest extends TestCase {

  public WeightConversionTraversalTest(String testName) {
    super(testName);
  }

  /**
   * Test of afterNode method, of class WeightConversionTraversal.
   */
  public void testAfterNonWeighted() throws Exception {
    ArrayList<Node> internalNodes = new ArrayList<Node>();
    internalNodes.add(new Node("littlenode", "null"));
    Node root = new Node("bignode", internalNodes);

    IndriWeightConversionTraversal traversal = new IndriWeightConversionTraversal();
    Node result = traversal.afterNode(root);
    assertEquals(root, result);
  }

  public void testAfterWeighted() throws Exception {
    ArrayList<Node> internalNodes = new ArrayList<Node>();
    internalNodes.add(new Node("text", new NodeParameters("1.0")));
    internalNodes.add(new Node("text", new NodeParameters("dog")));
    Node root = new Node("weight", internalNodes);

    ArrayList<Node> expectedInternal = new ArrayList();
    expectedInternal.add(new Node("text", "dog"));
    NodeParameters expectedParameters = new NodeParameters();
    expectedParameters.set("0", 1.0);
    Node expected = new Node("combine", expectedParameters, expectedInternal, 0);

    IndriWeightConversionTraversal traversal = new IndriWeightConversionTraversal();
    Node result = traversal.afterNode(root);
    assertEquals(expected.toString(), result.toString());
  }

  public void testRealDecimals() throws Exception {
    Node root = StructuredQuery.parse("#weight(1.5 dog 2.0 cat)");
    assertEquals("#weight( #inside( #text:@/1/() #field:@/5/() ) #text:dog() #inside( #text:@/2/() #field:@/0/() ) #text:cat() )", root.toString());

    IndriWeightConversionTraversal traversal = new IndriWeightConversionTraversal();
    Node result = StructuredQuery.copy(traversal, root);
    assertEquals("#combine:0=1.5:1=2.0( #text:dog() #text:cat() )", result.toString());
  }

  public void testRealIntegers() throws Exception {
    Node root = StructuredQuery.parse("#weight(1 dog 2 cat)");
    assertEquals("#weight( #text:@/1/() #text:dog() #text:@/2/() #text:cat() )", root.toString());

    IndriWeightConversionTraversal traversal = new IndriWeightConversionTraversal();
    Node result = StructuredQuery.copy(traversal, root);
    assertEquals("#combine:0=1.0:1=2.0( #text:dog() #text:cat() )", result.toString());
  }
}
