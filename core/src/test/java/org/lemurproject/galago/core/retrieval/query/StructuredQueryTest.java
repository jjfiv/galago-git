// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.query;

import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import junit.framework.TestCase;
import java.util.ArrayList;

/**
 *
 * @author trevor
 */
public class StructuredQueryTest extends TestCase {

  public StructuredQueryTest(String testName) {
    super(testName);
  }

  public static class SimpleCopyTraversal implements Traversal {

    public void beforeNode(Node node) {
      // do nothing
    }

    public Node afterNode(Node node) {
      return new Node(node.getOperator(), node.getNodeParameters(), Node.cloneNodeList(node.getInternalNodes()), node.getPosition());
    }
  }

  public static class NullTraversal implements Traversal {

    public void beforeNode(Node n) {
    }

    public Node afterNode(Node n) {
      return null;
    }
  }

  public static Node createQuery() {
    Node childB = new Node("text", "b", 0);
    Node childA = new Node("text", "a", 0);
    ArrayList<Node> childList = new ArrayList();
    childList.add(childA);
    Node featureA = new Node("feature", "bm25", childList, 0);
    ArrayList<Node> featureList = new ArrayList<Node>();
    featureList.add(featureA);
    featureList.add(childB);
    Node tree = new Node("combine", featureList, 0);

    return tree;
  }

  public void testCopy() throws Exception {
    Traversal traversal = new SimpleCopyTraversal();
    Node tree = createQuery();
    Node result = StructuredQuery.copy(traversal, tree);

    assertEquals(tree.toString(), result.toString());
  }

  public void testWalk() throws Exception {
    Traversal traversal = new NullTraversal();
    Node tree = createQuery();

    StructuredQuery.walk(traversal, tree);
  }

  public void testText() {
    String query = "hello";
    Node result = StructuredQuery.parse(query);
    assertEquals("#text:hello()", result.toString());
  }

  public void testTextOperator() {
    String query = "#text:hello()";
    Node result = StructuredQuery.parse(query);
    assertEquals("#text:hello()", result.toString());
  }

  public void testSimpleCombine() {
    String query = "#combine( hello )";
    Node result = StructuredQuery.parse(query);
    assertEquals("#combine( #text:hello() )", result.toString());
  }

  public void testSimpleTextCombine() {
    String query = "#combine( #text:hello() )";
    Node result = StructuredQuery.parse(query);
    assertEquals("#combine( #text:hello() )", result.toString());
  }

  public void testSimpleParse() {
    String query = "#combine( #feature:bm25(a) b )";
    Node tree = createQuery();

    Node result = StructuredQuery.parse(query);
    assertEquals(tree.toString(), result.toString());
  }

  public void testFieldParse() {
    String query = "#combine( a.b c.d @/e/ @/f. h/.g )";
    Node result = StructuredQuery.parse(query);
    assertEquals(
            "#combine( #inside( #text:a() #field:b() ) #inside( #text:c() #field:d() ) #text:e() #inside( #text:f. h() #field:g() ) )",
            result.toString());
  }

  public void testFieldCombinationParse() {
    String query = "a.b.c";
    Node result = StructuredQuery.parse(query);
    assertEquals(
            "#inside( #inside( #text:a() #field:b() ) #field:c() )",
            result.toString());
  }

  public void testFieldWindow() {
    String query = "#1(a b).c";
    Node result = StructuredQuery.parse(query);
    assertEquals(
            "#inside( #1( #text:a() #text:b() ) #field:c() )",
            result.toString());
  }

  public void testFieldSmoothWindow() {
    String query = "#1(a b).(c)";
    Node result = StructuredQuery.parse(query);
    assertEquals(
            "#smoothinside( #1( #text:a() #text:b() ) #field:c() )",
            result.toString());
  }

  public void testFieldCombinationParseCommas() {
    String query = "a.b,c";
    Node result = StructuredQuery.parse(query);
    assertEquals(
            "#inside( #text:a() #extentor( #field:b() #field:c() ) )",
            result.toString());
  }

  public void testParensParse() {
    String query = "a.(b) a.(b,c)";
    Node result = StructuredQuery.parse(query);
    assertEquals(
            "#root( #smoothinside( #text:a() #field:b() ) #smoothinside( #text:a() #extentor( #field:b() #field:c() ) ) )",
            result.toString());
  }

  public void testQuotes() {
    String query = "\"a b c\"";
    Node result = StructuredQuery.parse(query);
    assertEquals("#quote( #text:a() #text:b() #text:c() )", result.toString());
  }

  public void testParameterDoubles() {
    String query = "#combine:0=1.2:3.4=5( a b )";
    Node result = StructuredQuery.parse(query);
    assertEquals("#combine:0=1.2:3.4=5( #text:a() #text:b() )", result.toString());
  }
}
