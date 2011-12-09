/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.query;

import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.lemurproject.galago.tupleflow.Parameters.Type;

/**
 *
 * @author sjh
 */
public class NodeParametersTest extends TestCase {

  public NodeParametersTest(String testName) {
    super(testName);
  }

  public void testLongDoubleCasting() {
    NodeParameters p = new NodeParameters();
    p.set("long", 1234);
    p.set("double", 1234.0);

    assert p.getDouble("long") == p.getDouble("double");

    Exception e = null;
    try {
      p.getLong("double");
    } catch (Exception ex) {
      e = ex;
    }
    assert e != null;
  }

  public void testConfusableParameters() {
    NodeParameters p = new NodeParameters();
    p.set("bool", "true");
    p.set("long", "1234");
    p.set("double", "0.1234");
    p.set("string", "'test-string'");
    p.set("other1", "@test");
    p.set("other2", ":test");
    p.set("other3", "=test");

    assert "true".equals(p.getString("bool"));
    assert "1234".equals(p.getString("long"));
    assert "0.1234".equals(p.getString("double"));
    assert "'test-string'".equals(p.getString("string"));
    assert "@test".equals(p.getString("other1"));
    assert ":test".equals(p.getString("other2"));
    assert "=test".equals(p.getString("other3"));

    String paramString = p.toString();
    assertEquals(":bool=@/true/"
            + ":double=@/0.1234/"
            + ":long=@/1234/"
            + ":other1=@/@test/"
            + ":other2=@/:test/"
            + ":other3=@/=test/"
            + ":string=@/'test-string'/", paramString);

    Node query = new Node("op", p, new ArrayList(), 0);
    query.toString();
    Node parsed = StructuredQuery.parse(query.toString());
    NodeParameters parsedParams = parsed.getNodeParameters();

    paramString = parsedParams.toString();
    assertEquals(
            ":bool=@/true/"
            + ":double=@/0.1234/"
            + ":long=@/1234/"
            + ":other1=@/@test/"
            + ":other2=@/:test/"
            + ":other3=@/=test/"
            + ":string=@/'test-string'/", paramString);

  }

  public void testNodeParameters() {
    NodeParameters p = new NodeParameters();
    // try setting some stuff
    p.set("bool", true);
    p.set("long", 1234L);
    p.set("double", 0.1234);
    p.set("string", "test string");

    // try getting it back again
    assert p.getBoolean("bool") == true;
    assert p.getLong("long") == 1234;
    assert p.getDouble("double") == 0.1234;
    assert p.getString("string").equals("test string");

    // try adding something badly
    Exception e = null;
    try {
      p.set("bool", "fail");
    } catch (Exception except) {
      e = except;
    }
    assert e != null;

    // try toString methods
    assert p.getAsString("bool").equals("true");
    assert p.getAsString("long").equals("1234");
    assert p.getAsString("double").equals("0.1234");
    assert p.getAsString("string").equals("test string");

    // try serialisation
    String paramString = p.toString();
    assert paramString.equals(
            ":bool=true"
            + ":double=0.1234:long=1234"
            + ":string=test string");

    Node n = new Node("op", p, new ArrayList(), 0);

    Node m = StructuredQuery.parse(n.toString());

    NodeParameters p2 = m.getNodeParameters();
    assert (p2.getKeyType("bool").equals(Type.BOOLEAN));
    assert (p2.getKeyType("long").equals(Type.LONG));
    assert (p2.getKeyType("double").equals(Type.DOUBLE));
    assert (p2.getKeyType("string").equals(Type.STRING));
  }
}
