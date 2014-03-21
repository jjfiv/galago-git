/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.query;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.Parameters.Type;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author sjh
 */
public class NodeParametersTest {
  @Test
  public void testLongDoubleCasting() {
    NodeParameters p = new NodeParameters();
    p.set("long", 1234);
    p.set("double", 1234.0);

    assertEquals(p.getDouble("long"), p.getDouble("double"), 0.0001);

    Exception e = null;
    try {
      p.getLong("double");
    } catch (Exception ex) {
      e = ex;
    }
    assert e != null;
  }

  @Test
  public void testConfusableParameters() {
    NodeParameters p = new NodeParameters();
    p.set("bool", "true");
    p.set("long", "1234");
    p.set("double", "0.1234");
    p.set("string", "'test-string'");
    p.set("other1", "@test");
    p.set("other2", ":test");
    p.set("other3", "=test");

    assertEquals("true", p.getString("bool"));
    assertEquals("1234", p.getString("long"));
    assertEquals("0.1234", p.getString("double"));
    assertEquals("'test-string'", p.getString("string"));
    assertEquals("@test", p.getString("other1"));
    assertEquals(":test", p.getString("other2"));
    assertEquals("=test", p.getString("other3"));

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

  @Test
  public void testNodeParameters() {
    NodeParameters p = new NodeParameters();
    // try setting some stuff
    p.set("bool", true);
    p.set("long", 1234L);
    p.set("double", 0.1234);
    p.set("string", "test string");

    // try getting it back again
    assertEquals(true, p.getBoolean("bool"));
    assertEquals(1234, p.getLong("long"));
    assertEquals(0.1234, p.getDouble("double"), 0.0001);
    assertEquals("test string", p.getString("string"));

    // try adding something badly
    Exception e = null;
    try {
      p.set("bool", "fail");
    } catch (Exception except) {
      e = except;
    }
    assertNotNull(e);

    // try toString methods
    assertEquals("true", p.getAsString("bool"));
    assertEquals("1234", p.getAsString("long"));
    assertEquals("0.1234", p.getAsString("double"));
    assertEquals("test string", p.getAsString("string"));

    // try serialisation
    String paramString = p.toString();
    assertEquals(":bool=true"
        + ":double=0.1234:long=1234"
        + ":string=test string", paramString);

    Node n = new Node("op", p, new ArrayList(), 0);

    Node m = StructuredQuery.parse(n.toString());

    NodeParameters p2 = m.getNodeParameters();
    assertEquals (Type.BOOLEAN, p2.getKeyType("bool"));
    assertEquals (Type.LONG, p2.getKeyType("long"));
    assertEquals (Type.DOUBLE, p2.getKeyType("double"));
    assertEquals (Type.STRING, p2.getKeyType("string"));
  }
}
