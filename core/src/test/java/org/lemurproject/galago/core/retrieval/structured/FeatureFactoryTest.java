// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.structured;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.FeatureFactory;
import org.lemurproject.galago.core.retrieval.iterator.*;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.utility.Parameters;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor, irmarc
 */
public class FeatureFactoryTest {
  /**
   * Test of getClassName method, of class FeatureFactory.
   */
  @Test
  public void testGetClassName() throws Exception {
    FeatureFactory f = new FeatureFactory(Parameters.create());
    String actual = f.getClassName(new Node("syn", "fakeargument"));
    assertEquals(SynonymIterator.class.getName(), actual);
  }

  /**
   * Test of getClass method, of class FeatureFactory.
   */
  @Test
  public void testGetClass() throws Exception {
    FeatureFactory f = new FeatureFactory(Parameters.create());
    Class c = f.getClass(new Node("combine", ""));
    assertEquals(ScoreCombinationIterator.class.getName(), c.getName());
  }
  
  /**
   * Test of getNodeType method, of class FeatureFactory.
   */
  @Test
  public void testGetNodeType() throws Exception {
    FeatureFactory f = new FeatureFactory(Parameters.create());
    NodeType type = f.getNodeType(new Node("combine", ""));
    Class c = type.getIteratorClass();
    assertEquals(ScoreCombinationIterator.class.getName(), c.getName());
  }

  /**
   * Test of getIterator method, of class FeatureFactory.
   */
  @Test
  public void testGetIterator() throws Exception {
    FeatureFactory f = new FeatureFactory(Parameters.create());
    ArrayList<BaseIterator> iterators = new ArrayList<BaseIterator>();
    iterators.add(new NullExtentIterator());

    NodeParameters np = new NodeParameters();
    np.set("default", 5);
    BaseIterator iterator = f.getIterator(new Node("od", np, new ArrayList(), 0), iterators);
    assertEquals(OrderedWindowIterator.class.getName(), iterator.getClass().getName());
  }

  @Test
  public void testGetClassNameConfig() throws Exception {
    String config = "{ \"operators\" : {\"a\" : \"b\" } }";
    Parameters p = Parameters.parseString(config);
    FeatureFactory f = new FeatureFactory(p);

    assertEquals("b", f.getClassName(new Node("a", new ArrayList())));
  }
}
