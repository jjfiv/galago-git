// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.query;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Date;

import static org.junit.Assert.*;

/**
 *
 * @author trevor
 */
public class NodeTypeTest {
  @Test
  public void testGetIteratorClass() {
    NodeType n = new NodeType(ExtentIterator.class);
    assertEquals(ExtentIterator.class, n.getIteratorClass());
  }

  @Test
  public void testIsIteratorOrArray() {
    NodeType n = new NodeType(ExtentIterator.class);
    assertTrue(n.isIteratorOrArray(ExtentIterator.class));
    assertTrue(n.isIteratorOrArray(BaseIterator.class));
    assertFalse(n.isIteratorOrArray(Integer.class));
    assertFalse(n.isIteratorOrArray(Date.class));
    assertTrue(n.isIteratorOrArray(new ExtentIterator[0].getClass()));
  }

  @Test
  public void testGetInputs() throws Exception {
    NodeType n = new NodeType(FakeIterator.class);
    Class[] input = n.getInputs();
    assertEquals(3, input.length);
    assertEquals(NodeParameters.class, input[0]);
    assertEquals(ExtentIterator.class, input[1]);
    assertEquals(new ScoreIterator[0].getClass(), input[2]);
  }

  @Test
  public void testGetParameterTypes() throws Exception {
    NodeType n = new NodeType(FakeIterator.class);
    Class[] input = n.getParameterTypes(4);
    assertEquals(4, input.length);
    assertEquals(ExtentIterator.class, input[0]);
    assertEquals(ScoreIterator.class, input[1]);
    assertEquals(ScoreIterator.class, input[2]);
    assertEquals(ScoreIterator.class, input[3]);
  }

  @Test
  public void testGetConstructor() throws Exception {
    NodeType n = new NodeType(FakeIterator.class);
    Constructor c = n.getConstructor();
    Constructor actual =
            FakeIterator.class.getConstructor(NodeParameters.class, ExtentIterator.class,
            new ScoreIterator[0].getClass());
    assertEquals(actual, c);
  }

  public static class FakeIterator implements BaseIterator {

    public FakeIterator(NodeParameters parameters, ExtentIterator one, ScoreIterator[] two) {
    }

    @Override
    public void reset() throws IOException {
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public long currentCandidate() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void movePast(long identifier) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void syncTo(long identifier) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasMatch(ScoringContext context) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasAllCandidates() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getValueString(ScoringContext sc) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
