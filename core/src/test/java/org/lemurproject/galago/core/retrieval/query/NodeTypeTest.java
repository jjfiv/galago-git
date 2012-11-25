// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.query;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Date;
import junit.framework.TestCase;
import org.lemurproject.galago.core.retrieval.iterator.ExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class NodeTypeTest extends TestCase {

  public NodeTypeTest(String testName) {
    super(testName);
  }

  public void testGetIteratorClass() {
    NodeType n = new NodeType(MovableExtentIterator.class);
    assertEquals(MovableExtentIterator.class, n.getIteratorClass());
  }

  public void testIsMovableIteratorOrArray() {
    NodeType n = new NodeType(MovableExtentIterator.class);
    assertTrue(n.isMovableIteratorOrArray(MovableExtentIterator.class));
    assertTrue(n.isMovableIteratorOrArray(MovableIterator.class));
    assertFalse(n.isMovableIteratorOrArray(Integer.class));
    assertFalse(n.isMovableIteratorOrArray(Date.class));
    assertTrue(n.isMovableIteratorOrArray(new MovableExtentIterator[0].getClass()));
  }

  public void testGetInputs() throws Exception {
    NodeType n = new NodeType(FakeIterator.class);
    Class[] input = n.getInputs();
    assertEquals(3, input.length);
    assertEquals(NodeParameters.class, input[0]);
    assertEquals(MovableExtentIterator.class, input[1]);
    assertEquals(new MovableScoreIterator[0].getClass(), input[2]);
  }

  public void testGetParameterTypes() throws Exception {
    NodeType n = new NodeType(FakeIterator.class);
    Class[] input = n.getParameterTypes(4);
    assertEquals(4, input.length);
    assertEquals(MovableExtentIterator.class, input[0]);
    assertEquals(MovableScoreIterator.class, input[1]);
    assertEquals(MovableScoreIterator.class, input[2]);
    assertEquals(MovableScoreIterator.class, input[3]);
  }

  public void testGetConstructor() throws Exception {
    NodeType n = new NodeType(FakeIterator.class);
    Constructor c = n.getConstructor();
    Constructor actual =
            FakeIterator.class.getConstructor(NodeParameters.class, MovableExtentIterator.class,
            new MovableScoreIterator[0].getClass());
    assertEquals(actual, c);
  }

  public static class FakeIterator implements MovableIterator {

    public FakeIterator(NodeParameters parameters, MovableExtentIterator one, MovableScoreIterator[] two) {
    }

    public void reset() throws IOException {
    }

    public boolean isDone() {
      return true;
    }

    @Override
    public int currentCandidate() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void movePast(int identifier) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void syncTo(int identifier) throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean hasMatch(int identifier) {
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
    public String getEntry() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setContext(ScoringContext context) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public ScoringContext getContext() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int compareTo(MovableIterator t) {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
