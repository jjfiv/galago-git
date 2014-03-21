// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.window;

import org.junit.Test;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.tupleflow.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class WindowProducerTest {

  @Test
  public void testOrderedWindowProduction() throws IOException, IncompatibleProcessorException {
    // Document:
    Document doc1 = new Document();
    doc1.fileId = 1;
    doc1.identifier = 10;
    doc1.terms = Arrays.asList(new String[]{"1", "9", "2", "8", "3", "7", "4", "6", "5"});
    // "5","6","4","7","3","8","2","9","1"} );

    Document doc2 = new Document();
    doc2.fileId = 2;
    doc2.identifier = 10;
    doc2.terms = Arrays.asList(new String[]{"5", "6", "4", "7", "3", "8", "2", "9", "1"});

    Catcher<Window> catcher = new Catcher();

    // first try bi-grams ~(#od:2(a b))
    Parameters p = new Parameters();
    p.set("n", 2);
    p.set("width", 2);
    p.set("ordered", true);
    WindowProducer bigramProducer = new WindowProducer(new FakeParameters(p));
    bigramProducer.setProcessor(catcher);

    bigramProducer.process(doc1);

    assert (catcher.data.size() == 15);    
    assert (Utility.toString(catcher.data.get(0).data).equals("1~9"));
    assert (Utility.toString(catcher.data.get(1).data).equals("1~2"));
    assert (Utility.toString(catcher.data.get(2).data).equals("9~2"));
    assert (Utility.toString(catcher.data.get(3).data).equals("9~8"));

    assertEquals(catcher.data.get(4).document, 10);
    assertEquals(catcher.data.get(5).document, 10);

    assertEquals(catcher.data.get(6).begin, 3);
    assertEquals(catcher.data.get(6).end, 5);

    assertEquals(catcher.data.get(7).begin, 3);
    assertEquals(catcher.data.get(7).end, 6);


    assertEquals(catcher.data.get(9).file, 1);
    assertEquals(catcher.data.get(10).filePosition, 10);

    bigramProducer.process(doc2);

    assertEquals(catcher.data.get(17).file, 2);
    assertEquals(catcher.data.get(18).filePosition, 3);

  }

  @Test
  public void testUnorderedWindowProduction() throws IOException, IncompatibleProcessorException {
    // Document:
    Document doc = new Document();
    doc.identifier = 10;
    doc.terms = Arrays.asList(new String[]{"1", "9", "2", "8", "3", "7", "4", "6", "5"});
    // "5","6","4","7","3","8","2","9","1"} );

    Catcher<Window> catcher = new Catcher();

    // first try bi-grams ~(#uw:3(a b))
    Parameters p = new Parameters();
    p.set("n", 2);
    p.set("width", 3);
    p.set("ordered", false);
    WindowProducer bigramProducer = new WindowProducer(new FakeParameters(p));
    bigramProducer.setProcessor(catcher);

    bigramProducer.process(doc);

    assertEquals(15, catcher.data.size());
    assert (Utility.toString(catcher.data.get(0).data).equals("1~9"));
    assert (Utility.toString(catcher.data.get(1).data).equals("1~2"));
    assert (Utility.toString(catcher.data.get(2).data).equals("2~9"));
    assert (Utility.toString(catcher.data.get(3).data).equals("8~9"));

    assertEquals(10, catcher.data.get(4).document);
    assertEquals(10, catcher.data.get(5).document);

    assertEquals(3, catcher.data.get(6).begin);
    assertEquals(5, catcher.data.get(6).end);

    assertEquals(3, catcher.data.get(7).begin);
    assertEquals(6, catcher.data.get(7).end);

  }

  public void testTaggedOrderedWindowProduction() throws IOException, IncompatibleProcessorException {
    // Document:
    Document doc1 = new Document();
    doc1.fileId = 1;
    doc1.identifier = 10;
    doc1.terms = Arrays.asList(new String[]{"1", "9", "2", "8", "3", "7", "4", "6", "5"});
    doc1.tags = Arrays.asList(new Tag[]{new Tag("one", null, 0, 4), new Tag("one", null, 5, 8)});
    // "5","6","4","7","3","8","2","9","1"} );

    Catcher<Window> catcher = new Catcher();

    // first try bi-grams ~(#od:2(a b))
    Parameters p = new Parameters();
    p.set("n", 2);
    p.set("width", 1);
    p.set("ordered", true);
    p.set("fields", Arrays.asList(new String[]{"one"}));
    WindowProducer bigramProducer = new WindowProducer(new FakeParameters(p));
    bigramProducer.setProcessor(catcher);

    bigramProducer.process(doc1);

    assertEquals (catcher.data.size(), 5);
    assert (Utility.toString(catcher.data.get(0).data).equals("1~9"));
    assert (Utility.toString(catcher.data.get(1).data).equals("9~2"));
    assert (Utility.toString(catcher.data.get(2).data).equals("2~8"));
    assert (Utility.toString(catcher.data.get(3).data).equals("7~4"));

    assertEquals(catcher.data.get(3).document, 10);
    assertEquals(catcher.data.get(4).document, 10);

    assertEquals(catcher.data.get(1).begin, 1);
    assertEquals(catcher.data.get(1).end, 3);

    assertEquals(catcher.data.get(2).begin, 2);
    assertEquals(catcher.data.get(2).end, 4);

    assertEquals(catcher.data.get(3).file, 1);
    assertEquals(catcher.data.get(4).filePosition, 4);
  }

  @Test
  public void testOverlappingTagOrderedWindowProduction() throws IOException, IncompatibleProcessorException {
    // Document:
    Document doc1 = new Document();
    doc1.fileId = 1;
    doc1.identifier = 10;
    doc1.terms = Arrays.asList(new String[]{"1", "9", "2", "8", "3", "7", "4", "6", "5"});
    doc1.tags = Arrays.asList(new Tag[]{new Tag("two", null, 0, 4), new Tag("two", null, 5, 9), new Tag("three", null, 1, 4)});
    // "5","6","4","7","3","8","2","9","1"} );

    Catcher<Window> catcher = new Catcher();

    // first try bi-grams ~(#od:2(a b))
    Parameters p = new Parameters();
    p.set("n", 2);
    p.set("width", 1);
    p.set("ordered", true);
    p.set("fields", Arrays.asList(new String[]{"two", "three"}));
    WindowProducer bigramProducer = new WindowProducer(new FakeParameters(p));
    bigramProducer.setProcessor(catcher);

    bigramProducer.process(doc1);

    //catcher.printAll();

    assert (catcher.data.size() == 6);
    assert (Utility.toString(catcher.data.get(0).data).equals("1~9"));
    assert (Utility.toString(catcher.data.get(1).data).equals("9~2"));
    assert (Utility.toString(catcher.data.get(2).data).equals("2~8"));
    assert (Utility.toString(catcher.data.get(3).data).equals("7~4"));

    assertEquals(catcher.data.get(4).document, 10);
    assertEquals(catcher.data.get(5).document, 10);

    assertEquals(catcher.data.get(1).begin, 1);
    assertEquals(catcher.data.get(1).end, 3);

    assertEquals(catcher.data.get(2).begin, 2);
    assertEquals(catcher.data.get(2).end, 4);

    assertEquals(catcher.data.get(3).file, 1);
    assertEquals(catcher.data.get(4).filePosition, 4);
  }

  
  private static class Catcher<T> implements Processor<T> {

    ArrayList<T> data = new ArrayList();

    public void reset() {
      data = new ArrayList();
    }

    public void process(T object) throws IOException {
      data.add(object);
    }

    public void close() throws IOException {
      //nothing
    }
  }
}
