// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.window;

import org.lemurproject.galago.core.window.Window;
import org.lemurproject.galago.core.window.WindowProducer;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import junit.framework.TestCase;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class WindowProducerTest extends TestCase {

  public WindowProducerTest(String testName) {
    super(testName);
  }

  public void testOrderedWindowProduction() throws IOException, IncompatibleProcessorException {
    // Document:
    Document doc1 = new Document();
    doc1.fileId = 1;
    doc1.identifier = 10;
    doc1.terms = Arrays.asList(new String[]{"1","9","2","8","3","7","4","6","5"});
              // "5","6","4","7","3","8","2","9","1"} );

   Document doc2 = new Document();
    doc2.fileId = 2;
    doc2.identifier = 10;
    doc2.terms = Arrays.asList(new String[]{"5","6","4","7","3","8","2","9","1"} );
    
    Catcher<Window> catcher = new Catcher();

    // first try bi-grams ~(#od:1(a b))
    Parameters p = new Parameters();
    p.set( "n", 2 );
    p.set( "width", 2 );
    p.set( "ordered", true );
    WindowProducer bigramProducer = new WindowProducer(new FakeParameters(p));
    bigramProducer.setProcessor( catcher );

    bigramProducer.process( doc1 );

    assert( catcher.data.size() == 15 );
    assert( Utility.toString( catcher.data.get(0).data ).equals("1~9") );
    assert( Utility.toString( catcher.data.get(1).data ).equals("1~2") );
    assert( Utility.toString( catcher.data.get(2).data ).equals("9~2") );
    assert( Utility.toString( catcher.data.get(3).data ).equals("9~8") );

    assert( catcher.data.get(4).document == 10 );
    assert( catcher.data.get(5).document == 10 );

    assert( catcher.data.get(6).begin == 3 );
    assert( catcher.data.get(6).end == 4 );

    assert( catcher.data.get(7).begin == 3 );
    assert( catcher.data.get(7).end == 5 );


    assert( catcher.data.get(9).file == 1 );
    assert( catcher.data.get(10).filePosition == 10 );

    bigramProducer.process( doc2 );

    assert( catcher.data.get(17).file == 2 );
    assert( catcher.data.get(18).filePosition == 3 );

  }

  public void testUnorderedWindowProduction() throws IOException, IncompatibleProcessorException {
    // Document:
    Document doc = new Document();
    doc.identifier = 10;
    doc.terms = Arrays.asList(new String[]{"1","9","2","8","3","7","4","6","5"});
              // "5","6","4","7","3","8","2","9","1"} );

    Catcher<Window> catcher = new Catcher();

    // first try bi-grams ~(#od:1(a b))
    Parameters p = new Parameters();
    p.set( "n", 3 );
    p.set( "width", 5 );
    p.set( "ordered", false );
    WindowProducer bigramProducer = new WindowProducer(new FakeParameters(p));
    bigramProducer.setProcessor( catcher );

    bigramProducer.process( doc );

    assert( catcher.data.size() == 34 );
    assert( Utility.toString( catcher.data.get(0).data ).equals("1~2~9") );
    assert( Utility.toString( catcher.data.get(1).data ).equals("1~8~9") );
    assert( Utility.toString( catcher.data.get(10).data ).equals("7~8~9") );
    assert( Utility.toString( catcher.data.get(12).data ).equals("2~3~8") );

    assert( catcher.data.get(4).document == 10 );
    assert( catcher.data.get(5).document == 10 );

    assert( catcher.data.get(23).begin == 3 );
    assert( catcher.data.get(23).end == 7 );

    assert( catcher.data.get(27).begin == 4 );
    assert( catcher.data.get(27).end == 7 );

  }


  public class Catcher<T> implements Processor<T> {

    ArrayList<T> data = new ArrayList();

    public void reset(){
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
