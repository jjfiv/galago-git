/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskNameWriter;
import org.lemurproject.galago.core.types.DocumentMappingData;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author sjh
 */
public class DocumentNumberMapperTest {
  private static void makeNamesIndex(int maxDocNum, File folder) throws Exception {
    File temp = new File(folder + File.separator + "names");
    Parameters p = Parameters.instance();
    p.set("filename", temp.getAbsolutePath());
    DiskNameWriter writer = new DiskNameWriter(new FakeParameters(p));

    for (int i = 0; i <= maxDocNum; i++) {
      writer.process(new NumberedDocumentData("doc-"+i, "", "", i, 0));
    }

    writer.close();
  }

  @Test
  public void testDocumentMappingCreator() throws Exception {
    File index1 = null;
    File index2 = null;
    File index3 = null;
    try {
      index1 = FileUtility.createTemporaryDirectory();
      index2 = FileUtility.createTemporaryDirectory();
      index3 = FileUtility.createTemporaryDirectory();

      // three 10 document indexes (0 -> 9)
      makeNamesIndex(9, index1);
      makeNamesIndex(9, index2);
      makeNamesIndex(9, index3);

      Catcher<DocumentMappingData> catcher = new Catcher<DocumentMappingData>();
      DocumentNumberMapper mapper = new DocumentNumberMapper();
      mapper.setProcessor( catcher );

      mapper.process( new DocumentSplit(index1.getAbsolutePath(), "", new byte[0],new byte[0],0,3 ));
      mapper.process( new DocumentSplit(index2.getAbsolutePath(), "", new byte[0],new byte[0],1,3 ));
      mapper.process( new DocumentSplit(index3.getAbsolutePath(), "", new byte[0],new byte[0],2,3 ));

      mapper.close();

      assertEquals(0, catcher.data.get(0).indexId);
      assertEquals(0, catcher.data.get(0).docNumIncrement);
      assertEquals(1, catcher.data.get(1).indexId);
      assertEquals(10, catcher.data.get(1).docNumIncrement);
      assertEquals(2, catcher.data.get(2).indexId);
      assertEquals(20, catcher.data.get(2).docNumIncrement);

    } finally {
      if (index1 != null) {
        Utility.deleteDirectory(index1);
      }
      if (index2 != null) {
        Utility.deleteDirectory(index2);
      }
      if (index3 != null) {
        Utility.deleteDirectory(index3);
      }
    }
  }

  public static final class Catcher<T> implements Processor<T> {

    ArrayList<T> data = new ArrayList<T>();

    public void reset(){
      data = new ArrayList<T>();
    }

    public void process(T object) throws IOException {
      data.add(object);
    }

    public void close() throws IOException {
      //nothing
    }
  }

}
