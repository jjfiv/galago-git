// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.junit.Test;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.ZipUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor
 */
public class DocumentSourceTest {
  public static final class FakeProcessor implements Processor<DocumentSplit> {

    public ArrayList<DocumentSplit> splits = new ArrayList<DocumentSplit>();

    @Override
    public void process(DocumentSplit split) {
      splits.add(split);
    }

    @Override
    public void close() throws IOException {
    }
  }

  @Test
  public void testUnknownFile() throws Exception {
    Parameters p = Parameters.instance();
    p.set("filename", "foo.c");
    DocumentSource source = new DocumentSource(new FakeParameters(p));
    FakeProcessor processor = new FakeProcessor();
    source.setProcessor(processor);

    boolean threwException = false;
    try {
      source.run();
    } catch (Exception e) {
      threwException = true;
    }
    assertTrue(threwException);
  }

  @Test
  public void testUnknownExtension() throws Exception {
    File tempFile = FileUtility.createTemporary();
    Parameters p = Parameters.instance();
    p.set("filename", tempFile.getAbsolutePath());
    DocumentSource source = new DocumentSource(new FakeParameters(p));
    FakeProcessor processor = new FakeProcessor();
    source.setProcessor(processor);

    source.run();
    assertEquals(0, processor.splits.size());
    assertTrue(tempFile.delete());
  }

  @Test
  public void testForcedZipFile() throws IOException {
    File tmp = null;

    try {
      tmp = File.createTempFile("zipUtilTest", ".zip");

      String fooContents = "foo is the best";
      String fooPath = "data/foo.txt";

      String trecWebContents = "<DOC>\n"
          + "<DOCNO>CACM-0001</DOCNO>\n"
          + "<DOCHDR>\n"
          + "http://www.yahoo.com:80 some extra text here\n"
          + "even more text in this part\n"
          + "</DOCHDR>\n"
          + "This is some text in a document.\n"
          + "</DOC>\n";
      String trecWebPath = "data/blah/easy.trecweb";

      // write zip file:
      ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(tmp.getAbsolutePath()));
      ZipUtil.write(zos, fooPath, ByteUtil.fromString(fooContents));
      ZipUtil.write(zos, trecWebPath, ByteUtil.fromString(trecWebContents));
      zos.close();

      ZipFile zipFile = ZipUtil.open(tmp);
      // read zip file:
      List<String> entries = ZipUtil.listZipFile(zipFile);
      assertEquals(2, entries.size());
      zipFile.close();

      List<DocumentSplit> splits = DocumentSource.processZipFile(tmp, Parameters.parseArray("filetype", "foo"));
      assertEquals(2, splits.size());
      assertEquals("foo", splits.get(0).fileType);
      assertEquals("foo", splits.get(1).fileType);

    } finally {
      if(tmp != null) assertTrue(tmp.delete());
    }
  }

  @Test
  public void testZipFile() throws IOException {
    File tmp = null;

    try {
      tmp = File.createTempFile("zipUtilTest", ".zip");

      String fooContents = "foo is the best";
      String fooPath = "data/foo.txt";

      String barContents = "bar is the best";
      String barPath = "data/subdir/ignore/bar.txt";

      String trecWebContents = "<DOC>\n"
              + "<DOCNO>CACM-0001</DOCNO>\n"
              + "<DOCHDR>\n"
              + "http://www.yahoo.com:80 some extra text here\n"
              + "even more text in this part\n"
              + "</DOCHDR>\n"
              + "This is some text in a document.\n"
              + "</DOC>\n";
      String trecWebPath = "data/blah/easy.trecweb";
      String guessTrecWebPath = "data/blah/guess_trecweb";

      // write zip file:
      ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(tmp.getAbsolutePath()));
      ZipUtil.write(zos, fooPath, ByteUtil.fromString(fooContents));
      ZipUtil.write(zos, barPath, ByteUtil.fromString(barContents));
      ZipUtil.write(zos, trecWebPath, ByteUtil.fromString(trecWebContents));
      ZipUtil.write(zos, guessTrecWebPath, ByteUtil.fromString(trecWebContents));
      zos.close();

      ZipFile zipFile = ZipUtil.open(tmp);
      // read zip file:
      List<String> entries = ZipUtil.listZipFile(zipFile);
      assertEquals(4, entries.size());
      zipFile.close();

      List<DocumentSplit> splits = DocumentSource.processZipFile(tmp, Parameters.instance());
      assertEquals(4, splits.size());
      assertEquals("txt", splits.get(0).fileType);
      assertEquals("txt", splits.get(1).fileType);
      assertEquals("trecweb", splits.get(2).fileType);
      assertEquals("trecweb", splits.get(3).fileType);

    } finally {
      if(tmp != null) assertTrue(tmp.delete());
    }
  }
}
