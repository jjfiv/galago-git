// BSD License (http://lemurproject.org/galago-license)
// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.EOFException;
import java.io.FileInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.lemurproject.galago.tupleflow.StreamCreator;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.tukaani.xz.XZInputStream;

/**
 * A version of the UniversalParser which merely counts how many parseable
 * docs are in a particular split. This matters if you want to generate a
 * subcollection.
 *
 * @author irmarc
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
public class UniversalCounter extends StandardStep<DocumentSplit, KeyValuePair> {

  private Counter documentCounter;
  private Parameters parameters;
  private long count;
  private Logger LOG = Logger.getLogger(getClass().toString());
  private Closeable source;

  public UniversalCounter(TupleFlowParameters parameters) {
    documentCounter = parameters.getCounter("Documents Parsed");
    this.parameters = parameters.getJSON();
    initializeParsers();
  }

  /**
   * Use this code to pick a different tag other than 'pb' to break documents.
   * To do book-level parsing, use a tag that doesn't occur in the format, such as
   * "book".
   */
  private void initializeParsers() {
    MBTEIParser.splitTag = parameters.get("splitTag", "pb");
  }

  public void process(DocumentSplit split) throws IOException {
    DocumentStreamParser parser = null;
    source = null;

    try {

      if (split.fileType.equals("html")
              || split.fileType.equals("xml")
              || split.fileType.equals("txt")) {
        parser = new FileParser(parameters, split.fileName, getLocalBufferedReader(split));
      } else if (split.fileType.equals("arc")) {
        parser = new ArcParser(getLocalBufferedInputStream(split));
      } else if (split.fileType.equals("warc")) {
        parser = new WARCParser(getLocalBufferedInputStream(split));
      } else if (split.fileType.equals("trectext")) {
        parser = new TrecTextParser(getLocalBufferedReader(split));
      } else if (split.fileType.equals("trecweb")) {
        parser = new TrecWebParser(getLocalBufferedReader(split));
      } else if (split.fileType.equals("twitter")) {
        parser = new TwitterParser(getLocalBufferedReader(split));
      } else if (split.fileType.equals("corpus")) {
        parser = new CorpusSplitParser(split);
      } else if (split.fileType.equals("wiki")) {
        parser = new WikiParser(getLocalBufferedReader(split));
      } else if (split.fileType.equals("wex")) {
          parser = new WikiWexParser(getLocalBufferedReader(split));
      } else if (split.fileType.equals("mbtei")) {
        parser = new MBTEIParser(split, getLocalBufferedInputStream(split));
      } else if (split.fileType.equals("xz")) {
        parser = new TrecKBAParser(split, getLocalBufferedInputStream(split));
      }else {
        throw new IOException("Unknown fileType: " + split.fileType
                + " for fileName: " + split.fileName);
      }
    } catch (EOFException ee) {
      System.err.printf("Found empty split %s. Skipping due to no content.", split.toString());
      return;
    }
    Document document;
    count = 0;

    while ((document = parser.nextDocument()) != null) {
      count++;
      if (documentCounter != null) {
        documentCounter.increment();
      }
    }
    if (source != null) {
      source.close();
    }

    KeyValuePair kvp = new KeyValuePair();
    kvp.key = split.fileName.getBytes();
    kvp.value = Utility.compressLong(count);
    processor.process(kvp);
  }

  public static boolean isParsable(String extension) {
    return extension.equals("html")
            || extension.equals("xml")
            || extension.equals("txt")
            || extension.equals("arc")
            || extension.equals("warc")
            || extension.equals("trectext")
            || extension.equals("trecweb")
            || extension.equals("twitter")
            || extension.equals("corpus")
            || extension.equals("wiki")
            || extension.equals("mbtei")
            || extension.equals("xz")
    		|| extension.equals("wex");
  }

  public BufferedReader getLocalBufferedReader(DocumentSplit split) throws IOException {
    BufferedReader br = getBufferedReader(split);
    source = br;
    return br;
  }

  public static BufferedReader getBufferedReader(DocumentSplit split) throws IOException {
    FileInputStream stream = StreamCreator.realInputStream(split.fileName);
    BufferedReader reader;

    if (split.isCompressed) {
      // Determine compression type
      if (split.fileName.endsWith("gz")) { // Gzip
        reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(stream)));
      } else if(split.fileName.endsWith("xz")) {
        reader = new BufferedReader(new InputStreamReader(new XZInputStream(stream,  10*1024)));
      } else if (split.fileName.endsWith("bz2")) { // BZip2
        BufferedInputStream bis = new BufferedInputStream(stream);
        //bzipHeaderCheck(bis);
        reader = new BufferedReader(new InputStreamReader(new BZip2CompressorInputStream(bis)));
      } else {
          throw new UnsupportedOperationException("Unsupported compressed File type! : " + split.fileName);
      }
    } else {
      reader = new BufferedReader(new InputStreamReader(stream));
    }
    return reader;
  }

  public BufferedInputStream getLocalBufferedInputStream(DocumentSplit split) throws IOException {
    BufferedInputStream bis = getBufferedInputStream(split);
    source = bis;
    return bis;
  }

  public static BufferedInputStream getBufferedInputStream(DocumentSplit split) throws IOException {
    FileInputStream fileStream = StreamCreator.realInputStream(split.fileName);
    BufferedInputStream stream;

    if (split.isCompressed) {
      // Determine compression algorithm
      if (split.fileName.endsWith("gz")) { // Gzip
        stream = new BufferedInputStream(new GZIPInputStream(fileStream));
      }  else if (split.fileName.endsWith("xz")) {
          stream = new BufferedInputStream(new XZInputStream(fileStream));
      } else { // bzip2
        BufferedInputStream bis = new BufferedInputStream(fileStream);
        stream = new BufferedInputStream(new BZip2CompressorInputStream(bis));
      }
    } else {
      stream = new BufferedInputStream(fileStream);
    }
    return stream;
  }
}
