package org.lemurproject.galago.core.repair;

import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseWriter;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Sorter;

import java.io.IOException;

/**
 * @author jfoley.
 */
public class IndexRepair {
  public static void createNamesReverseFromNames(String names, String outputName, Parameters opts) throws IOException, IncompatibleProcessorException {
    // input
    DiskNameReader namesReader = new DiskNameReader(names);

    Parameters oldP = namesReader.getManifest();
    Parameters newP = new Parameters();
    newP.put("filename", outputName);

    if(opts.get("keepBlockSize", true)) {
      newP.put("blockSize", oldP.getLong("blockSize"));
    }
    int flushSize = (int) opts.get("flushSize", 10000);

    DiskNameReverseWriter reverseWriter = new DiskNameReverseWriter(new FakeParameters(newP));

    // build a tupleflow pipeline to resort the data
    Sorter<NumberedDocumentData> pipe = new Sorter<NumberedDocumentData>(new NumberedDocumentData.IdentifierOrder());
    pipe.setProcessor(reverseWriter);

    // iterate over the disknamesource
    DataSource<String> source = namesReader.getSource();
    long count = 0;
    while(!source.isDone()) {
      long identifier = source.currentCandidate();
      String name = source.data(identifier);
      NumberedDocumentData ndd = new NumberedDocumentData();
      ndd.fieldList = "";
      ndd.url = "";
      ndd.textLength = 0;
      ndd.identifier = name;
      ndd.number = identifier;
      pipe.process(ndd);
      if(count % flushSize == 0) {
        System.err.println("# converted: "+count+" names");
        pipe.flush();
      }
      count++;
      source.movePast(identifier);
    }
    pipe.close();
  }
}
