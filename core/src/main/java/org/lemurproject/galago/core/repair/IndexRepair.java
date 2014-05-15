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
  public static void createNamesReverseFromNames(String names, String outputName) throws IOException, IncompatibleProcessorException {
    // input
    DiskNameReader namesReader = new DiskNameReader(names);

    Parameters oldP = namesReader.getManifest();
    Parameters newP = Parameters.parseArray("filename", outputName,
      "blockSize", oldP.getLong("blockSize"),
      "maxKeySize", oldP.getLong("maxKeySize"));
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
      ndd.identifier = name;
      ndd.number = identifier;
      pipe.process(ndd);
      if(count++ % 10000 == 0) {
        System.err.println("# converted: "+count+" names");
        pipe.flush();
      }
      source.movePast(identifier);
    }
    pipe.close();
  }
}
