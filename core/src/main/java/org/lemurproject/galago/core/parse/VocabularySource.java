// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.HashSet;
import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;

/**
 *
 * @author irmarc
 */
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})
public class VocabularySource implements ExNihiloSource<KeyValuePair> {

  Counter vocabCounter;
  Counter skipCounter;
  public Processor<KeyValuePair> processor;
  TupleFlowParameters parameters;
  BTreeReader reader;
  BTreeReader.BTreeIterator iterator;
  HashSet<String> inclusions = null;
  HashSet<String> exclusions = null;

  public VocabularySource(TupleFlowParameters parameters) throws Exception {
    String partPath = parameters.getJSON().getString("filename");
    reader = BTreeFactory.getBTreeReader(partPath);
    vocabCounter = parameters.getCounter("terms read");
    skipCounter = parameters.getCounter("terms skipped");
    iterator = reader.getIterator();

    // Look for queries to base the extraction
    Parameters p = parameters.getJSON();
    inclusions = new HashSet<String>();
    if (p.isString("includefile")) {
      File f = new File(p.getString("includefile"));
      if (f.exists()) {
        System.err.printf("Opening inclusion file: %s\n", f.getCanonicalPath());
        inclusions = Utility.readFileToStringSet(f);
      }
    } else if (p.isList("include")) {
      List<String> inc = p.getList("include");
      for (String s : inc) {
        inclusions.add(s);
      }
    }

    exclusions = new HashSet<String>();
    if (p.isString("excludefile")) {
      File f = new File(p.getString("excludefile"));
      if (f.exists()) {
        System.err.printf("Opening exclusion file: %s\n", f.getCanonicalPath());
        exclusions = Utility.readFileToStringSet(f);
      }
    } else if (p.isList("exclude")) {
      List<String> inc = p.getList("exclude");
      for (String s : inc) {
        exclusions.add(s);
      }
    }
  }

  public void run() throws IOException {
    KeyValuePair kvp;
    int number = 0;
    while (!iterator.isDone()) {

      // Filter if we need to
      if (!inclusions.isEmpty() || !exclusions.isEmpty()) {
        String s = Utility.toString(iterator.getKey());
        if (inclusions.contains(s) == false) {
          iterator.nextKey();
          if (skipCounter != null) {
            skipCounter.increment();
          }
          continue;
        }

        if (exclusions.contains(s) == true) {
          iterator.nextKey();
          if (skipCounter != null) {
            skipCounter.increment();
          }
          continue;
        }
      }

      kvp = new KeyValuePair();
      kvp.key = iterator.getKey();
      kvp.value = Utility.fromInt(number);
      processor.process(kvp);
      if (vocabCounter != null) {
        vocabCounter.increment();
      }
      number++;
      iterator.nextKey();
    }
    processor.close();
    reader.close();
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    FileSource.verify(parameters, handler);
    String partPath = parameters.getJSON().getString("filename");
    try {
      if (!BTreeFactory.isBTree(partPath)) {
        handler.addError(partPath + " is not an index file.");
      }
    } catch (FileNotFoundException fnfe) {
      handler.addError(partPath + " could not be found.");
    } catch (IOException ioe) {
      handler.addError("Generic IO error: " + ioe.getMessage());
    }
  }
}
