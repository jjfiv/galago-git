// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.parse;

import org.lemurproject.galago.core.types.IndexSplit;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;
import org.lemurproject.galago.core.index.disk.DiskNameReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * From a specified indexPath input, split the count of documents in the index
 * into many IndexSplit records so that documents can be extracted from the 
 * index in a distributed manner.  This is the start stage of a Galago pipeline.
 *
 *
 * @author smh
 */

@Verified
@OutputClass (className = "org.lemurproject.galago.core.types.IndexSplit", order = {"+index", "+begId"})

public class IndexDocIdSplit implements ExNihiloSource<IndexSplit> {

  private static final Logger INDEXSPLITLOG = Logger.getLogger (IndexDocIdSplit.class.getSimpleName ());
  private Counter               indexSplitCounter;
  public  Processor<IndexSplit> processor;
  private TupleFlowParameters   parameters;


  public IndexDocIdSplit (TupleFlowParameters parameters) {
    this.parameters = parameters;
    this.indexSplitCounter = parameters.getCounter ("Index Parts Processed");
  }


  @Override
  public void run () throws IOException {

    // indexSplitBuffer stores the full list of index document splits to emit.
    ArrayList<IndexSplit> indexSplitBuffer = new ArrayList<>();
    Parameters conf = parameters.getJSON ();

    String indexPath = conf.getString ("indexPath");
    int splitPieces = conf.get ("distrib", 1);
    File indexFile = new File (indexPath);

    //- The index path should exist and be a directory.
    if (indexFile.exists () && indexFile.isDirectory ()) {

      //- Get doc count and then distribute the documents up as evenly as possible 
      //  across processors (create IndexSplit objects for each split).
      DiskNameReader dnr = new DiskNameReader (indexPath + File.separator + "names");

      if (dnr.getManifest ().getBoolean ("emptyIndexFile")) {
        INDEXSPLITLOG.warning ("Empty Names Index File.  Quitting.");
        return;
      }

      //- How many docs?
      long docCount = dnr.getManifest().get ("keyCount", 0);

      if (docCount == 0) {
        INDEXSPLITLOG.warning ("The index has no documents.  Quitting.");
        return;
      }

      INDEXSPLITLOG.warning ("Total docs in index: " + docCount);

      //- How many docs per split?
      //int docsPerSplit = (int)Math.floor (docCount/splitPieces);
      long docsPerSplit = docCount/splitPieces;
      long remainder = docCount % splitPieces;
      long stop = splitPieces;

      if (remainder > 0) {
        stop = splitPieces - 1;
      }

      long i=0L;
      long beginId=0L, endId=0L;
      for ( ; i<stop; i++) {
        beginId = i * docsPerSplit;
        endId = beginId + docsPerSplit - 1;
        IndexSplit indexSplit = new IndexSplit (indexPath, beginId, endId);

	//* DEBUG
	System.out.printf ("Adding index split \'%s-%d-%d\' %n", indexPath, beginId, endId);
	indexSplitBuffer.add (indexSplit);
      }

      if (remainder >  0L) {
        IndexSplit indexSplit = new IndexSplit (indexPath, beginId, endId);

	//* DEBUG
        System.out.printf ("Adding index split \'%s-%d-%d\' %n", indexPath, beginId, endId);
        indexSplitBuffer.add (indexSplit);
      }
    }
    else {
      throw new IOException("Couldn't find file/directory: \'" + indexPath + "\'");
    }

    // we now have an accurate count of emitted files / splits
    int totalIndexSplitCount = indexSplitBuffer.size();

    // now process each file
    for (IndexSplit indexSplit : indexSplitBuffer) {
      indexSplitCounter.increment ();

      //- IndexSplits off to the Sorter
      //* DEBUG
      System.out.println ("Passing index split to processor");
      processor.process (indexSplit);
    }

    processor.close ();
  }   //- end method run


  @Override
  public void setProcessor (Step processor) throws IncompatibleProcessorException {
    Linkage.link (this, processor);
  }


  public static void verify (TupleFlowParameters parameters, ErrorStore store) {

    String indexPath = null;

    if (!parameters.getJSON().containsKey ("indexPath")) {
      store.addError ("IndexPath is not defined.");
    }
    else {
      indexPath = parameters.getJSON().getString ("indexPath");
      File indexFile = new File (indexPath);

      if (!indexFile.exists()) {
        store.addError ("IndexPath \'" + indexPath + "\' does not exist.");
      }

      if (!indexFile.isDirectory()) {
        store.addError ("IndexPath \'" + indexPath + "\' is not a directory.");
      }
    }

  }  //- end method verify

}  //- end class IndexDocIdSplit
