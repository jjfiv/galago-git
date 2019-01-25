// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.index.disk;

import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.core.index.KeyValueWriter;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.btree.GenericElement;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.IOException;

/**
 * Writes the forward index file 
 * 
 * @author smh
 */

@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})

public class ForwardIndexWriter extends KeyValueWriter<KeyValuePair> {

  TupleFlowParameters tfParameters;    
  int lastDocument = -1;
  long docsInCollection;
  long totalTermsInCollection;
  long maxTermFreqInCollection;
  Counter written;


  public ForwardIndexWriter (TupleFlowParameters parameters) throws IOException {

    super (parameters, "Fwindex Docs Written");
    this.tfParameters = parameters;
    
    Parameters manifest = writer.getManifest ();
    manifest.set ("writerClass", ForwardIndexWriter.class.getName ());
    manifest.set ("readerClass", ForwardIndexReader.class.getName ());

    this.written = parameters.getCounter ("Fwindex Docs Written");

    this.docsInCollection = 0L;
    this.totalTermsInCollection = 0L;
    this.maxTermFreqInCollection = 0L;
  }


  @Override
  public GenericElement prepare (KeyValuePair kvp) throws IOException {
    assert ((lastDocument < 0) || (lastDocument < Utility.toLong (kvp.key))) : 
            "ForwardIndexWriter keys must be unique and in sorted order.";

    //- Update collection wide statistics.  Unfortunately, need to de-serialize the KVP value to get current
    //  document values for statistics.  Probably a better way to do this, but didn't want to create another
    //  tupleflow data stream just for this information.  Can't seem to get the information between reduce and
    //  writer classes, which is likely the better way to accomplish this.
    this.docsInCollection++;
    this.totalTermsInCollection += this.tfParameters.getJSON().getLong ("fwindexStatistics/totalTermsInCollection");
    this.maxTermFreqInCollection = Math.max (this.maxTermFreqInCollection, 
                                             this.tfParameters.getJSON().getLong ("fwindexStatistics/maxTermFreqInCollection"));

    //- Write out the term info for the doc.
    GenericElement element = new GenericElement (kvp.key, kvp.value);
    written.increment ();
    return element;
  }


  //- Write stats to the manifest 
  @Override
  public void close () throws IOException {
    Parameters manifest = writer.getManifest ();

    manifest.set ("fwindexStatistics/docsInCollection", this.docsInCollection);
    manifest.set ("fwindexStatistics/totalTermsInCollection", this.totalTermsInCollection);
    manifest.set ("fwindexStatistics/maxTermFreqInCollection", this.maxTermFreqInCollection);
	   
    super.close ();
  }


  public static void verify (TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON ().isString ("filename")) {
      store.addError ("KeyValueWriters require a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON ().getString ("filename");
    Verification.requireWriteableFile (index, store);
  }

}  //- end class ForwardIndexWriter

