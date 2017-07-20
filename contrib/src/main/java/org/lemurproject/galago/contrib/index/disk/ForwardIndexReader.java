// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.index.disk;

import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.contrib.parse.DocTermsInfo;
import org.lemurproject.galago.contrib.index.source.ForwardIndexSource;
import org.lemurproject.galago.contrib.index.disk.ForwardIndexSerializer;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.disk.DiskDataIterator;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.btree.BTreeReader;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 *
 * @author smh
 */
public class ForwardIndexReader extends KeyValueReader {

  public long docsInCollection;
  public long totalTermsInCollection;
  public long maxTermFreqInCollection;
  protected Parameters manifest;

  public ForwardIndexReader (String filename) throws FileNotFoundException, IOException {
    super (filename);
    this.manifest = this.reader.getManifest ();
    this.docsInCollection = this.getManifest ().get ("fwindexStatistics/docsInCollection", 0L);
    this.totalTermsInCollection = this.getManifest ().get ("fwindexStatistics/totalTermsInCollection", 0L);
    this.maxTermFreqInCollection = this.getManifest ().get ("fwindexStatistics/maxTermFreqInCollection", 0L);
  }


  public ForwardIndexReader (BTreeReader r) {
    super (r);
    this.manifest = this.reader.getManifest ();
    this.docsInCollection = this.getManifest ().getLong ("fwindexStatistics/docsInCollection");
    this.totalTermsInCollection = this.getManifest ().getLong ("fwindexStatistics/totalTermsInCollection");
    this.maxTermFreqInCollection = this.getManifest ().getLong ("fwindexStatistics/maxTermFreqInCollection");
  }


  public DocTermsInfo getForwardDocTerms (long document) throws IOException {
    byte[] valueBytes = reader.getValueBytes (Utility.fromLong(document));

    if ((valueBytes == null) || (valueBytes.length == 0)) {
      return null;
    } 
    else {
	return (DocTermsInfo)ForwardIndexSerializer.fromBytes (valueBytes);
    }
  }


  @Override
  public KeyIterator getIterator () throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("fwindex", new NodeType(DiskDataIterator.class));
    return types;
  }

  @Override
  public DiskDataIterator getIterator (Node node) throws IOException {
    if (node.getOperator ().equals ("fwindex")) {
      return new DiskDataIterator (new ForwardIndexSource (reader));
    } 
    else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }


  public static class KeyIterator extends KeyValueReader.KeyValueIterator {

    public KeyIterator (BTreeReader reader) throws IOException {
      super (reader);
    }


    @Override
    public String getKeyString () {
      return Long.toString (Utility.toLong (iterator.getKey()));
    }


    @Override
    //- Just returns the first term
    public String getValueString () {
      try {
        //byte[] valueBytes = ForwardIndexSerializer.getCurrentScore ());
        byte[] valueBytes = iterator.getValueBytes ();
        DocTermsInfo dti = ForwardIndexSerializer.fromBytes (valueBytes);
	ArrayList<String> termList = dti.getDocTerms ();

	if (termList.size () == 0) {
          return  null;
	}
	else {
          return termList.get (0);
	}
      }
      catch (IOException ioe) {
        throw new RuntimeException (ioe);
      }
    }


    public boolean skipToKey (long key) throws IOException {
      return skipToKey(Utility.fromLong (key));
    }


    public long getCurrentDocument () {
      return (long) Utility.toLong (iterator.getKey());
    }


    public DocTermsInfo getCurrentDocTermsInfo () throws IOException {
      byte[] valueBytes = iterator.getValueBytes ();
      if ((valueBytes == null) || (valueBytes.length == 0)) {
        return null;
      }
      else {
        return ForwardIndexSerializer.fromBytes (valueBytes);
      }
    }

    public ForwardIndexSource getValueSource () throws IOException {
      return new ForwardIndexSource (reader);
    }


    @Override
    public DiskDataIterator getValueIterator () throws IOException {
      return new DiskDataIterator (new ForwardIndexSource (reader));
    }

  }  //- end inner class KeyIterator

}  //- end class ForwardIndexReader
