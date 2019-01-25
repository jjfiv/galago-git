// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.index.source;

import org.lemurproject.galago.contrib.parse.DocTermsInfo;
import org.lemurproject.galago.contrib.index.disk.ForwardIndexSerializer;
import org.lemurproject.galago.core.index.source.BTreeKeySource;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.btree.BTreeReader;

import java.io.IOException;
//import java.util.logging.Level;
//import java.util.logging.Logger;

/**
 *
 * @author smh
 */

public class ForwardIndexSource extends BTreeKeySource implements DataSource {
//public class ForwardIndexSource extends BTreeKeySource implements DiskSource {

  public ForwardIndexSource (BTreeReader rdr) throws IOException {
    super (rdr);
    final Parameters manifest = btreeReader.getManifest ();
  }


  @Override
  public boolean hasAllCandidates () {
    //return true;
    return false;
  }


  @Override
  public String key () {
    //return ByteUtil.toString (key);
    return "fwindex";
  }


  @Override
  public boolean hasMatch (long id) {
    return true;
  }


  @Override
  //- Return the DocTermsInfo object for the doc ID
  public DocTermsInfo data  (long id) {  //throws IOException {

    try {
      if (currentCandidate () == id) {
        byte[] valueBytes = btreeIter.getValueBytes ();

        //- Get the DocTermsInfo object for the value bytes
        DocTermsInfo dti = (DocTermsInfo)ForwardIndexSerializer.fromBytes (valueBytes);
        
        //- Return the first term.  A bit awkward getting some entry without
        //  knowing the key.
	//Map.Entry<String, String> entry = Map.entrySet ().iterator ().next ();
        //String term = (String)entry.key;
        //return term;

        return dti;
      }
      else {
        return null;
      }
    } 
    catch (Exception ex) {
      //throw new IOException (ex);
      ex.printStackTrace ();
      return null;
    }
  }

}  //- end class ForwardIndexSource
