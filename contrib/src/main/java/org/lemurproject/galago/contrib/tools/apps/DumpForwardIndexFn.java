/*
 *  BSD License (http://lemurproject.org/galago-license)
 */

//package org.lemurproject.galago.contrib.tools.apps;
package org.lemurproject.galago.contrib.tools.apps;

import org.lemurproject.galago.contrib.parse.DocTermsInfo;
import org.lemurproject.galago.contrib.parse.DocTermsInfo.TermInfo;
import org.lemurproject.galago.contrib.parse.DocTermsInfo.PositionInfo;
import org.lemurproject.galago.contrib.index.disk.ForwardIndexReader;
import org.lemurproject.galago.contrib.index.disk.ForwardIndexSerializer;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.tools.Arguments;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Set;
import java.util.Iterator;
import java.util.logging.Logger;
import java.lang.StringBuffer;
import java.io.PrintStream;


/**
 *
 * Dump contents of a Forward Index part of a specified index
 * 
 * Output can be the entire index or selected documents specified by internal or
 * external ID.  Output can also be terms only, terms with position or offset
 * information as well as document or collection statistics.
 * 
 * @author smh
 */

public class DumpForwardIndexFn extends AppFunction {

  public static final Logger logger = Logger.getLogger ("DumpForwardIndexFn");


  public static void main (String[] args) throws Exception {
    (new DumpForwardIndexFn ()).run (Arguments.parse (args), System.out);
  }


  @Override
  public String getName () {
    return "dump-forward-index";
  }


  @Override
  public String getHelpString () {
    return "galago dump-forward-index \n"
            + "Dumps terms from a specified list of internal or external doc IDs.\n\n"

            + "--index=<index_name>                   [The forward index part name is \"fwindex\" and will be appended to the\n"
            + "                                        index name provided.] \n"
            + "--iidList=<list_of_iids> \n" 
            + "--eidList=<list_of_eids> \n"
            + "--terms_only=true|false                [Output terms only.  If true, include external doc ID along with internal \n"
            + "                                        ID with each term.                      default true] \n"
            + "--term_positions=true|false            [Output position info for each term.     Default = false] \n"
            + "--term_offsets=true|false              [Output file offset info for each term.  Default = false] \n"
            + "--include_collection_stats=true|false  [Output doc count, total terms and max tf value for collection.  Default = false] \n"
            + "--include_doc_stats=true|false         [Output internal and external doc IDs, unique termcount, \n"
            + "                                        total terms and max tf value for document.    Default = false] \n\n"

            + "  One must use a trailing comma if only one IID is specified so the processor knows it is a string\n"
            + "  and not a number.  If spaces are placed between listed IDs, the entire ID string should be quoted.\n\n";
  }


  @Override
  public void run (Parameters p, PrintStream output) throws Exception {

    //- Must have an index root defined. 
    assert (p.isString ("index")) : "dump-forward-index requires an 'index' paramter.";
    assert (p.containsKey ("iidList") ||
            p.containsKey ("eidList") ||
            p.containsKey ("collection_stats_only")) : "dump-forward-index requires an iidList, eidList and/or collection_stats_only parameter.";

    boolean termsOnly = true;
    boolean termPositions = false;
    boolean termOffsets = false;
    boolean iterateOverALL = false;
    boolean collectionStats = false;
    boolean documentStats = false;

    long docsInCollection = 0L;
    long totalTermsInCollection = 0L;
    long maxTermFreqInCollection = 0L;


    if (p.containsKey ("term_positions")) {
      termPositions = p.getBoolean ("term_positions");
    }

    if (p.containsKey ("term_offsets")) {
      termOffsets = p.getBoolean ("term_offsets");
    }

    if (p.containsKey ("terms_only")) {
      termsOnly = p.getBoolean ("terms_only");

      if (termsOnly) {
        termPositions = false;
        termOffsets = false;
      }
      else {
        termPositions = true;
        termOffsets = true;
      }
    }

    if (p.containsKey ("include_collection_stats")) {
      collectionStats = p.getBoolean ("include_collection_stats");
    }

    if (p.containsKey ("include_doc_stats")) {
      documentStats = p.getBoolean ("include_doc_stats");
    }

    /*
    // Support print to file
    if (p.isString("outputFile")) {
      boolean append = p.get ("appendFile", false);
      PrintStream out = new PrintStream (new BufferedOutputStream (
                            new FileOutputStream(p.getString("outputFile"), append)), true,  "UTF-8");
    }
    */

    // This is the forward index root string
    String indexRoot = p.getString ("index");

    // The forward index part
    String fwIndexFileName = indexRoot + File.separator + "fwindex";

    Map <Long, String> iid2eidTM = new TreeMap<>();
    //ArrayList<Long> iidsList = new ArrayList<>();
    TreeSet<Long> iidsTS = new TreeSet<> ();

    String idsStr = "";
    String[] toks = null;
    int len = 0;
    long iid = 0L;

    try {
      //- See if we can read back an accurate manifest
      ForwardIndexReader reader = new ForwardIndexReader (fwIndexFileName);
      Parameters manifest = reader.getManifest ();
      if (manifest.get ("emptyIndexFile", false)) {
        System.out.println ("Empty index file.");
        return;
      }

      //- Collection wide stats
      docsInCollection = manifest.getLong ("fwindexStatistics/docsInCollection");

      if (collectionStats) {
        totalTermsInCollection = manifest.getLong ("fwindexStatistics/totalTermsInCollection");
        maxTermFreqInCollection = manifest.getLong ("fwindexStatistics/maxTermFreqInCollection");
        System.out.println ("Collection Stats ==> doc count: " + docsInCollection +
                "\t total terms: " + totalTermsInCollection +
                "\t max term frequency: " + maxTermFreqInCollection);
      }

      //- Generate a list of IIDs
      if (p.containsKey ("iidList")) {
        idsStr = p.getString ("iidList");
        toks = idsStr.split ("[ ,]+");

        len = toks.length;

        //- If the IIDList contains a single element "ALL", then we iterate over
        //  the entire index figuring out EIDs later.

        if (len == 1 && toks[0].equalsIgnoreCase ("all")) {
          iterateOverALL = true;
        }
        else if (len > 0) {
          //- Add IIDs to a TreeSet to account for possible duplicates from EID list

          for (String id : toks) {
            iid = Long.parseLong (id);
            iidsTS.add (iid);
          }
        }
      }

      //- Convert any provided EIDs to IIDs and add them to the IIDs list
      if (p.containsKey ("eidList") && !iterateOverALL) {
        idsStr = p.getString ("eidList");
        toks = idsStr.split ("[ ,]+");

        len = toks.length;

        //- Handle EID ALL specifier
        if (len == 1 && toks[0].equalsIgnoreCase ("all")) {
          iterateOverALL = true;
        }
        else if (len > 0) {
          try {
            DiskNameReverseReader dnrr = new DiskNameReverseReader (indexRoot + File.separator + "names.reverse");

            for (String eidStr : toks) {
              iid = dnrr.getDocumentIdentifier (eidStr);

              if (iid == -1L) {
                System.out.println ("Failure finding doc EID \'" + eidStr + "\'.  Ignoring.");
                continue;
              }

              iidsTS.add (iid);
            }

            dnrr.close();
          }
          catch (Exception ex) {
            System.out.println ("Exception accessing Reverse Name index part.");
            System.out.println ("Exception: " + ex.toString ());
	  }
        }
      }

      //- Will need to get EIDs for each IID sought
      DiskNameReader dnr = new DiskNameReader (indexRoot + File.separator + "names");

      //- Go through the index printing out terms and extents for specified documents.
      //  Output is in form  <doc_id>, <term>, <begPos>, <endPos>, <begOffs>, <endOffs>
      //  If positions or offsets are not wanted, they will be represented by empty space
      //  with comma separators.
      //  E.g. for full information "0, alpha, 5, 6, 102, 106" but for only term information
      //  "0, alpha, , , , "
      DocTermsInfo dti = null;
      TermInfo ti = null;
      long start = 0L;
      long end = docsInCollection;

      ForwardIndexReader.KeyIterator ki = reader.getIterator ();
      Long[] idArray = null;

      //- If iterating over a list, get the number of items on the list as the end
      if (! iterateOverALL) {
        end = iidsTS.size ();
        idArray = iidsTS.toArray (new Long[0]);
      }

      //- Iterate over iids count
      for (int i = 0; i < end; i++) {

        if (iterateOverALL) {
          iid = (long) i;
        } else {
          iid = idArray[i];
        }

        byte[] iidBytes = Utility.fromLong (iid);
        //ki.skipToKey (iidBytes);
        // iid = Utility.fromLong (iidsList.get (i));
        ki.findKey (iidBytes);
        //ki.skipToKey (iid);

        dti = (DocTermsInfo) ForwardIndexSerializer.fromBytes (ki.getValueBytes ());

        //- Find EID for IID
        String eidStr = null;

        try {
          eidStr = dnr.getDocumentName (iid);
        } catch (Exception ex) {
          System.out.println ("Exception accessing Name index part.");
          System.out.println ("Exception: " + ex.toString ());
        }

        //- Print the stats for the document if required
        if (documentStats) {
          System.out.println ("Doc ID: " + dti.docid + "[" + eidStr + "]" +
                  "\tUnique Terms: " + dti.docUniqueTermCount +
                  "\tTerm Count: " + dti.docTermCount +
                  "\t" +
                  "\tMax TF : " + dti.docMaxTermFreq);
        }

        Map<String, org.lemurproject.galago.contrib.parse.DocTermsInfo.TermInfo> tiHM = dti.termsInfoHM;
        Set ks = tiHM.entrySet ();
        Iterator itr = ks.iterator ();

        StringBuffer sb = new StringBuffer ();
        while (itr.hasNext ()) {
          Map.Entry me = (Map.Entry) itr.next ();
          String term = (String) me.getKey ();

          //- All output conditions include the doc ID and a term.  If we are printing terms only,
	  //  include the external Doc ID along with the internal one.
	  String baseOutputStr = null;

	  if (termsOnly) {
            baseOutputStr = new String (iid + "[" + eidStr + "], " + term + " ");
	  }
	  else {
	    baseOutputStr = new String (iid + ", " + term + " ");
	  }

          //- Clear the string buffer for a new output line
          if (sb.length () > 0) {
            //sb.delete (0, sb.length ());
            sb.setLength (0);
          }

          sb.append (baseOutputStr);

          if (termPositions || termOffsets) {
            ti = (TermInfo) me.getValue ();
            ArrayList<org.lemurproject.galago.contrib.parse.DocTermsInfo.PositionInfo> piList = ti.positionInfoList;

            for (PositionInfo pi : piList) {
              //- Create position output strings.  End positions and offsets are
              //  calculted rather than stored to save index space.
              if (termPositions) {
                //sb.append (" [" + pi.begPos + ", " + pi.endPos);
                sb.append (" [" + pi.begPos + ", " + (pi.begPos + 1) + ", ");
              } else {
                sb.append (" [ , , ");
              }

              if (termOffsets) {
                //sb.append (pi.begOffs + ", " + pi.endOffs + "]");
                sb.append (pi.begOffs + ", " + (pi.begOffs + term.length ()) + "]");
              } else {
                sb.append (" , ]");
              }
            }
          }

          System.out.println (sb.toString ());
          ki.nextKey ();
        }
      }

      reader.close ();
      dnr.close ();
    }
    catch(Exception ex){
      ex.printStackTrace ();
    }

  }  //- end run

}  //- end class DumpForwardIndexFn
