/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.KeyListReader;
//import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
//import org.lemurproject.galago.core.retrieval.iterator.DataIterator;
//import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.tools.Arguments;

import java.lang.*;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.io.PrintStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;

import java.util.logging.Logger;


/**
 *
 * @author smh
 */
public class DumpDocTermsFn extends AppFunction {

  public static final Logger logger = Logger.getLogger("DumpDocTermsFn");


  public static void main (String[] args) throws Exception {
    (new DumpDocTermsFn()).run(Arguments.parse(args), System.out);
  }  //- end main


  @Override
  public String getName() {
    return "dump-doc-terms";
  }


  @Override
  public String getHelpString() {
    return "galago dump-doc-terms --index=<index_part> --iidList=<list_of_iids> --eidList=<list_of_eids>\n\n"
            + "  Dumps the term statistics from various inverted list data for one or more specified\n"
            + "  documents.  Term statistics are output in CSV format.\n\n"
            + "  Also provided are internal and external document IDs, maxTF, term count and total word\n"
            + "  statistics for each document listed.\n\n"
            + "  Internal and External document IDs are specified using the appropriate command line flag.\n"
            + "  One must use a trailing comma if only one IID is specified so the processor knows it is a string\n"
            + "  and not a number.  If spaces are placed between listed IDs, the entire ID string should be quoted.\n\n";
  }


  @Override
  public void run (Parameters p, PrintStream output) throws Exception {

    // Parameter check
    assert (p.isString ("index")) : "dump-doc-terms requires an 'index' paramter.";
    assert (p.containsKey("iidList") || p.containsKey("eidList")) : "dump-doc-terms requires an 'iidList' and/or 'eidList' parameter.";


    String eid = null;
    long   iid = 0L;


    //- Must have an index root defined.
    if (!p.containsKey ("index") || (!p.containsKey("iidList") && !p.containsKey("eidList"))) {
      System.out.println ("No index defined or missing a doc ID list.");
      System.out.println (this.getHelpString());
      return;
    }

    // Ensure print to file
    if (p.isString("outputFile")) {
      boolean append = p.get ("appendFile", false);
      PrintStream out = new PrintStream (new BufferedOutputStream (
               new FileOutputStream(p.getString("outputFile"), append)), true,  "UTF-8");
    }

    // This is the postings index string
    String index = p.getString ("index");

    // But for getting internal and external doc IDs, we need the index root
    int lastSlash = index.lastIndexOf ('/');
    String indexRoot = index.substring (0, lastSlash);

    Map<Long, String> iid2eidTM = new TreeMap<>();

    //- Figure out External IDs from provided Internal ID's if prsent
    if (p.containsKey ("iidList")) {
      String idsStr = p.getString ("iidList");
      String[] toks = idsStr.split ("[ ,]+");
    
      int len = toks.length;

      if (len > 0) {
        DiskNameReader dnr = new DiskNameReader (indexRoot + "/names");

        for (String id : toks) {
          iid = Long.parseLong (id);
          eid = dnr.getDocumentName (iid);

          if (!iid2eidTM.containsKey (iid)) {
            iid2eidTM.put (iid, eid);
          }
        }

        dnr.close();
      }
    }

    //- Do the same thing as above for any provided External IDs
    if (p.containsKey ("eidList")) {
      long badIDCounter = 0;

      String idsStr = p.getString ("eidList");
      String[] toks = idsStr.split ("[ ,]+");
    
      int len = toks.length;

      if (len > 0) {
        DiskNameReverseReader dnrr = new DiskNameReverseReader (indexRoot + "/names.reverse");
  
        for (String eidStr : toks) {
          iid = dnrr.getDocumentIdentifier (eidStr);

          if (iid == -1) {
            iid = -1 - badIDCounter;
            badIDCounter++;
          }

          if (!iid2eidTM.containsKey (iid)) {
            iid2eidTM.put (iid, eidStr);
          }
	     }

        dnrr.close();
      }
    }

    //- Open postings index and get stats for each target doc
    Map<Long, ArrayList<String>> iid2statslistsHM = new HashMap<>();
    Map<Long, Integer>    iid2tcHM = new HashMap<>();  // doc term counts
    Map<Long, Integer>    iid2mtfHM = new HashMap<>(); // doc max term freq
    Map<Long, Integer>    iid2twHM = new HashMap<>();  // doc total words (indexed)
    ArrayList<String>     currentStatsList = null;
    long              currentIID = 0L;
    String            currentEID = null;
    int               currentMaxTF = 0;
    int               currentTermCount = 0;
    int               currentTotalWords = 0;

    IndexPartReader ipr = DiskIndex.openIndexPart (index);

    if (ipr.getManifest().get ("emptyIndexFile", false)) {
      System.out.println ("Empty Index File.  Quitting.");
      return;
    }

    KeyIterator iterator = ipr.getIterator ();

    // if we have a key-list index 
    if (KeyListReader.class.isAssignableFrom (ipr.getClass())) {

      // Iterate through all term keys paying attention only to entries from
      // docs of interest to us.
      while (!iterator.isDone()) {
        BaseIterator vIter = iterator.getValueIterator ();
        ScoringContext sc = new ScoringContext ();

        while (!vIter.isDone()) {
          sc.document = vIter.currentCandidate();
          long currentDocID = sc.document;

          // Get stats if we are interested in this doc
          if (iid2eidTM.containsKey (currentDocID)) {
            String statsStr = vIter.getValueString (sc);
            String[] postingParts =  statsStr.split (",");
            int tf = postingParts.length - 2;
              
            // Add term stats
            if (!iid2statslistsHM.containsKey (currentDocID)) {
              currentStatsList = new ArrayList<>();
            }
            else {
              currentStatsList = iid2statslistsHM.get (currentDocID);
	    }

            currentStatsList.add (statsStr);
            iid2statslistsHM.put (currentDocID, currentStatsList);

            // Update Max TF
            if (iid2mtfHM.containsKey(currentDocID)) {
              currentMaxTF = iid2mtfHM.get(currentDocID);
            }
            else {
              currentMaxTF = 0;
            }

            if (tf > currentMaxTF) {
              iid2mtfHM.put (currentDocID, tf);
            }

            // Update total word countd
            if (iid2twHM.containsKey(currentDocID)) {
              currentTotalWords = iid2twHM.get(currentDocID);
            }
            else {
              currentTotalWords = 0;
            }
            currentTotalWords += tf;
            iid2twHM.put (currentDocID, currentTotalWords);

            // Update term count
            if (iid2tcHM.containsKey(currentDocID)) {
              currentTermCount = iid2tcHM.get(currentDocID);
            }
            else {
              currentTermCount = 0;
            }
            currentTermCount ++;
            iid2tcHM.put (currentDocID, currentTermCount);
	  }

          vIter.movePast (vIter.currentCandidate());
        }
        iterator.nextKey();
      }
    }

    /* NOT RELEVANT TO POSTINGS TASK but left in "just in case"
     *   A KeyValueReader is used on indexes such as names and reverse.names so shouldn't
     *   be needed for posting statistics.  Testing showed this code was not used for
     *   postings, extents and field index files.
    // otherwise we could have a key-value index
    else if (KeyValueReader.class.isAssignableFrom (ipr.getClass())) {
      while (!iterator.isDone ()) {
        String valuesStr = (iterator.getKeyString() + "," + iterator.getValueString());
        System.out.println ("Values String: " + valuesStr);
        String[] postingParts =  valuesStr.split (",");
        int freq = postingParts.length - 2;
        String termStr = postingParts[0];
        String docIdStr = postingParts[1];
        System.out.println ("Term: " + termStr + "\t DocID: " + docIdStr + "\t Freq: " + freq);

        iterator.nextKey();
      }
    }
    */
    else {
      System.out.println ("Unable to read index as a key-list or a key-value reader.");
    }

    ipr.close ();

    // Dump the info we've accumulated
    for (Map.Entry<Long, String> entry : iid2eidTM.entrySet()) {
      iid = entry.getKey();
      eid = entry.getValue();

      if (iid < 0) {
        System.out.printf ("Doc: --- [%s] \t Doc ID does not exist %n", eid);
      }
      else if (eid == null) {
        System.out.printf ("Doc: %d [ --- ] \t Doc ID does not exist %n", iid);
      }
      else {
        int termCount = iid2tcHM.get (iid);
        int totalWords = iid2twHM.get (iid);
        int maxTF = iid2mtfHM.get (iid);

        System.out.printf ("Doc: %d [%s] \t Term Count: %d \t Total Words: %d \t Max TF: %d %n",
                iid, eid, termCount, totalWords, maxTF);

        currentStatsList = iid2statslistsHM.get (iid);

        for (String statsStr : currentStatsList) {
          System.out.println ("\t " + statsStr);
        }

        System.out.println ("\n");
      }
    }

    System.out.println ("\nDONE\n");

  }  //- end run

}  //- end class
