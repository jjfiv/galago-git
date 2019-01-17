/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.stem.KrovetzStemmer;
import org.lemurproject.galago.core.parse.stem.Porter2Stemmer;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.tools.Arguments;

import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;



/**
 * @author smh
 */
public class DumpDocTermsFn extends AppFunction {

    public static final Logger logger = Logger.getLogger("DumpDocTermsFn");


    public static void main(String[] args) throws Exception {
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
    public void run(Parameters p, PrintStream output) throws Exception {

        // Parameter check
        assert (p.isString("index")) : "dump-doc-terms requires an 'index' paramter.";
        assert (p.containsKey("iidList") || p.containsKey("eidList")) : "dump-doc-terms requires an 'iidList' and/or 'eidList' parameter.";


        String eid = null;
        long iid = 0L;


        //- Must have an index root defined.
        if (!p.containsKey("index") || (!p.containsKey("iidList") && !p.containsKey("eidList"))) {
            output.println("No index defined or missing a doc ID list.");
            output.println(this.getHelpString());
            return;
        }
/*
    // Support print to file
    if (p.isString("outputFile")) {
      boolean append = p.get ("appendFile", false);
      PrintStream out = new PrintStream (new BufferedOutputStream (
               new FileOutputStream(p.getString("outputFile"), append)), true,  "UTF-8");
    }
*/

        // This is the postings index string
        String index = p.getString("index");

        Stemmer  stemmer = null;
        // check if they want stemmed terms
        if (index.endsWith("porter")){
            stemmer = new Porter2Stemmer();
        }
        if (index.endsWith("krovetz")){
            stemmer = new KrovetzStemmer();
        }

        // But for getting internal and external doc IDs, we need the index root
        int lastSlash = index.lastIndexOf(File.separator);
        String indexRoot = index.substring(0, lastSlash);

        Map<Long, String> iid2eidTM = new TreeMap<>();

        //- Figure out External IDs from provided Internal ID's if prsent
        if (p.containsKey("iidList")) {
            String idsStr = p.getString("iidList");
            String[] toks = idsStr.split("[ ,]+");

            int len = toks.length;

            if (len > 0) {
                DiskNameReader dnr = new DiskNameReader(indexRoot + File.separator + "names");

                for (String id : toks) {
                    iid = Long.parseLong(id);
                    eid = dnr.getDocumentName(iid);

                    if (!iid2eidTM.containsKey(iid)) {
                        iid2eidTM.put(iid, eid);
                    }
                }

                dnr.close();
            }
        }

        //- Do the same thing as above for any provided External IDs
        if (p.containsKey("eidList")) {
            long badIDCounter = 0;

            String idsStr = p.getString("eidList");
            String[] toks = idsStr.split("[ ,]+");

            int len = toks.length;

            if (len > 0) {
                DiskNameReverseReader dnrr = new DiskNameReverseReader(indexRoot + File.separator + "names.reverse");

                for (String eidStr : toks) {
                    iid = dnrr.getDocumentIdentifier(eidStr);

                    if (iid == -1) {
                        iid = -1 - badIDCounter;
                        badIDCounter++;
                    }

                    if (!iid2eidTM.containsKey(iid)) {
                        iid2eidTM.put(iid, eidStr);
                    }
                }

                dnrr.close();
            }
        }

        LocalRetrieval retrieval = new LocalRetrieval(indexRoot, Parameters.create());
        // Dump the info we've accumulated.
        for (Map.Entry<Long, String> entry : iid2eidTM.entrySet()) {
            iid = entry.getKey();
            eid = entry.getValue();

            // If a provided IID had no corresponding EID, the IID is what was provided and the
            // EID is null.
            if (iid < 0) {
                output.println("Doc: ---\t[" + eid + "]\tDoc ID does not exist");
            }
            // If the IID was < 0, then the provided EID did not exist so print out the provided
            // IID only.
            else if (eid == null) {
                output.println("Doc: " + iid + "\t[ --- ]\tDoc ID does not exist");
            } else {

                // MCZ - old code iterated through the entire postings list and
                // collected statistics - not very efficient of you're using this
                // on a collection such as ClueWeb. So, modifying to get them from
                // the actual document.
                Document doc = retrieval.getDocument(eid, new Document.DocumentComponents(true, true, true));
                Map<String, List<Integer>> termPos = doc.getTermPositions(stemmer);

                StringBuilder sb = new StringBuilder();
                long maxTF = 0;

                for (Map.Entry<String, List<Integer>> entry2 : termPos.entrySet()) {
                    String term = entry2.getKey();
                    List<Integer> pos = entry2.getValue();
                    sb.append(term + "," + iid);
                    if (pos.size() > maxTF) {
                        maxTF = pos.size();
                    }
                    for (Integer i : pos) {
                        sb.append("," + i);
                    }
                    sb.append("\n");
                }

                output.println("Doc: " + iid + " [" + eid
                        + "]\tTerm Count: " + termPos.size()
                        + "\tTotal Words: " + doc.terms.size()
                        + "\tMax TF: " + maxTF);

                output.println(sb.toString());

            }
        }
    }  //- end run

}  //- end class
