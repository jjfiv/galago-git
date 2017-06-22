/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.index.disk;

import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.contrib.parse.DocTermsInfo;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.Parameters;

import org.lemurproject.galago.core.retrieval.iterator.disk.DiskDataIterator;

import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;
import java.util.Iterator;
import java.lang.StringBuffer;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;


/**
 *
 * Create three documents, then build a DocTermsInfo object for
 * each document, write the object to a forward index file, then
 * read the object content back for each document confirming the
 * information read back matches the information written.
 * 
 * @author smh
 */

public class ForwardIndexWriterTest {

  //- Some fake positions for terms in three docs.  (note end positions will be
  //  these positions + 1).
  private static final int[][] positions = new int[][] {
                                             new int[] {0, 1, 2, 3},
                                             new int[] {0, 1, 2, 3},
                                             new int[] {0, 1, 2}
                                           };

  //- Some fake begin offsets for terms in three docs
  private static final int[][] begoffs = new int[][] {
                                           new int[] {0, 5, 10, 16},
                                           new int[] {0, 5, 12, 17},
                                           new int[] {0, 5, 15}
                                         };

  //- Fake end offsets for terms in three docs
  private static final int[][] endoffs_increments = new int[][] {
                                           new int[] {3, 7, 13, 20},
                                           new int[] {3, 9, 15, 20},
                                           new int[] {3, 10, 19}
                                         };

  //- Fake terms in three docs
  private static final String[][] terms = new String[][] {
                                            new String[] {"now", "is", "the", "time"},
                                            new String[] {"the", "time", "is", "now"},
                                            new String[] {"to", "party", "time"}
                                          };

  //- Termf freqs in three docs
  private static final int[][] tf = new int[][] {
                                      new int[] {1, 1, 1, 1},
                                      new int[] {1, 1, 1, 1},
                                      new int[] {1, 1, 1}
                                    };

  //- Unique term counts in three docs
  private static final int[] uniqueTermCnt = new int[] {4, 4, 3};

  //- Max TF in three docs
  private static final int[] maxTF = new int[] {1, 1, 1};

  //- Collection wide stats
  public long docsInCollection = 0L;
  public long totalTermsInCollection = 0L;
  public long maxTermFreqInCollection = 0L;


  @Test
  public void testWriteThenRead () throws Exception {

    Map<Long, DocTermsInfo> docTermsInfoHM = new HashMap<>();
    Map<Long, DocTermsInfo> trueData = new HashMap<>();

    File tmp = File.createTempFile ("forwardIndexWriterTest", ".tmp");

    if (!tmp.exists () ) {
      tmp.createNewFile ();
    }
    String fileName = tmp.getAbsolutePath ();

    Parameters p = Parameters.create ();
    p.set ("filename", fileName);
    p.set ("fwindexStatistics/docsInCollection", 0L);
    p.set ("fwindexStatistics/totalTermsInCollection", 0L);
    p.set ("fwindexStatistics/maxTermFreqInCollection", 0L);

    try {
      ForwardIndexWriter writer = new ForwardIndexWriter (new FakeParameters (p));

      //- Create DocTermsInfo object for each doc and store them
      for (int i=0; i<terms.length; i++) {   // terms.length is the doc count [3]
        DocTermsInfo dti = new DocTermsInfo ((long)i);
        dti.docUniqueTermCount = uniqueTermCnt[i];
        dti.docTermCount = terms[i].length;
        dti.docMaxTermFreq = maxTF[i];

        //- Terms in each doc
        for (int j=0; j<terms[i].length; j++) {
          DocTermsInfo.TermInfo ti = new DocTermsInfo.TermInfo ();
          ti.term = terms[i][j];
          ti.termFreq = tf[i][j];
 
          int begPos = positions[i][j];
          //int endPos = begPos + 1;
          int begOffs = begoffs[i][j];
          //int endOffs = begOffs + endoffs_increments[i][j];
          //DocTermsInfo.PositionInfo pi = new DocTermsInfo.PositionInfo (begPos, endPos, begOffs, endOffs);
          DocTermsInfo.PositionInfo pi = new DocTermsInfo.PositionInfo (begPos, begOffs);
          ti.positionInfoList.add (pi);

          dti.termsInfoHM.put (ti.term, ti);
        }

        trueData.put ((long)i, dti);

        byte[] keyBytes = Utility.fromLong ((long)i);
        byte[] valueBytes = ForwardIndexSerializer.toBytes (dti);
        KeyValuePair kvp = new KeyValuePair (keyBytes, valueBytes);
        writer.process (kvp);

        //- Update manifest with collection wide statistics
        docsInCollection++;
        totalTermsInCollection += dti.docTermCount;
        maxTermFreqInCollection = Math.max (maxTermFreqInCollection, dti.docMaxTermFreq);

        //- Update collections stats via param values
        p.set ("fwindexStatistics/docsInCollection", docsInCollection);
        p.set ("fwindexStatistics/totalTermsInCollection", totalTermsInCollection);
        p.set ("fwindexStatistics/maxTermFreqInCollection", maxTermFreqInCollection);
      }

      writer.close ();

      //- See if we can read back an accurate manifest
      ForwardIndexReader reader = new ForwardIndexReader (fileName);
      Parameters manifest = reader.getManifest ();
      String manifestString = manifest.toString ();
      String manifestStringTruth =
              "{ \"blockCount\" : 1 , \"blockSize\" : 16383 , " +
              "\"emptyIndexFile\" : false , " +
              "\"filename\" : \"" + fileName + "\" , " +
              "\"fwindexStatistics/docsInCollection\" : 3 , " +
              "\"fwindexStatistics/maxTermFreqInCollection\" : 1 , " +
              "\"fwindexStatistics/totalTermsInCollection\" : 12 , " +
              "\"keyCount\" : 3 , \"maxKeySize\" : 16383 , " +
              "\"readerClass\" : \"org.lemurproject.galago.contrib.index.disk.ForwardIndexReader\" , " +
              "\"writerClass\" : \"org.lemurproject.galago.contrib.index.disk.ForwardIndexWriter\" }";

      assertEquals ("Written and expected manifests differ.", manifestString, manifestStringTruth);

      //- Make sure we can read back from the index the same data we stored
      ForwardIndexReader.KeyIterator ki = reader.getIterator ();

      while (!ki.isDone ()) {
	  long id = Long.parseLong (ki.getKeyString ());
	  DocTermsInfo dti = (DocTermsInfo)ForwardIndexSerializer.fromBytes (ki.getValueBytes());
	  DocTermsInfo dtiTrue = (DocTermsInfo)trueData.get (id);
	  assertEquals (dti.docUniqueTermCount, dtiTrue.docUniqueTermCount);
	  assertEquals (dti.docTermCount, dtiTrue.docTermCount);
	  assertEquals (dti.docMaxTermFreq, dtiTrue.docMaxTermFreq);

	  Map<String, DocTermsInfo.TermInfo> tiHM = dti.termsInfoHM;
	  Map<String, DocTermsInfo.TermInfo> trueTiHM = dtiTrue.termsInfoHM;
	  assertEquals (tiHM.size (), trueTiHM.size ());

	  Set ks = tiHM.entrySet ();
	  Set trueKs = trueTiHM.entrySet ();
	  assertEquals (ks.size (), trueKs.size ());
	  assertTrue ("Ks Set CONTAINS all True Keys", trueKs.containsAll (trueKs));
	  assertFalse ("Ks Set DOES NOT CONTAIN all True Keys", !trueKs.containsAll (trueKs));

	  Iterator itr = ks.iterator ();
	  while (itr.hasNext ()) {
            Map.Entry me = (Map.Entry)itr.next();
            String term = (String)me.getKey ();
            DocTermsInfo.TermInfo ti = (DocTermsInfo.TermInfo)me.getValue ();
            ArrayList<DocTermsInfo.PositionInfo> piList = ti.positionInfoList;
            DocTermsInfo.TermInfo trueTi = trueTiHM.get (term);
            ArrayList<DocTermsInfo.PositionInfo> truePiList = trueTi.positionInfoList;

            //- Create output strings from true and indexed data and compare them
            StringBuffer sb = new StringBuffer ();
            StringBuffer trueSb = new StringBuffer ();

            String tiStr = DocTermsInfo.toString (ti);
            String trueTiStr = DocTermsInfo.toString (trueTi);

            sb.append ("Doc: " + id + "  " + term + " --> " + tiStr);
            trueSb.append ("Doc: " + id + "  " + term + " --> " + trueTiStr);

            String sbStr = sb.toString ();
            String trueSbStr = trueSb.toString ();

            assertEquals (piList.size (), truePiList.size ());			
            assertEquals (sbStr, trueSbStr);

            //System.out.println ("Index ==> " + sbStr + "\nTruth  ==> " + trueSbStr + "\nPASSED");
	  }

	  ki.nextKey ();
      }

      reader.close ();
    }
    catch (Exception ex) {
      ex.printStackTrace ();
    }
    finally {
      assertTrue (tmp.delete ());
    }

  }  //- end main

}  //- end class ForwardIndexWriter
