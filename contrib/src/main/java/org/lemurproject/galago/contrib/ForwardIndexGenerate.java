/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.parse;

import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;
import org.lemurproject.galago.core.types.IndexSplit;
import org.lemurproject.galago.core.types.DocumentTermInfo;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.logging.Logger;
import java.io.File;
import java.io.IOException;


/**
 * Open the specified index part (corpus) for reading.  Obtain the doc count
 * for the index using the names part.  Iterate through each document internal
 * ID to extract the document terms (using the parser/tokenzier used to build
 * the index).  Get the provided begin and end offset for each term and its
 * begin position.  The end position is assumed to be bPos+1 since these are 
 * single token terms.
 *
 * Output the docid, term, bPos, ePos, bOffs and eOffset.
 *
 * This output is to be sorted by docid/term/bPos and then fed to the
 * ForwardIndexReduce stage for writing out to a forward index.
 *
 * @author smh
 */


@Verified
@InputClass  (className = "org.lemurproject.galago.core.types.IndexSplit", order = {"+index", "+begId"})
@OutputClass (className = "org.lemurproject.galago.core.types.DocumentTermInfo", order = {"+docid", "+term", "+begPos"})

public class ForwardIndexGenerate extends StandardStep <IndexSplit, DocumentTermInfo> {

  private Counter             fwindexGenerateCounter;
  private TupleFlowParameters parameters;
  private String              indexRoot;
  private static final Logger FWINDEXGENERATELOG = Logger.getLogger (ForwardIndexGenerate.class.getSimpleName ());

  private Parameters     docParams;
  private DiskNameReader diskNameReader;
  private long           docCount;
  private Retrieval      retrieval;

    
  public ForwardIndexGenerate (TupleFlowParameters parameters) {
    this.parameters = parameters;
    this.fwindexGenerateCounter = parameters.getCounter ("FWIndex Terms Generated");
    //this.parameters = parameters.getJSON ();
    //this.indexRoot = parameters.getJSON ().get ("indexPath", "/home/harding/work/idx/ap89_fields.idx");
    this.indexRoot = parameters.getJSON ().getString ("indexPath");

    this.docParams = Parameters.create ();
    this.docParams.set ("index", indexRoot);
    this.docParams.set ("tokenize", true);
    this.docParams.set ("text", false);
    this.docParams.set ("metadata", false);

    try {
      //- Get doc count and iterate over all docs in the split
      this.diskNameReader = new DiskNameReader (this.indexRoot + File.separator +"names");

      if (diskNameReader.getManifest ().getBoolean ("emptyIndexFile")) {
        FWINDEXGENERATELOG.warning ("Empty Names Index File.  Quitting.");
        return;
      }

      //- How many docs?
      this.docCount = this.diskNameReader.getManifest().get ("keyCount", 0);
      //System.err.println ("Total Docs: " + docCount);

      this.retrieval = RetrievalFactory.instance (this.indexRoot, Parameters.create ());
      assert this.retrieval.getAvailableParts ().containsKey ("corpus") : "Index does not contain a corpus part.";
    }
    catch (Exception ex) {
	System.out.println ("Exception: " + ex.toString ());
    }
  }


  public void process (IndexSplit indexSplit) throws IOException {
    long   iid = -1L;
    String eid = "";
    String docName = "";
    String docText = "";

    String indexRoot = indexSplit.index;
    long   begId = indexSplit.begId;
    long   endId = indexSplit.endId;
      
    int docCount = -1;
    int docTermCount = -1;

    Map<String, String> docMetadata = null;
    List<String> docTerms = null;
    List<Integer> docTermCharBegin = null;
    List<Integer> docTermCharEnd = null;
    //List<Tag> docTags = null;

    //- Get Document terms only
    FWINDEXGENERATELOG.info ("Processing Index Split.  Index: " + indexRoot +
                        "   Docs " + begId + " to " + endId);

    try {
      long lastDocID = -1;

      for (long id=begId; id<=endId; id++) {
        eid = this.diskNameReader.getDocumentName (id);
        this.docParams.set ("id", eid);

        DocumentComponents dc = new DocumentComponents (this.docParams);
        Document document = retrieval.getDocument (eid, dc);

        if (document != null) {
          docName         = document.name;
          docText         = document.text;
          docTerms         = document.terms;
          docTermCharBegin = document.termCharBegin;
          docTermCharEnd   = document.termCharEnd;
          //docTags          = document.tags;

          //- Term info  
          if (docTerms != null) {
            docTermCount = docTerms.size ();
            //System.out.printf ("Doc: %s[%d]  Term Info[%d] %n", eid, id, docTermCount);

            for (int i=0; i<docTermCount; i++) {
              String t = docTerms.get(i);
              int bOffs = docTermCharBegin.get(i);
              int eOffs = docTermCharEnd.get(i);
              int bPos = i;

              //- Single position terms for now
              int ePos = i + 1;

              //- Output format is IID (%d), term(%s), begin_pos(%d), endPos(%d), beginOffs(%d), end_offs(%d)
              //System.out.printf ("%d %s %d %d %d %d%n", id, t, bPos, ePos, bOffs, eOffs);
              DocumentTermInfo dti = new DocumentTermInfo (id, t, bPos, ePos, bOffs, eOffs);
              processor.process (dti);
              fwindexGenerateCounter.increment ();        
            }
          }
	  else {
            FWINDEXGENERATELOG.warning ("Document " + eid + " has no terms in index " + indexRoot + ".");
	  }
	}
        else {
          FWINDEXGENERATELOG.warning ("Document " + eid + " does not exist in index " + indexRoot + ".");
        }
      }

      //diskNameReader.close();
    }
    catch (Exception ex) {
      FWINDEXGENERATELOG.severe ("*** Exception ***");
      ex.printStackTrace();
    }

  }  //- end  method process

}  //- end class ForwardIndexGenerate
