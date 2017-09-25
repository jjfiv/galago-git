/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.utility.tools.Arguments;
import org.lemurproject.galago.utility.tools.AppFunction;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;

//- Stanford NER Stuff
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.*;
import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.sequences.SeqClassifierFlags;
import edu.stanford.nlp.sequences.DocumentReaderAndWriter;
import edu.stanford.nlp.util.Triple;

//import java.lang.ClassNotFoundException;
import java.lang.StringBuilder;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.Iterator;


/**
 * Generate document annotations using Stanford NER entity extraction
 * on a specified document or list of documents from file or existing
 * index.  Up to three entity types can be extracted from texts.
 *
 * Entity types are LOCATIONS, PERSONS and ORGANIZATIONS.
 *
 * @author smh
 */

public class AnnotationsGenerateFn extends AppFunction {

  private static final String background = SeqClassifierFlags.DEFAULT_BACKGROUND_SYMBOL;
  public static final Logger logger = Logger.getLogger("AnnotationsGenerateFn");
  public static PrintStream output;

  public static void main (String[] args) throws Exception {
    (new AnnotationsGenerateFn ()).run (Arguments.parse (args), System.out);
  }  //- end main


  @Override
  public String getName () {
    return "annotations-generate";
  }
    

  @Override
  public String getHelpString () {
    return "galago generate-annotations --indexPath=<index_postings> OR --inputFiles=<input_files>\n"
	           + "  --iidList=<list_of_iids> --eidList=<list_of_eids>\n"
	           + "  --annotationTypes=<person || organizations || location || comma separated list of these>\n\n"

                   + "  Can build one or more annotation index parts of type PERSON, ORGANIZATION or \n"
                   + "  LOCATION entities using the default Stanford Named Entity Resolution library.\n\n"

                   + "  Generates document annotations as entity types along with their statistics from an index or file input\n"
	           + "  for one or more specified documents.  Term statistics are output in CSV format.\n\n"
                   + "  Also provided are internal and external document IDs, maxTF, term count and total word\n"
                   + "  statistics for each document listed.\n\n"
	
	           + "  Inputs are either a document text file (which will be appropriately parsed) or an existing index.\n"
                   + "  Internal and External document IDs are specified using the appropriate command line flag.\n"
                   + "  One must use a trailing comma if only one IID is specified so the processor knows it is a string\n"
                   + "  and not a number.  If spaces are placed between listed IDs, the entire ID string should be quoted.\n\n";

  }  //- end method getHelpString


  public void run (Parameters p, PrintStream output) throws Exception {

    String annotationTypeString = null;
    String[] annotationTypes = null;
    String index = null;
    HashSet<String> annotationTypesHS = new HashSet<> ();
    
    //- Support print to file                                                                                                                            
    if (p.isString ("outputFile")) {                                                                                                                     
      boolean append = p.get ("appendFile", false);                                                                                                     
      PrintStream out = new PrintStream (new BufferedOutputStream (                                                                                     
                         new FileOutputStream (p.getString ("outputFile"), append)), true,  "UTF-8");                                                         }                                                                                                                                                   
      
    //- Parameters check
    if (!p.containsKey ("annotationTypes")) {
      output.println ("generate-annotations requires an \'annotationTypes\' parameter.");
      output.println (this.getHelpString ());
      return;
    }
    else {
      annotationTypeString = p.getString ("annotationTypes");
      annotationTypes = annotationTypeString.split ("[ ,]+");

      //- Ignore unknown annotation types
      for (String type : annotationTypes) {
        if (!type.equals ("person") && !type.equals ("location") && !type.equals ("organization")) {
          output.printf ("Unknown annotation type ^%s^.  Must be person, organization or location. Ignoring type. %n",
                         type);
	  continue;
	}
	
	annotationTypesHS.add (type.toLowerCase ());
      }

      //- Bail out if no recognized types defined
      if (annotationTypesHS.size () == 0) {
        output.println ("No recognized annotation types defined.  Quitting.");
        return;
      }
    }

    //- Set up NER Extraction
    String serializedClassifier = null;
    AbstractSequenceClassifier<CoreLabel> classifier = null;

    try {
      serializedClassifier = "edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz";
      classifier = CRFClassifier.getClassifier (serializedClassifier);
    }
    catch (ClassNotFoundException cnfe) {
      output.println ("Class not found exception initializing NER extractions.");
      output.println ("Exception: " + cnfe.toString ());
    }

    //- Handle annotation generation from an existing index
    if (p.containsKey ("indexPath")) {
      String indexPath = null;
	
      try {
        if (!p.containsKey ("eidList") && !p.containsKey ("iidList")) {
          output.println ("generate-annotations requires an \'iidList\' and/or \'eidList\' parameter when using an existing index.");
          output.println (this.getHelpString ());
          return;
        }

        //- Do we have a valid index?
        indexPath = (new File (p.getString ("indexPath"))).getAbsolutePath ();
        File indexFile = new File (indexPath);
        assert (indexFile.isDirectory ());

        /*
        if (!indexFile.isFile () && !indexFile.isDirectory ()) {
          throw new IOException ("Couldn't find file/directory ^" + indexPath + "^");
        }
        */
      
        /*	    
        Handle a trailing slash index part specification
        int lastSlash = indexPath.lastIndexOf (File.separator);
        if (lastSlash == indexPath.length) {
        }
        */

        //- Store internal to external IDs in a TreeMap
        Map<Long, String> iid2eidTM = new TreeMap<>();
        long docCount = 0L;

        //- Figure out External IDs from provided Internal ID's if prsent
        if (p.containsKey ("iidList")) {
          String idsStr = p.getString ("iidList");
          String[] iidsList = idsStr.split ("[ ,]+");
          int len = iidsList.length;

          if (len > 0) {
            //- Open a disk name reader to get doc count and for internal to external ID conversions
            DiskNameReader dnr = new DiskNameReader (indexPath + File.separator + "names");

            if (dnr.getManifest ().getBoolean ("emptyIndexFile")) {
              output.println ("Empty Names Index File.  Quitting.");
              return;
            }

            //- How many docs?
            docCount = dnr.getManifest().get ("keyCount", 0);
            output.println ("Index ^" + indexPath + "^ contains " + docCount + " total docs");
		    
            for (String id : iidsList) {
              long iid = Long.parseLong (id);
              String eid = dnr.getDocumentName (iid);

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
          String[] eidsList = idsStr.split ("[ ,]+");
          int len = eidsList.length;
	    
          if (len > 0) {
            DiskNameReverseReader dnrr = new DiskNameReverseReader (
                                               indexPath + File.separator + "names.reverse");

            for (String eidStr : eidsList) {
              long iid = dnrr.getDocumentIdentifier (eidStr);

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
      
        //- Get Document text only
        Parameters docParams = Parameters.create ();
        docParams.set ("index", indexPath);
        docParams.set ("tokenize", false);
        docParams.set ("text", true);
        docParams.set ("metadata", false);

        DocumentComponents dc = null;
        Retrieval r = null;

        dc = new DocumentComponents (docParams);
        r = RetrievalFactory.instance (indexPath, Parameters.create ());
        assert r.getAvailableParts ().containsKey ("corpus") : "Index does not contain a corpus part.";

        long lastDocID = -1;
      
        //- Do extraction over provided document IDs
        Set tmSet = iid2eidTM.entrySet ();
        Iterator it = tmSet.iterator ();

        while (it.hasNext ()) {
          Map.Entry me = (Map.Entry)it.next ();
          long iid = ((Long)me.getKey ()).longValue ();
          String eid = (String)me.getValue ();
	
          Document document = r.getDocument (eid, dc);

          if (document == null) {
            output.printf ("Document [%d]  %s does not exist %n", iid, eid);
            continue;
          }
          else {
            String docName                 = document.name;
            String docText                 = document.text;
	      
            //- Extract entities from text (using Stanford NER API)
            if (docText == null) {
              output.printf ("Document [%d]  %s has no text in index ^%s^. %n", iid, eid, indexPath);
              continue;
            }
            else {
              StringBuilder entitySB = null;
              int begPos = 0;
              int endPos = 0;
              int begOffs = 0;
              int endOffs = 0;
              int tokenCnt = 0;
              String lastEntityType = null;

              output.printf ("Doc [%d]  %s %n", iid, docName);

              //- Go through the text entity extractions
              for (List<CoreLabel> lcl : classifier.classify (docText)) {
                for (CoreLabel cl : lcl) {
                  String entityType = (cl.get(CoreAnnotations.AnswerAnnotation.class)).toLowerCase ();

                  //- Only pay attention to entity types specified
                  //if (! annotationTypesHS.contains (entityType)) {
                  //  continue;
                  //}

                  //- Which entity value to use?
                  //String entity = (cl.get(CoreAnnotations.TextAnnotation.class)).toLowerCase().toLowerCase();
		            String entity = (cl.get(CoreAnnotations.TextAnnotation.class)).toLowerCase();
                  //String entity = cl.get(CoreAnnotations.OriginalTextAnnotation.class).toLowerCase();
                  //String entity = cl.get(CoreAnnotations.ValueAnnotation.class).toLowerCase();

                  //- Useful for assembling entity tokens into an entity extraction (multi-token)
                  String before = cl.get(CoreAnnotations.BeforeAnnotation.class);
                  String after = cl.get(CoreAnnotations.AfterAnnotation.class);

                  //- Position info
                  int currentBegPos = Integer.parseInt (cl.get(CoreAnnotations.PositionAnnotation.class));
                  int currentEndPos = currentBegPos + 1;

                  int currentBegOffs = cl.get(CoreAnnotations.CharacterOffsetBeginAnnotation.class);
                  int currentEndOffs = cl.get(CoreAnnotations.CharacterOffsetEndAnnotation.class);

                  if (lastEntityType == null) {
                    if (background.equals(entityType)) {
                      //- Not building a current entity and type is ignored (skipping "O" answers)
                      //  but still need to update begin/end positions since they are cumulative.
                      begPos = tokenCnt++;    //currentBegPos;
                      endPos = tokenCnt;      //currentEndPos;
                      lastEntityType = entityType;
                      continue;
                    }
                    else {
                      //- A non-background entity type.  Start building a new entity string
                      entitySB = new StringBuilder (entity);
                      begPos = tokenCnt++;   //currentBegPos;
                      endPos = tokenCnt;     //currentEndPos;
                      begOffs = currentBegOffs;
                      endOffs = currentEndOffs;
                      lastEntityType = entityType;
                      continue;
                    }
                  }
                  else if (entityType.equals(lastEntityType)) {
                    if (!background.equals(entityType)) {

                      //- Not a background entity type so add to current entity
                      entitySB.append ("_");
                      entitySB.append (entity);
 
                      //- Update position info
                      endPos = tokenCnt++;    //currentEndPos;
                      endOffs = currentEndOffs;
                      continue;
                    }
                    else {
                      //- Entity type is background.  Finish off any existing entity and output
                      if (entitySB != null) {

                        //- Print out only if entity is of requested type
                        //if (annotationTypes.contains (lastEntityType)) {
			if (annotationTypesHS.contains (lastEntityType)) {
                          output.printf ("%d %s %s %d %d %d %d%n",
                                         iid, lastEntityType, entitySB.toString(),
                                         begPos, endPos, begOffs, endOffs);
			}
                      }  

                      //- Start new entity.
                      begPos = tokenCnt++;    //currentBegPos;
                      endPos = tokenCnt;      //currentEndPos;
                      begOffs = 0;
                      endOffs = 0;
                      entitySB = null;
                      lastEntityType = entityType;
                    }
                  }
                  else if (!entityType.equals (lastEntityType)) {
                    //- A new entity type.  Output existing entity if one is being built, then
                    //  start a new entity if it is not type "O"
                    if (!lastEntityType.equals (background)) {
                      //- Print entity only if it is requested type
                      //if (annotationTypes.contains (lastEntityType)) {
		      if (annotationTypesHS.contains (lastEntityType)) {	
                        output.printf ("%d %s %s %d %d %d %d%n",
                                       iid, lastEntityType, entitySB.toString(),
                                       begPos, endPos, begOffs, endOffs);
		      }
                    }

                    if (!entityType.equals (background)) {
                      entitySB = new StringBuilder (entity);
                      begPos = tokenCnt++;    //currentBegPos;
                      endPos = tokenCnt;      //currentEndPos;
                      begOffs = currentBegOffs;
                      endOffs = currentEndOffs;
                    }
                    else {
                      entitySB = null;
                      begPos = tokenCnt++;    //currentBegPos;
                      endPos = tokenCnt;      //currentEndPos;
                      begOffs = 0;
                      endOffs = 0;
                    }

                    lastEntityType = entityType;
                  }  //- end else if
                }  //- end for each core label
	      } //- end core classify
	    }  //- end else if docText
	  }  //- end else document exists
	}  //- end while more IDs
      }  //- end try
      catch (Exception ex) {
        output.println ("Exception extracting annotations from index ^" + indexPath + "^");
        output.println ("Exception: " + ex.toString ());
        throw new RuntimeException (ex);
      }
    }  //- end annotations from docs in index

    //- Extract annotations from provided document text files
    else {
      output.println ("*** EXTRACT ANNOTATIONS FOR DOCS IN TEXT FILE ***");
    }
  }  //- end method run

}  //- end class AnnotationsGenerateFn

