// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.lemurproject.galago.core.eval.Eval;
import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.IndexPartModifier;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader.DocumentIterator;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.core.index.merge.MergeIndex;
import org.lemurproject.galago.core.learning.LearnQueryParameters;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.FileOrderedReader;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.JobExecutor;
import org.mortbay.jetty.Server;

/**
 * @author sjh, irmarc, trevor
 */
public class App {

  // this interface for each function
  public static abstract class AppFunction {

    public abstract String getHelpString();

    public abstract void run(Parameters p, PrintStream output) throws Exception;

    public void run(String[] args, PrintStream output) throws Exception {
      Parameters p = new Parameters();

      if (args.length == 1) {
        output.print(this.getHelpString());
        return;

      } else if (args.length > 1) {
        p = new Parameters(Utility.subarray(args, 1));
        // don't want to wipe an existing parameter:
      }

//      if ((args.length > 0) && (!p.containsKey("command"))) {
//        p.set("command", Utility.join(args, " "));
//      }

      run(p, output);
    }
  }
  /**
   * function selection and processing
   */
  static protected HashMap<String, AppFunction> appFunctions = new HashMap();

  static {
    //
    appFunctions.put("chain-jobs", new ChainJobs());  

    // build functions
    appFunctions.put("build", new BuildIndex());
    appFunctions.put("build-special", new BuildSpecialPart());
    appFunctions.put("build-topdocs", new BuildTopDocsFn());
    appFunctions.put("build-window", new BuildWindowIndex());
    appFunctions.put("make-corpus", new MakeCorpusFn());
    appFunctions.put("build-entity-corpus", new BuildEntityCorpus());
    appFunctions.put("merge-index", new MergeIndex());
    appFunctions.put("subcollection", new BuildSubCollection());
    appFunctions.put("overwrite-manifest", new OverwriteManifest());
    appFunctions.put("stemmer-conflation", new BuildStemmerConflation());
    appFunctions.put("link-indexes", new IndexLinker());
    appFunctions.put("build-topics", new TopicIndexBuilder());
    appFunctions.put("build-pictures", new BuildPictureStore()); 
    appFunctions.put("build-dates", new BuildWordDateIndex());

    // background functions
    appFunctions.put("build-background", new BuildBackground());
    appFunctions.put("install-background", new BuildBackground());

    // search functions
    appFunctions.put("batch-search", new BatchSearch());
    appFunctions.put("search", new SearchFn());

    // eval 
    appFunctions.put("eval", new Eval());

    // learning
    appFunctions.put("learning", new LearnQueryParameters());

    // dump functions
    appFunctions.put("dump-connection", new DumpConnectionFn());
    appFunctions.put("dump-corpus", new DumpCorpusFn());
    appFunctions.put("dump-index", new DumpIndexFn());
    appFunctions.put("dump-keys", new DumpKeysFn());
    appFunctions.put("dump-keyvalue", new DumpKeyValueFn()); // -- should be implemented in dump-index
    appFunctions.put("dump-modifier", new DumpModifierFn());
    appFunctions.put("dump-index-manifest", new DumpIndexManifestFn());

    // corpus + index querying
    appFunctions.put("doc", new DocFn());
    appFunctions.put("doc-id", new DocIdFn());
    appFunctions.put("xcount", new XCountFn());
    appFunctions.put("doccount", new XDocCountFn());

    // help function
    appFunctions.put("help", new HelpFn());
  }

  /*
   * Eval function
   */
  public static void main(String[] args) throws Exception {
    App.run(args);
  }

  public static void run(String[] args) throws Exception {
    run(args, System.out);
  }

  public static void run(String[] args, PrintStream out) throws Exception {
    String fn = "help";
    if (args.length > 0 && appFunctions.containsKey(args[0])) {
      fn = args[0];
    }
    appFunctions.get(fn).run(args, out);
  }

  public static void run(String fn, Parameters p, PrintStream out) throws Exception {
    appFunctions.get(fn).run(p, out);


  }

  /**
   * Function implementations - in alphbetical order
   */
  private static class BuildTopDocsFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago build-topdocs --index=<index> --part=<part> [--size=<size>] [--minLength=<minlength>]\n\n"
              + "  Constructs topdoc lists consisting of <size> documents,\n"
              + "  and only for lists longer than <minlength>. Note that\n"
              + "  <index> needs to point an index, while <part> is the part to scan.\n";
    }

    @Override
    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length < 3) {
        output.println(getHelpString());
        return;
      }

      Parameters p = new Parameters(Utility.subarray(args, 1));
      assert (p.isString("index"));
      assert (p.isString("part"));
      // assert(p.isLong("size"));
      // assert(p.isLong("minLength"));
      run(p, output);
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      BuildTopDocs build = new BuildTopDocs();
      Job job = build.getIndexJob(p);
      runTupleFlowJob(job, p, output);
    }
  }
    
  private static class ChainJobs extends AppFunction {
    @Override
    public String getHelpString() {
      return "galago chain-jobs <input parameter file>\n\n"
	  + " Runs multiple jobs in a chain. Looks for the\n"
	  + " 'jobs' element in the parameter file. If the\n"
	  + " value is an array, the jobs are run in serial.\n"
	  + " If the value is a map, the jobs are runs as separate\n"
	  + " child threads. Each inner job should have 'command'"
	  + " and 'parameters' field.";
    }

    @Override
    public void run(Parameters jobs, PrintStream output) throws Exception {
	if (jobs.isList("jobs")) {
	    List<Parameters> serialJobs = (List<Parameters>) jobs.getAsList("jobs");
	    for (Parameters jobStep : serialJobs) {
                if (jobStep.get("active", true)) {
                  Parameters stepParameters = findJobParameters(jobStep);
                  String cmdName = jobStep.get("command", "none");
                  if (!appFunctions.containsKey(cmdName)) {
                    output.printf("Couldn't find command %s.\n", cmdName);
                    return;
                  }
		  output.printf("Executing command: %s\n", cmdName);
                  App.run(cmdName, stepParameters, output);
              }
	    }
	} else if (jobs.isMap("jobs")) {
	    output.printf("Not implemented yet. Sorry.\n");	    
	}
    }

    public Parameters findJobParameters(Parameters step) throws IOException {
	if (step.isString("parameters")) {
	    File parameterPath = new File(step.getString("parameters"));
	    if (!parameterPath.exists()) {
		throw new IOException(String.format("Unable to locate parameter file '%s'\n",
						    step.getString("parameters")));
	    }
	    return Parameters.parse(parameterPath);
	} else if (step.isMap("parameters")) {
	    return step.getMap("parameters");
	} else {
	    throw new RuntimeException("No acceptable parameters found.");
	}
    }
  }

  private static class DocFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago doc [--help] --index=<index> --id=<identifier> [format parameters]\n\n"
              + "  Prints the full text of the document named by <identifier>.\n"
              + "  The document is retrieved from a Corpus file named corpus."
              + "  <index> must contain a corpus structure.";
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      if (p.get("help", false)) {
        output.println(getHelpString());
        return;
      }
      String indexPath = p.getString("index");
      String identifier = p.getString("id");
      Retrieval r = RetrievalFactory.instance(indexPath, new Parameters());
      assert r.getAvailableParts().containsKey("corpus") : "Index does not contain a corpus part.";
      Document document = r.getDocument(identifier, p);
      if (document != null) {
	  output.println(document.toString());
      } else {
        output.println("Document " + identifier + " does not exist in index " + indexPath + ".");
      }
    }
  }

  private static class DocIdFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago doc-id <names.reverse> <identifier>\n"
              + "  Prints the internal document number of the document named by <identifier>.\n";
    }

    @Override
    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 2) {
        output.println(getHelpString());
        return;
      }
      String indexPath = args[1];
      String identifier = args[2];
      DiskNameReverseReader reader = new DiskNameReverseReader(indexPath);
      int docNum = reader.getDocumentIdentifier(identifier);
      output.println(docNum);
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      String indexPath = p.getString("indexPath");
      String identifier = p.getString("identifier");
      run(new String[]{"", indexPath, identifier}, output);
    }
  }

  private static class DocNameFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago doc-name <names> <internal-number>\n"
              + "  Prints the external document identifier of the document <internal-number>.\n";
    }

    @Override
    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 2) {
        output.println(getHelpString());
        return;
      }
      String indexPath = args[1];
      String identifier = args[2];
      DiskNameReader reader = new DiskNameReader(indexPath);
      String docIdentifier = reader.getDocumentName(Integer.parseInt(identifier));
      output.println(docIdentifier);
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      String indexPath = p.getString("indexPath");
      String identifier = p.getString("identifier");
      run(new String[]{"", indexPath, identifier}, output);
    }
  }

  private static class DumpConnectionFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago dump-connection <connection-file>\n\n"
              + "  Dumps tuples from a Galago TupleFlow connection file in \n"
              + "  CSV format.  This can be useful for debugging strange problems \n"
              + "  in a TupleFlow execution.\n";
    }

    @Override
    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }
      FileOrderedReader reader = new FileOrderedReader(args[1]);
      Object o;
      while ((o = reader.read()) != null) {
        output.println(o);
      }
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      String connectionPath = p.getString("connectionPath");
      run(new String[]{"", connectionPath}, output);
    }
  }

  private static class DumpCorpusFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago dump-corpus --path=<corpus> [limit fields]\n\n"
              + " Dumps all documents from a corpus file to stdout.\n"
              + " Limits (all boolean) include:\n pseudo tags terms metadata"
              + " text\n";
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      DocumentReader reader = new CorpusReader(p.getString("path"));
      if (reader.getManifest().get("emptyIndexFile", false)) {
        output.println("Empty Corpus.");
        return;
      }

      DocumentReader.DocumentIterator iterator = (DocumentIterator) reader.getIterator();
      while (!iterator.isDone()) {
        output.println("#IDENTIFIER: " + iterator.getKeyString());
        Document document = iterator.getDocument(p);
        output.println("#METADATA");
        for (Entry<String, String> entry : document.metadata.entrySet()) {
          output.println(entry.getKey() + "," + entry.getValue());
        }
        output.println("#TEXT");
        output.println(document.text);
        iterator.nextKey();
      }
      reader.close();
    }
  }

  private static class DumpIndexFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago dump-index <index-part>\n\n"
              + "  Dumps inverted list data from any index file in a StructuredIndex\n"
              + "  (That is, any index that has a readerClass that's a subclass of\n"
              + "  StructuredIndexPartReader).  Output is in CSV format.\n";
    }

    @Override
    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      IndexPartReader reader = DiskIndex.openIndexPart(args[1]);

      if (reader.getManifest().get("emptyIndexFile", false)) {
        output.println("Empty Index File.");
        return;
      }

      KeyIterator iterator = reader.getIterator();

      // if we have a key-list index
      if (KeyListReader.class.isAssignableFrom(reader.getClass())) {
        while (!iterator.isDone()) {
          ValueIterator vIter = iterator.getValueIterator();
          while (!vIter.isDone()) {
            output.println(vIter.getEntry());
            vIter.movePast(vIter.currentCandidate());
          }
          iterator.nextKey();
        }

        // otherwise we could have a key-value index
      } else if (KeyValueReader.class.isAssignableFrom(reader.getClass())) {
        while (!iterator.isDone()) {
          output.println(iterator.getKeyString() + "," + iterator.getValueString());
          iterator.nextKey();
        }
      } else {
        output.println("Unable to read index as a key-list or a key-value reader.");
      }

      reader.close();
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      String indexPath = p.getString("indexPath");
      run(new String[]{"", indexPath}, output);
    }
  }

  private static class DumpIndexManifestFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago dump-index-manifest <index-path>\n\n"
              + "  Dumps the manifest for an index file.";
    }

    @Override
    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      BTreeReader indexReader = BTreeFactory.getBTreeReader(args[1]);
      output.println(indexReader.getManifest().toPrettyString());
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      String indexPath = p.getString("indexPath");
      run(new String[]{"", indexPath}, output);
    }
  }

  private static class DumpKeysFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago dump-keys <index-part>\n\n"
              + "  Dumps keys from an index file.\n"
              + "  Output is in CSV format.\n";
    }

    @Override
    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      IndexPartReader reader = DiskIndex.openIndexPart(args[1]);

      if (reader.getManifest().get("emptyIndexFile", false)) {
        output.println("Empty Index File.");
        return;
      }

      KeyIterator iterator = reader.getIterator();
      while (!iterator.isDone()) {
        output.println(iterator.getKeyString());
        iterator.nextKey();
      }
      reader.close();
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      String indexPath = p.getString("indexPath");
      run(new String[]{"", indexPath}, output);
    }
  }

  private static class DumpKeyValueFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago dump-keys <indexwriter-file> <key>\n\n"
              + "  Dumps all data associated with a particular key from a file\n"
              + "  created by IndexWriter.  This includes corpus files and all\n"
              + "  index files built by Galago.\n";
    }

    @Override
    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 2) {
        output.println(getHelpString());
        return;
      }
      String key = args[2];
      output.printf("Dumping key: %s\n", key);
      IndexPartReader reader = DiskIndex.openIndexPart(args[1]);

      if (reader.getManifest().get("emptyIndexFile", false)) {
        output.println("Empty Index File.");
        return;
      }

      KeyIterator iterator = reader.getIterator();

      if (iterator.skipToKey(Utility.fromString(key))) {
        if (KeyListReader.class.isAssignableFrom(reader.getClass())) {
          ValueIterator vIter = iterator.getValueIterator();
          while (!vIter.isDone()) {
            output.printf("%s\n", vIter.getEntry());
            vIter.movePast(vIter.currentCandidate());
          }
        } else {
          output.printf("%s\n", iterator.getValueString());
        }
      }
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      String indexPath = p.getString("indexPath");
      String key = p.getString("key");
      run(new String[]{"", indexPath, key}, output);
    }
  }

  private static class DumpModifierFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago dump-modifier <modifier file>\n\n"
              + "  Dumps the contents of the specified modifier file.\n";
    }

    @Override
    public void run(String[] args, PrintStream output) throws Exception {
      if (args.length <= 1) {
        output.println(getHelpString());
        return;
      }

      IndexPartModifier modifier = DiskIndex.openIndexModifier(args[1]);

      if (modifier.getManifest().get("emptyIndexFile", false)) {
        output.println("Empty Index File.");
        return;
      }


      modifier.printContents(System.out);
      modifier.close();
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      String modifierPath = p.getString("modifierPath");
      run(new String[]{"", modifierPath}, output);
    }
  }

  private static class HelpFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago help [<function>]+\n\n"
              + "   Prints the usage information for any galago function.";
    }

    @Override
    public void run(String[] args, PrintStream output) throws Exception {

      StringBuilder defaultOutput = new StringBuilder(
              "Type 'galago help <command>' to get more help about any command.\n\n"
              + "Popular commands:\n"
              + "   build-fast\n"
              + "   search\n"
              + "   batch-search\n\n"
              + "All commands:\n");
      List<String> cmds = new ArrayList(appFunctions.keySet());
      Collections.sort(cmds);
      for (String cmd : cmds) {
        defaultOutput.append("   ").append(cmd).append("\n");
      }

      // galago help 
      if (args.length == 0) {
        output.println(defaultOutput);
        output.println();
      } else if (args.length == 1) {
        output.println(getHelpString());
        output.println();
        output.println(defaultOutput);
        output.println();
      } else {
        for (String arg : Utility.subarray(args, 1)) {
          output.println("function: " + arg + "\n");
          if (appFunctions.containsKey(arg)) {
            output.println(appFunctions.get(arg).getHelpString());
          } else {
            output.println("  UNKNOWN.");
          }
          output.println();
        }
      }
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      run((String[]) p.getList("function").toArray(new String[0]), output);
    }
  }

  private static class MakeCorpusFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago make-corpus [flags]+ --corpusPath=<corpus> (--inputPath=<input>)+\n\n"
              + "  Copies documents from input files into a corpus file.  A corpus\n"
              + "  structure is required to use any of the document lookup features in \n"
              + "  Galago, like printing snippets of search results.\n\n"
              + "<corpus>: Corpus output path or directory\n\n"
              + "<input>:  Can be either a file or directory, and as many can be\n"
              + "          specified as you like.  Galago can read html, xml, txt, \n"
              + "          arc (Heritrix), trectext, trecweb and corpus files.\n"
              + "          Files may be gzip compressed (.gz).\n\n"
              + "Algorithm Flags:\n"
              + "  --corpusFormat={folder|file}: Selects which format of corpus to produce.\n"
              + "                           File is a single file corpus. Folder is a folder of data files with an index.\n"
              + "                           The folder structure can be produce in a parallel manner.\n"
              + "                           [default=folder]\n\n"
              + getTupleFlowParameterString();
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      if (!p.containsKey("corpusPath") && !p.containsKey("inputPath")) {
        output.println(getHelpString());
        return;
      }
      MakeCorpus mc = new MakeCorpus();
      Job job = mc.getMakeCorpusJob(p);
      runTupleFlowJob(job, p, output);
    }
  }

  private static class OverwriteManifest extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago overwrite-manifest --indexPath=/path/to/index/file --key=value\n"
              + "  Rewrites internal index manifest data for index file.\n"
              + "  Allows parameters to be changed after index files have been written.\n\n"
              + "  WARNING : Use with caution - changing some parameters may make the index file non-readable.\n";
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      // first open the index
      String filename = p.getString("indexPath");
      RandomAccessFile indexReaderWriter = new RandomAccessFile(filename, "rw");

      long length = indexReaderWriter.length();
      long footerOffset = length - Integer.SIZE / 8 - 3 * Long.SIZE / 8;

      indexReaderWriter.seek(footerOffset);

      // read metadata values:
      long vocabularyOffset = indexReaderWriter.readLong();
      long manifestOffset = indexReaderWriter.readLong();
      int blockSize = indexReaderWriter.readInt();
      long magicNumber = indexReaderWriter.readLong();

      indexReaderWriter.seek(manifestOffset);
      byte[] xmlData = new byte[(int) (footerOffset - manifestOffset)];
      indexReaderWriter.read(xmlData);
      Parameters newParameters = Parameters.parse(xmlData);
      newParameters.copyFrom(p);

      indexReaderWriter.seek(manifestOffset);

      // write the new data back to the file
      xmlData = newParameters.toString().getBytes("UTF-8");
      indexReaderWriter.write(xmlData);
      indexReaderWriter.writeLong(vocabularyOffset);
      indexReaderWriter.writeLong(manifestOffset);
      indexReaderWriter.writeInt(blockSize);
      indexReaderWriter.writeLong(magicNumber);
      indexReaderWriter.close();
    }
  }

  private static class SearchFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago search <args> \n\n"
              + "  Starts a web interface for searching an index interactively.\n"
              + "  The URL to use in your web browser will appear in the command \n"
              + "  output.  Cancel the process (Control-C) to quit.\n\n"
              + "  If you specify a parameters file, you can direct Galago to load \n"
              + "  extra operators or traversals from your own jar files.  See \n"
              + "  the documentation in \n"
              + "  org.lemurproject.galago.core.retrieval.structured.FeatureFactory for more\n"
              + "  information.\n\n"
              + "  JSONParameters availiable:\n"
              + "   --corpus={file path} : corpus file path\n"
              + "   --index={file path}  : index file path\n"
              + "   --index={url}        : galago search url (for distributed retrieval)\n"
              + "   --port={int<65000}   : port number for web retrieval.\n\n"
              + "  JSONParameters can also be input through a configuration file.\n"
              + "  For example: search.parameters\n"
              + "  <parameters>\n"
              + "   <index>/path/to/index1</index>\n"
              + "   <index>/path/to/index2</index>\n"
              + "   <corpus>/path/to/corpus</corpus>\n"
              + "  </parameters>\n\n"
              + "  Note that the set of  parameters must include at least one index path.\n";
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      if (!p.containsKey("index")) {
        output.println(getHelpString());
        return;
      }

      Search search = new Search(p);
      int port = (int) p.get("port", 0);
      if (port == 0) {
        port = Utility.getFreePort();
      } else {
        if (!Utility.isFreePort(port)) {
          throw new IOException("Tried to bind to port " + port + " which is in use.");
        }
      }
      Server server = new Server(port);
      URLMappingHandler mh = new URLMappingHandler();
      mh.setHandler("/stream", new StreamContextHandler(search));
      mh.setHandler("/xml", new XMLContextHandler(search));
      mh.setHandler("/json", new JSONContextHandler(search));
      mh.setDefault(new SearchWebHandler(search));
      server.addHandler(mh);
      server.start();
      output.println("Server: http://localhost:" + port);

      // Ensure we print out the ip addr url as well
      InetAddress address = InetAddress.getLocalHost();
      String masterURL = String.format("http://%s:%d", address.getHostAddress(), port);
      output.println("ServerIP: " + masterURL);
    }
  }

  private static class XCountFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago xcount --x=<countable-query> --index=<index> \n\n"
              + "  Returns the number of times the countable-query occurs.\n"
              + "  More than one index and expression can be specified.\n"
              + "  Examples of countable-expressions: terms, ordered windows and unordered windows.\n";
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      if (!p.containsKey("index") || !p.containsKey("x")) {
        output.println(this.getHelpString());
        return;
      }

      Retrieval r = RetrievalFactory.instance(p);

      long count;
      for (String query : (List<String>) p.getList("x")) {
        Node parsed = StructuredQuery.parse(query);
        parsed.getNodeParameters().set("queryType", "count");
        Node transformed = r.transformQuery(parsed, new Parameters());

        if (p.get("printTransformation", false)) {
          System.err.println(query);
          System.err.println(parsed);
          System.err.println(transformed);
        }

        count = r.nodeStatistics(transformed).nodeFrequency;
        output.println(count + "\t" + query);
      }
      r.close();
    }
  }

  private static class XDocCountFn extends AppFunction {

    @Override
    public String getHelpString() {
      return "galago doccount --x=<countable-query> --index=<index> \n\n"
              + "  Returns the number of documents that contain the countable-query.\n"
              + "  More than one index and expression can be specified.\n"
              + "  Examples of countable-expressions: terms, ordered windows and unordered windows.\n";
    }

    @Override
    public void run(Parameters p, PrintStream output) throws Exception {
      if (!p.containsKey("index") || !p.containsKey("x")) {
        output.println(this.getHelpString());
        return;
      }

      Retrieval r = RetrievalFactory.instance(p);

      long count;
      for (String query : (List<String>) p.getList("x")) {
        Node parsed = StructuredQuery.parse(query);
        parsed.getNodeParameters().set("queryType", "count");
        Node transformed = r.transformQuery(parsed, new Parameters());

        if (p.get("printTransformation", false)) {
          System.err.println(query);
          System.err.println(parsed);
          System.err.println(transformed);
        }

        count = r.nodeStatistics(transformed).nodeDocumentCount;
        output.println(count + "\t" + query);
      }
      r.close();
    }
  }

  public static String getTupleFlowParameterString() {
    return "Tupleflow Flags:\n"
            + "  --printJob={true|false}: Simply prints the execution plan of a Tupleflow-based job then exits.\n"
            + "                           [default=false]\n"
            + "  --mode={local|threaded|drmaa}: Selects which executor to use \n"
            + "                           [default=local]\n"
            + "  --port={int<65000} :     port number for web based progress monitoring. \n"
            + "                           [default=randomly selected free port]\n"
            + "  --galagoJobDir=/path/to/temp/dir/: Sets the galago temp dir \n"
            + "                           [default = uses folders specified in ~/.galagotmp or java.io.tmpdir]\n"
            + "  --deleteJobDir={true|false}: Selects to delete the galago job directory\n"
            + "                           [default = true]\n"
            + "  --distrib={int > 1}:     Selects the number of simultaneous jobs to create\n"
            + "                           [default = 10]\n"
            + "  --server={true|false}:   Selects to use a server to show the progress of a tupleflow execution.\n"
            + "                           [default = true]\n";
  }

  // Static helper functions
  public static void runTupleFlowJob(Job job, Parameters p, PrintStream output) throws Exception {
    String printJob = p.get("printJob", "none");
    if (printJob.equals("plan")) {
      output.println(job.toString());
      return;
    } else if (printJob.equals("dot")) {
      output.println(job.toDotString());
      return;
    }

    int hash = (int) p.get("distrib", 0);
    if (hash > 0) {
      job.properties.put("hashCount", Integer.toString(hash));
    }

    ErrorStore store = new ErrorStore();
    JobExecutor.runLocally(job, store, p);
    if (store.hasStatements()) {
      output.println(store.toString());
    }
  }
}
