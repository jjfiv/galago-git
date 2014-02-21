/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import java.io.PrintStream;
import java.net.InetAddress;
import org.eclipse.jetty.server.Server;
import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.Search;
import org.lemurproject.galago.core.tools.SearchWebHandler;
import org.lemurproject.galago.core.tools.StreamContextHandler;
import org.lemurproject.galago.core.tools.URLMappingHandler;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class SearchFn extends AppFunction {

  @Override
  public String getName() {
    return "search";
  }

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
    }
    Server server = new Server(port);
    URLMappingHandler mh = new URLMappingHandler();
    mh.setHandler("/stream", new StreamContextHandler(search));
    mh.setDefault(new SearchWebHandler(search));
    server.setHandler(mh);
    server.start();
    output.println("Server: http://localhost:" + port);

    // Ensure we print out the ip addr url as well
    InetAddress address = InetAddress.getLocalHost();
    String masterURL = String.format("http://%s:%d", address.getHostAddress(), port);
    output.println("ServerIP: " + masterURL);
  }
}
