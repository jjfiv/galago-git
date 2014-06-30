/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.tools.apps;

import org.lemurproject.galago.core.tools.AppFunction;
import org.lemurproject.galago.core.tools.Search;
import org.lemurproject.galago.core.tools.SearchWebHandler;
import org.lemurproject.galago.core.tools.StreamContextHandler;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.web.WebHandler;
import org.lemurproject.galago.tupleflow.web.WebServer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintStream;

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
            + "  {\n"
            + "   \"index\": [\"/path/to/index1\",\n"
            + "               \"/path/to/index2\"],\n"
            + "   \"corpus\":\"/path/to/corpus\"\n"
            + "  }\n\n"
            + "  Note that the set of  parameters must include at least one index path.\n";
  }

  @Override
  public void run(Parameters p, PrintStream output) throws Exception {
    if (!p.containsKey("index")) {
      output.println(getHelpString());
      return;
    }

    Search search = new Search(p);
    final StreamContextHandler streamHandler = new StreamContextHandler(search);
    final SearchWebHandler searchHandler = new SearchWebHandler(search);

    WebServer server = WebServer.start(p, new WebHandler() {
      @Override
      public void handle(HttpServletRequest request, HttpServletResponse response) throws Exception {
        if(request.getPathInfo().equals("/stream")) {
          streamHandler.handle(request, response);
        } else {
          searchHandler.handle(request, response);
        }
      }
    });

    output.println("Server: "+server.getURL());
  }
}
