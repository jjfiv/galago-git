// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.Search.SearchResult;
import org.lemurproject.galago.core.tools.Search.SearchResultItem;
import org.lemurproject.galago.tupleflow.web.WebHandler;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;
import org.znerd.xmlenc.XMLOutputter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.Map.Entry;

/**
 * <p>Handles web search requests against a Galago index. Also handles XML
 * requests for documents, snippets and search results.</p>
 *
 * <p>This class is set up to work with an embedded Jetty instance, but it
 * should be fairly easy to wrap into a Servlet for use with something else
 * (Tomcat, Glassfish, etc.)</p>
 *
 * <p>URLs supported:</p>
 *
 * <table> <tr> <td>/</td> <td>Main Page</td> </tr> <tr> <td>/search</td>
 * <td>HTML Search Results (q, start, n)</td> </tr> <tr> <td>/xmlsearch</td>
 * <td>XML Search Results (q, start, n)</td> </tr> <tr> <td>/snippet</td>
 * <td>XML Snippet Result (name, term+)</td> </tr> <tr> <td>/document</td>
 * <td>Document Result (name)</td> </tr> </table>
 *
 * @author trevor, irmarc
 */
public class SearchWebHandler implements WebHandler {

  protected Search search;

  public SearchWebHandler(Search search) {
    this.search = search;
  }

  public String getEscapedString(String text) {
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < text.length(); ++i) {
      char c = text.charAt(i);
      if (c >= 128) {
        builder.append("&#").append((int) c).append(";");
      } else {
        builder.append(c);
      }
    }

    return builder.toString();
  }

  public void handleDocument(HttpServletRequest request, HttpServletResponse response) throws IOException {
    request.getParameterMap();
    String identifier = request.getParameter("identifier");
    identifier = URLDecoder.decode(identifier, "UTF-8");
    DocumentComponents p = new DocumentComponents(true, true, false);
    Document document = search.getDocument(identifier, p);
    response.setContentType("text/html; charset=UTF-8");

    PrintWriter writer = response.getWriter();
    
    writer.write(document.name);
    writer.write("<p>");
    Map<String, String> metadata = document.metadata;
    if (metadata != null) {
        writer.append("META:<br>");
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            writer.write("key: " + entry.getKey());
            writer.write(" value:" +getEscapedString(entry.getValue()));
            writer.write("<br>");
        }
        writer.write("<p>");
    }

    writer.write("TEXT:");
    writer.write(getEscapedString(document.text));
    writer.close();
  }

  public void handleSnippet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    String identifier = request.getParameter("identifier");
    String[] terms = request.getParameterValues("term");
    Set<String> queryTerms = new HashSet<String>(Arrays.asList(terms));

    DocumentComponents p = new DocumentComponents(true, true, false);
    Document document = search.getDocument(identifier, p);

    if (document == null) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      response.setContentType("text/xml");
      PrintWriter writer = response.getWriter();
      String snippet = search.getSummary(document, queryTerms);
      String title = document.metadata.get("title");
      String url = document.metadata.get("url");

      if (snippet == null) {
        snippet = "";
      }

      response.setContentType("text/xml");
      writer.append("<response>\n");
      writer.append(String.format("<snippet>%s</snippet>\n", snippet));
      writer.append(String.format("<identifier>%s</identifier>\n", identifier));
      writer.append(String.format("<title>%s</title>\n", scrub(title)));
      writer.append(String.format("<url>%s</url>\n", scrub(url)));
      writer.append("</response>");
      writer.close();
    }
  }

  protected String scrub(String s) throws UnsupportedEncodingException {
    if (s == null) {
      return null;
    }
    return s.replace("<", "&gt;").replace(">", "&lt;").replace("&", "&amp;");
  }

  protected String decode(String s) throws UnsupportedEncodingException {
    String decoded = URLDecoder.decode(s, "UTF-8");
    return decoded;
  }

  public void retrieveImage(OutputStream output) throws IOException {
    InputStream image = getClass().getResourceAsStream("/images/galago.png");
    StreamUtil.copyStream(image, output);
    output.close();
  }

  public void handleImage(HttpServletRequest request, HttpServletResponse response) throws IOException {
    OutputStream output = response.getOutputStream();
    response.setContentType("image/png");
    retrieveImage(output);
  }

  public void handleSearch(HttpServletRequest request, HttpServletResponse response) throws Exception {
    SearchResult result = performSearch(request, true);
    response.setContentType("text/html");
    String displayQuery = scrub(request.getParameter("q"));
    String encodedQuery = URLEncoder.encode(request.getParameter("q"), "UTF-8");

    PrintWriter writer = response.getWriter();
    writer.append("<html>\n");
    writer.append("<head>\n");
    writer.append(String.format("<title>%s - Galago Search</title>\n", displayQuery));
    writeStyle(writer);
    writer.append("<script type=\"text/javascript\">\n");
    writer.append("function toggleDebug() {\n");
    writer.append("   var object = document.getElementById('debug');\n");
    writer.append("   if (object.style.display != 'block') {\n");
    writer.append("     object.style.display = 'block';\n");
    writer.append("  } else {\n");
    writer.append("     object.style.display = 'none';\n");
    writer.append("  }\n");
    writer.append("}\n");
    writer.append("</script>\n");
    writer.append("</head>\n<body>\n");

    writer.append("<div id=\"header\">\n");
    writer.append("<table><tr>");
    writer.append("<td><a href=\"http://lemurproject.org\">"
            + "<img src=\"/images/galago.png\"></a></td>");
    writer.append("<td><br/><form action=\"search\">")
        .append("<input name=\"q\" size=\"40\" value=\"")
        .append(displayQuery).append("\" />")
        .append("<input value=\"Search\" type=\"submit\" /></form></td>");
    writer.append("</tr>");
    writer.append("</table>\n");
    writer.append("</div>\n");

    writer.append("<center>[<a href=\"#\" onClick=\"toggleDebug(); return false;\">debug</a>]</center>");
    writer.append("<div id=\"debug\">");
    writer.append("<table>");
    writer.append(String.format("<tr><td>%s</td><td>%s</td></tr>",
            "Original Query", result.queryAsString));
    writer.append(String.format("<tr><td>%s</td><td>%s</td></tr>",
            "Parsed Query", result.query.toString()));
    writer.append(String.format("<tr><td>%s</td><td>%s</td></tr>",
            "Transformed Query", result.transformedQuery.toString()));
    writer.append("</table>");
    writer.append("</div>");

    for (SearchResultItem item : result.items) {
      writer.append("<div id=\"result\">\n");
      writer.append(String.format("<a href=\"document?identifier=%s\">%s</a><br/>"
              + "<div id=\"summary\">%s</div>\n"
              + "<div id=\"meta\">%s - %s - %.2f</div>\n",
              item.identifier,
              item.displayTitle,
              item.summary,
              scrub(item.identifier),
              scrub(item.url),
              item.score));
      writer.append("</div>\n");
    }

    String startAtString = request.getParameter("start");
    String countString = request.getParameter("n");
    int startAt = 0;
    int count = 10;

    if (startAtString != null) {
      startAt = Integer.parseInt(startAtString);
    }
    if (countString != null) {
      count = Integer.parseInt(countString);
    }

    writer.append("<center>\n");
    if (startAt != 0) {
      writer.append(String.format("<a href=\"search?q=%s&start=%d&n=%d\">Previous</a>",
              encodedQuery, Math.max(startAt - count, 0), count));
      if (result.items.size() >= count) {
        writer.append(" | ");
      }
    }

    if (result.items.size() >= count) {
      writer.append(String.format("<a href=\"search?q=%s&start=%d&n=%d\">Next</a>",
              encodedQuery, startAt + count, count));
    }
    writer.append("</center>");
    writer.append("</body>");
    writer.append("</html>");
    writer.close();
  }

  public void handleSearchXML(HttpServletRequest request, HttpServletResponse response) throws Exception {
    SearchResult result = performSearch(request, false);
    PrintWriter writer = response.getWriter();
    XMLOutputter outputter = new XMLOutputter(writer, "UTF-8");
    response.setContentType("text/xml");
    outputter.startTag("response");

    for (SearchResultItem item : result.items) {
      outputter.startTag("result");

      if (item.identifier != null) {
        outputter.startTag("identifier");
        outputter.pcdata(item.identifier);
        outputter.endTag();
      }

      if (item.displayTitle != null) {
        outputter.startTag("title");
        outputter.pcdata(item.displayTitle);
        outputter.endTag();
      }

      if (item.url != null) {
        outputter.startTag("url");
        outputter.pcdata(item.url);
        outputter.endTag();
      }

      if (item.summary != null) {
        outputter.startTag("snippet");
        outputter.pcdata(item.summary);
        outputter.endTag();
      }

      outputter.startTag("rank");
      outputter.pcdata("" + item.rank);
      outputter.endTag();

      outputter.startTag("document");
      //outputter.pcdata(Integer.toString(item.internalId));
      outputter.endTag();

      outputter.startTag("score");
      outputter.pcdata(Double.toString(item.score));
      outputter.endTag();

      outputter.startTag("metadata");
      for (Entry<String, String> entry : item.metadata.entrySet()) {
        outputter.startTag("item");
        outputter.startTag("key");
        outputter.pcdata(entry.getKey());
        outputter.endTag();
        outputter.startTag("value");
        outputter.pcdata(entry.getValue());
        outputter.endTag();
        outputter.endTag();
      }
      outputter.endTag(); // metadata

      outputter.endTag(); // result
    }
    outputter.endTag();
    outputter.endDocument();
  }

  public void handleXCount(HttpServletRequest request, HttpServletResponse response)
          throws Exception {
    String exp = request.getParameter("expression");
    long count = search.xCount(exp);
    PrintWriter writer = response.getWriter();
    XMLOutputter outputter = new XMLOutputter(writer, "UTF-8");
    response.setContentType("text/xml");
    outputter.startTag("response");

    outputter.startTag("count");
    outputter.pcdata(Long.toString(count));
    outputter.endTag(); // count

    outputter.endTag(); // response
    outputter.endDocument();
  }

  public void handleDocCount(HttpServletRequest request, HttpServletResponse response)
          throws Exception {
    String exp = request.getParameter("expression");
    long count = search.docCount(exp);
    PrintWriter writer = response.getWriter();
    XMLOutputter outputter = new XMLOutputter(writer, "UTF-8");
    response.setContentType("text/xml");
    outputter.startTag("response");

    outputter.startTag("count");
    outputter.pcdata(Long.toString(count));
    outputter.endTag(); // count

    outputter.endTag(); // response
    outputter.endDocument();
  }

  public void handleStats(HttpServletRequest request, HttpServletResponse response)
          throws IllegalStateException, IllegalArgumentException, IOException {

    // handle this better...
    Parameters parts = search.getAvailiableParts();
    String part = "postings";
    part = parts.containsKey("postings.porter") ? "postings.porter" : part;
    part = parts.containsKey("postings.krovetz") ? "postings.krovetz" : part;
    
    IndexPartStatistics stats = search.getIndexPartStatistics(part);
    PrintWriter writer = response.getWriter();
    writer.write(stats.toString()); // parameters are output into an XML format already
    writer.close();
  }

  public void handleParts(HttpServletRequest request, HttpServletResponse response)
          throws IllegalStateException, IllegalArgumentException, IOException {
    Parameters p = search.getAvailiableParts();
    PrintWriter writer = response.getWriter();
    writer.write(p.toString()); // parameters are output into an XML format already
    writer.close();
  }

  public void handleTransformQuery(HttpServletRequest request, HttpServletResponse response) throws ServletException {
    String nodeString = request.getParameter("q");
    String retrievalGroup = request.getParameter("retrievalGroup");
    Node root = StructuredQuery.parse(nodeString);
    try {
      Node transformed = search.retrieval.transformQuery(root, Parameters.instance());
      PrintWriter writer = response.getWriter();
      XMLOutputter outputter = new XMLOutputter(writer, "UTF-8");
      response.setContentType("text/xml");
      outputter.startTag("response");

      outputter.startTag("query");
      outputter.pcdata(transformed.toString());
      outputter.endTag(); // count

      outputter.endTag(); // response
      outputter.endDocument();
    } catch (Exception e) {
      throw new ServletException("Unable to complete request.", e);
    }
  }

  public void writeCollectionSelector(PrintWriter writer) {
    // About to add this in
    //ArrayList<String> collGroups = search.getCollectionGroups();
    ArrayList<String> collGroups = new ArrayList<String>();
    if (collGroups.size() == 1) {
      return;
    }

    writer.append("<tr><td>Select the sub-collection to search:&nbsp;");
    writer.append("<select id=\"subset\" name=\"subset\">\n");

    Collections.sort(collGroups, new CollectionGroupSorter());
    for (String desc : collGroups) {
      writer.append(String.format("<option %s value=\"%s\">%s</option>\n", (desc.toLowerCase().equals("all") ? "selected" : ""),
              desc, desc));
    }
    writer.append("</select>\n");
    writer.append("</td></tr>");
  }

  public void writeStyle(PrintWriter writer) {
    writer.write("<style type=\"text/css\">\n");
    writer.write("body { font-family: Helvetica, sans-serif; }\n");
    writer.write("img { border-style: none; }\n");
    writer.write("#box { border: 1px solid #ccc; margin: 100px auto; width: 500px;"
            + "background: rgb(210, 233, 217); }\n");
    writer.write("#box a { font-size: small; text-decoration: none; }\n");
    writer.write("#box a:link { color: rgb(0, 93, 40); }\n");
    writer.write("#box a:visited { color: rgb(90, 93, 90); }\n");
    writer.write("#header { background: rgb(210, 233, 217); border: 1px solid #ccc; }\n");
    writer.write("#result { padding: 10px 5px; max-width: 550px; }\n");
    writer.write("#meta { font-size: small; color: rgb(60, 100, 60); }\n");
    writer.write("#summary { font-size: small; }\n");
    writer.write("#debug { display: none; }\n");
    writer.write("</style>");
  }

  public void handleMainPage(HttpServletRequest request, HttpServletResponse response) throws IOException {
    PrintWriter writer = response.getWriter();
    response.setContentType("text/html");

    writer.append("<html>\n");
    writer.append("<head>\n");
    writeStyle(writer);
    writer.append("<title>Galago Search</title></head>");
    writer.append("<body>");
    writer.append("<center><br/><br/><div id=\"box\">"
            + "<a href=\"http://lemurproject.org\">"
            + "<img src=\"/images/galago.png\"/></a><br/>\n");
    writer.append("<form action=\"search\"><input name=\"q\" size=\"40\">"
            + "<input value=\"Search\" type=\"submit\" /></form><br/><br/>");
    writer.append("</div></center></body></html>\n");
    writer.close();
  }

  public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (request.getPathInfo().equals("/search")) {
      try {
        handleSearch(request, response);
      } catch (Exception e) {
          e.printStackTrace();
        throw new ServletException("Caught exception from handleSearch", e);
      }
    } else if (request.getPathInfo().equals("/document")) {
      handleDocument(request, response);
    } else if (request.getPathInfo().equals("/searchxml")) {
      try {
        handleSearchXML(request, response);
      } catch (Exception e) {
        throw new ServletException("Caught exception from handleSearchXML", e);
      }
    } else if (request.getPathInfo().equals("/xcount")) {
      try {
        handleXCount(request, response);
      } catch (Exception e) {
        throw new ServletException("Caught exception from handleXCount", e);
      }
    } else if (request.getPathInfo().equals("/doccount")) {
      try {
        handleDocCount(request, response);
      } catch (Exception e) {
        throw new ServletException("Caught exception from handleXCount", e);
      }
    } else if (request.getPathInfo().equals("/snippet")) {
      handleSnippet(request, response);
    } else if (request.getPathInfo().startsWith("/images")) {
      handleImage(request, response);
    } else if (request.getPathInfo().equals("/stats")) {
      handleStats(request, response);
    } else if (request.getPathInfo().equals("/parts")) {
      handleParts(request, response);
    } else if (request.getPathInfo().equals("/transform")) {
      handleTransformQuery(request, response);
    } else {
      handleMainPage(request, response);
    }
  }

  protected SearchResult performSearch(HttpServletRequest request, boolean snippets) throws Exception {
    String query = request.getParameter("q");
    System.out.println("q="+query);
    String transformString = request.getParameter("transform");
    boolean doTransform = Boolean.parseBoolean(transformString == null ? "true" : transformString);
    String startAtString = request.getParameter("start");
    String countString = request.getParameter("n");
    String id = (request.getParameterValues("indexId") == null) ? "0" : request.getParameterValues("indexId")[0];
    String retGroup = (request.getParameterValues("subset") == null) ? "all" : request.getParameterValues("subset")[0];
    String qtype = (request.getParameterValues("qtype") == null) ? "complex" : request.getParameterValues("qtype")[0];
    int startAt = (startAtString == null) ? 0 : Integer.parseInt(startAtString);
    int resultCount = (countString == null) ? 10 : Integer.parseInt(countString);

    Parameters p = Parameters.instance();
    p.set("indexId", id);
    p.set("queryType", qtype);
    p.set("requested", startAt + resultCount);
    p.set("startAt", startAt);
    p.set("resultCount", resultCount);
    p.set("retrievalGroup", retGroup);

    SearchResult result;
    if (doTransform) {
      result = search.runQuery(query, p, snippets);
    } else {
      result = search.runTransformedQuery(StructuredQuery.parse(query), p, snippets);
    }
    return result;
  }

  protected static class CollectionGroupSorter implements Comparator<String>, Serializable {

    public int compare(String s1, String s2) {
      if (s1.toLowerCase().equals("all")) {
        return -1;
      }
      if (s2.toLowerCase().equals("all")) {
        return 1;
      }
      return (s1.compareTo(s2));
    }
  }
}
