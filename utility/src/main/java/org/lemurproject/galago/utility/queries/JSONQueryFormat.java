package org.lemurproject.galago.utility.queries;

import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This method handles the parsing of a Galago "query file" in many places.
 * @author jfoley.
 */
public class JSONQueryFormat {
  /**
   * this function extracts a list of queries from a parameter object. - there
   * are several methods of inputting queries: (query/queries) ->
   * String/List(String)/List(Map)
   *
   * if List(Map): [{"number":"id", "text":"query text"}, ...]
   */
  public static List<Parameters> collectQueries(Parameters parameters) throws IOException {
    List<Parameters> queries = new ArrayList<>();
    int unnumbered = 0;
    if (parameters.isString("query") || parameters.isList("query", String.class)) {
      String id;
      for (String q : parameters.getAsList("query", String.class)) {
        id = "unk-" + unnumbered;
        unnumbered++;
        queries.add(Parameters.parseString(String.format("{\"number\":\"%s\", \"text\":\"%s\"}", id, q)));
      }
    }
    if (parameters.isString("queries") || parameters.isList("queries", String.class)) {
      String id;
      for (String q : parameters.getAsList("query", String.class)) {
        id = "unk-" + unnumbered;
        unnumbered++;
        queries.add(Parameters.parseString(String.format("{\"number\":\"%s\", \"text\":\"%s\"}", id, q)));
      }
    }
    if (parameters.isList("query", Parameters.class)) {
      queries.addAll(parameters.getList("query", Parameters.class));
    }
    if (parameters.isList("queries", Parameters.class)) {
      queries.addAll(parameters.getList("queries", Parameters.class));
    }
    return queries;
  }
}
