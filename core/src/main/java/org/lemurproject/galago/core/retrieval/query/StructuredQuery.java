// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.lemurproject.galago.core.retrieval.query.StructuredLexer.Token;
import org.lemurproject.galago.core.retrieval.query.StructuredLexer.Token.TokenType;
import org.lemurproject.galago.core.retrieval.query.StructuredLexer.TokenStream;

/**
 * Valid query language syntax:
 *
 * #operator:argument(...)
 * term, or term.field, or term.field.field, etc.
 *
 * [sjh] : added a parameterTerm parse function to allow decimal numbers to be passed
 *
 * @author trevor, sjh
 */
public class StructuredQuery {

  public static Token parseParameterTerm(TokenStream tokens) {
    Token term = new Token(tokens.current().text, tokens.current().position, tokens.current().type);
    tokens.next();

    while ((!tokens.currentEquals(":"))
            && (!tokens.currentEquals("="))
            && (!tokens.currentEquals("("))) {
      term.text = term.text + tokens.current().text;
      if (tokens.current().type != TokenType.TERM) {
        term.type = tokens.current().type;
      }
      tokens.next();
    }

    return term;
  }

  public static NodeParameters parseParameters(TokenStream tokens) {
    NodeParameters parameterData = new NodeParameters();
    assert tokens.currentEquals(":");

    Token key, value = null;

    while (tokens.currentEquals(":")) {
      tokens.next();
      key = parseParameterTerm(tokens);
      if (tokens.currentEquals("=")) {
        tokens.next();

        if (tokens.hasCurrent()) {
          value = parseParameterTerm(tokens);
          assert !parameterData.containsKey(key.text) : "Node parameters contains duplicate key " + key.text + ". Failed to parseParameters().";
          // if the Token is quoted or escaped in some way - it should be stored as a string.
        }
      } else {
        assert !parameterData.containsKey("default") : "Node parameters contains duplicate 'default' key. Failed to parseParameters().";
        value = key;
        key = new Token("default", value.position, TokenType.TERM);
      }

      if (value.type != TokenType.TERM) {
        parameterData.set(key.text, value.text);
      } else {
        parameterData.parseSet(key.text, value.text);
      }

    }

    return parameterData;
  }

  public static Node parseOperator(TokenStream tokens) {
    int position = tokens.current().position;
    assert tokens.currentEquals("#");
    tokens.next();

    String operatorName = tokens.current().text;
    tokens.next();
    NodeParameters parameters = new NodeParameters();

    if (tokens.currentEquals(":")) {
      parameters = parseParameters(tokens);
    }

    if (tokens.currentEquals("(")) {
      tokens.next();
    }

    ArrayList<Node> arguments = parseArgumentList(tokens);

    if (tokens.currentEquals(")")) {
      tokens.next();
    }

    return new Node(operatorName, parameters, arguments, position);
  }

  public static Node parseQuotedTerms(TokenStream tokens) {
    assert tokens.currentEquals("\"");
    ArrayList<Node> children = new ArrayList<Node>();
    int position = tokens.current().position;
    tokens.next();

    Node node = Node.Text(tokens.current().text, tokens.current().position);
    tokens.next();

    if (tokens.currentEquals("\"")) {
      tokens.next();
    }

    return node;
  }

  public static Node parseTerm(TokenStream tokens) {
    if (tokens.currentEquals("\"")) {
      return parseQuotedTerms(tokens);
    } else {
      Node node = Node.Text(tokens.current().text, tokens.current().position);
      tokens.next();
      return node;
    }
  }

  public static Node parseUnrestricted(TokenStream tokens) {
    if (tokens.currentEquals("#")) {
      return parseOperator(tokens);
    } else {
      return parseTerm(tokens);
    }
  }

  public static ArrayList<Node> parseFieldList(TokenStream tokens) {
    ArrayList<Node> nodes = new ArrayList<Node>();
    Node field = new Node("field", new NodeParameters(tokens.current().text), new ArrayList(), tokens.current().position);
    nodes.add(field);
    tokens.next();
    while (tokens.currentEquals(",")) {
      tokens.next();
      field = new Node("field", new NodeParameters(tokens.current().text), new ArrayList(), tokens.current().position);
      nodes.add(field);
      tokens.next();
    }
    return nodes;
  }

  public static Node nodeWithOptionalExtentOr(String operator, Node child, ArrayList<Node> orFields) {
    Node second = null;
    if (orFields.size() == 1) {
      second = orFields.get(0);
    } else {
      second = new Node("extentor", orFields);
    }
    ArrayList<Node> children = new ArrayList<Node>();
    children.add(child);
    children.add(second);
    return new Node(operator, children);
  }

  public static Node parseRestricted(TokenStream tokens) {
    Node node = parseUnrestricted(tokens);

    tokens.pushMark();
    while (tokens.hasCurrent() && tokens.currentEquals(".")) {
      tokens.next();

      if (tokens.currentEquals("(")) {
        // Not a restriction
        break;
      } else {
        ArrayList<Node> restrictNodes = parseFieldList(tokens);
        node = nodeWithOptionalExtentOr("inside", node, restrictNodes);
        // We successfully parsed this, so move the rewind marker
        tokens.resetMark();
      }
    }

    tokens.rewindToMark();
    return node;
  }

  public static Node parseArgument(TokenStream tokens) {
    Node node = parseRestricted(tokens);

    if (tokens.currentEquals(".")) {
      tokens.next();
      assert tokens.currentEquals("(");
      tokens.next();

      ArrayList<Node> smoothingNodes = parseFieldList(tokens);
      assert tokens.currentEquals(")");
      tokens.next();

      node = nodeWithOptionalExtentOr("smoothinside", node, smoothingNodes);
    }

    return node;
  }

  public static ArrayList<Node> parseArgumentList(TokenStream tokens) {
    ArrayList<Node> arguments = new ArrayList<Node>();
    while (tokens.hasCurrent()) {
      if (tokens.current().text.equals(")")) {
        break;
      } else {
        arguments.add(parseArgument(tokens));
      }
    }
    return arguments;
  }

  public static Node parse(String query) {
    StructuredLexer lexer = new StructuredLexer();
    ArrayList<StructuredLexer.Token> tokens;
    try {
      tokens = lexer.tokens(query);
    } catch (Exception e) {
      // TODO: fix this
      e.printStackTrace();
      return new Node("text", "");
    }
    TokenStream stream = new TokenStream(tokens);
    ArrayList<Node> arguments = parseArgumentList(stream);

    if (arguments.size() == 0) {
      return new Node("text", "");
    } else if (arguments.size() == 1) {
      return arguments.get(0);
    } else {
      return new Node("root", arguments, 0);
    }
  }

  public static Set<String> findQueryTerms(Node queryTree) {
    Set<String> operators = new HashSet();
    operators.add("counts");
    operators.add("extents");
    return findQueryTerms(queryTree, operators);
  }

  public static Set<String> findQueryTerms(Node queryTree, Set<String> operators) {
    HashSet<String> queryTerms = new HashSet<String>();

    if (operators.contains(queryTree.getOperator())) {
      queryTerms.add(queryTree.getNodeParameters().getString("default"));
    } else {
      for (Node child : queryTree.getInternalNodes()) {
        queryTerms.addAll(findQueryTerms(child, operators));
      }
    }

    return queryTerms;
  }
}
