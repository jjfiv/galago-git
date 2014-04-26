// BSD License (http://lemurproject.org/galago-galago-license)
package org.lemurproject.galago.tupleflow.typebuilder;

import org.antlr.runtime.RecognitionException;
import org.antlr.stringtemplate.CommonGroupLoader;
import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.antlr.stringtemplate.language.AngleBracketTemplateLexer;
import org.apache.maven.plugin.MojoExecutionException;
import org.lemurproject.galago.tupleflow.typebuilder.FieldSpecification.DataType;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

/**
 *
 * @author trevor
 */
public class TemplateTypeBuilder {

  StringTemplateGroup template;
  String typeName;
  String typePackage;
  ArrayList<Field> typeFields;
  ArrayList<OrderSpec> typeOrders;

  public static String caps(String input) {
    if (input.length() == 0) {
      return input;
    }
    char first = Character.toUpperCase(input.charAt(0));
    return "" + first + input.substring(1);
  }

  public static String plural(String input) {
    return input + "s";
  }

  public static class Field {

    public Field(DataType type, String name) {
      this.dataType = type;
      this.type = type.getType();
      this.name = name;
      this.capsName = caps(name);
      this.pluralName = plural(name);

      this.inputType = caps(type.getInternalType());
      this.baseType = type.getBaseType();
      this.classTypeName = type.getClassName();
      this.boxedName = type.getBoxedName();
      this.isArray = dataType.isArray();
      this.isString = dataType.isString();
    }

    public DataType dataType;
    public String type;
    public String name;
    public String baseType;
    public boolean isInteger;
    public boolean isString;
    public boolean isArray;
    public String classTypeName;
    public String boxedName;
    public String capsName;
    public String inputType;
    public String pluralName;
  }

  public static class OrderedField extends Field {

    public OrderedField(Field field, boolean ascending, boolean delta, boolean runLengthEncoded) {
      this(field.dataType, field.name, ascending, delta, runLengthEncoded);
    }

    public OrderedField(DataType type, String name, boolean ascending, boolean delta, boolean runLengthEncoded) {
      super(type, name);
      this.ascending = ascending;
      this.direction = ascending ? "+" : "-";
      this.directionName = ascending ? "Ascending" : "Descending";
      this.runLengthEncoded = runLengthEncoded;
      this.delta = delta;
    }
    public boolean ascending;
    public String direction;
    public String directionName;
    public boolean delta;
    public boolean runLengthEncoded;
    public ArrayList<OrderedField> remaining = new ArrayList<OrderedField>();
  }

  public static class OrderSpec {

    public OrderSpec(OrderSpecification spec, ArrayList<Field> allFields) {
      this.allFields = allFields;

      HashMap<String, Field> fieldMap = new HashMap<String,Field>();
      HashSet<String> orderedNames = new HashSet<String>();

      for (Field f : allFields) {
        fieldMap.put(f.name, f);
      }

      ArrayList<OrderedFieldSpecification> inputFields = spec.getOrderedFields();

      for (OrderedFieldSpecification inputField : inputFields) {
        // BUGBUG: this is where delta encoding should go
        boolean useDelta = false; //isLastField && (field.isInteger || field.isString);
        boolean useRLE = !useDelta;
        boolean isAscending = (inputField.getDirection() == Direction.ASCENDING);
        String fieldName = inputField.getName();

        if (fieldMap.get(fieldName) == null) {
          throw new RuntimeException("'" + fieldName + "' is specified in an order statement, "
              + "but it isn't a type field name.");
        }

        OrderedField ordered = new OrderedField(fieldMap.get(fieldName), isAscending, useDelta, useRLE);
        orderedFields.add(ordered);
        orderedNames.add(fieldName);
      }

      for (int i = 0; i < inputFields.size(); i++) {
        orderedFields.get(i).remaining.addAll(
                orderedFields.subList(i + 1, orderedFields.size()));
      }

      backwardOrderedFields.addAll(orderedFields);
      Collections.reverse(backwardOrderedFields);

      for (OrderedField field : orderedFields) {
        if (field.runLengthEncoded) {
          rleFields.add(field);
        }
        if (field.delta) {
          deltaFields.add(field);
        }
      }

      for (int i = 0; i < orderedFields.size(); i++) {
        OrderedField current = orderedFields.get(i);
        OrderedField next = ((i + 1) < orderedFields.size()) ? orderedFields.get(i + 1) : null;
        OrderedField previous = (i != 0) ? orderedFields.get(i - 1) : null;

        fieldPairs.add(new FieldPair(current, next, previous));
      }

      for (Field field : allFields) {
        if (orderedNames.contains(field.name)) {
          continue;
        }
        unorderedFields.add(field);
      }
    }

    /**
     * This is invoked by reflection.
     */
    public String getClassName() {
      StringBuilder builder = new StringBuilder();

      if (orderedFields.size() > 0) {
        for (OrderedField orderedField : orderedFields) {
          if (!orderedField.ascending) {
            builder.append("Desc");
          }
          builder.append(caps(orderedField.name));
        }

        builder.append("Order");
      } else {
        builder.append("Unordered");
      }

      return builder.toString();
    }

    public static class FieldPair {

      public FieldPair(OrderedField c, OrderedField n, OrderedField p) {
        current = c;
        next = n;
        previous = p;
      }
      public OrderedField current;
      public OrderedField next;
      public OrderedField previous;
    }
    public ArrayList<OrderedField> orderedFields = new ArrayList<OrderedField>();
    public ArrayList<OrderedField> backwardOrderedFields = new ArrayList<OrderedField>();
    public ArrayList<FieldPair> fieldPairs = new ArrayList<FieldPair>();
    public ArrayList<OrderedField> rleFields = new ArrayList<OrderedField>();
    public ArrayList<Field> unorderedFields = new ArrayList<Field>();
    public ArrayList<OrderedField> deltaFields = new ArrayList<OrderedField>();
    public ArrayList<Field> allFields;
  }

  /**
   * Returns the text of a Type<T> object for the T class.
   */
  @Override
  public String toString() {
    StringTemplate t = template.getInstanceOf("type");
    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("typeName", this.typeName);
    map.put("package", this.typePackage);
    map.put("orders", this.typeOrders);
    map.put("fields", this.typeFields);

    boolean containsArray = false;
    for (Field f : typeFields) {
      containsArray = containsArray || f.isArray;
    }
    map.put("containsArray", containsArray);
    t.setAttributes(map);
    return t.toString();
  }

  public TemplateTypeBuilder(TypeSpecification spec) {
    CommonGroupLoader loader = new CommonGroupLoader("org/lemurproject/galago/tupleflow/templates", null);
    StringTemplateGroup.registerGroupLoader(loader);
    StringTemplateGroup.registerDefaultLexer(AngleBracketTemplateLexer.class);
    this.template = StringTemplateGroup.loadGroup("GalagoType");

    this.typeName = spec.getTypeName();
    this.typePackage = spec.getPackageName();
    this.typeFields = new ArrayList<Field>();
    this.typeOrders = new ArrayList<OrderSpec>();

    for (FieldSpecification fieldSpec : spec.getFields()) {
      Field field = new Field(fieldSpec.getType(), fieldSpec.getName());
      typeFields.add(field);
    }

    for (OrderSpecification orderSpec : spec.getOrders()) {
      OrderSpec order = new OrderSpec(orderSpec, typeFields);
      typeOrders.add(order);
    }
  }

  public static void main(String[] args) throws java.lang.Exception {
    if (args.length < 1) {
      System.err.println(
              "Usage: " + TemplateTypeBuilder.class.getCanonicalName()
              + " <galago type files>");
      System.exit(0);
    }

    ArrayList<File> files = new ArrayList<File>();

    File outputDir = null;

    for (String arg : args) {
      if (arg.startsWith("--outputDir=")) {
        outputDir = new File(arg.split("=")[1]);
        assert outputDir.mkdirs() : "Output directory " + outputDir + " could not be created.";
        assert outputDir.isDirectory() : "Output directory " + outputDir + " does not exist, could not be created, or is not a directory.";
      } else {
        files.add(new File(arg));
      }
    }


    for (File f : files) {
      if (f.isFile() && f.getName().endsWith("galagotype")) {
        TypeSpecification spec;
        java.io.FileWriter writer = null;

        try {
          spec = ParserDriver.getTypeSpecification(f.getAbsolutePath());
        } catch (IOException ex) {
          throw new MojoExecutionException("Couldn't open file: " + f.getAbsolutePath(), ex);
        } catch (RecognitionException ex) {
          throw new MojoExecutionException("Parsing failed: " + f.getAbsolutePath(), ex);
        }

        String outputFilename =
                f.getPath().replaceAll(".galagotype$", ".java");

        File outputFile = new File(outputFilename);
        if (outputDir != null) {
          outputFile = new File(outputDir + File.separator + outputFile.getName());
        }

        // always generate the new file
        System.err.println("Generating " + spec.getTypeName());
        TemplateTypeBuilder builder = new TemplateTypeBuilder(spec);
        try {
          writer = new java.io.FileWriter(outputFile);
          final String comment =
              "// This file was automatically generated with the command: \n"
            + "//     java " + TemplateTypeBuilder.class.getCanonicalName() + " ...\n";
          writer.write(comment);
          writer.write(builder.toString());
        } catch (IOException e) {
          throw new MojoExecutionException("Trouble writing " + outputFile);
        } finally {
          if(writer != null) writer.close();
        }
      }
    }
  }
}
