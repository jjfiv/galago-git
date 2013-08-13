// BSD License (http://lemurproject.org/galago-galago-license)
package org.lemurproject.galago.tupleflow.typebuilder;

import java.io.File;
import java.io.IOException;
import org.antlr.runtime.RecognitionException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

/**
 * This is the Maven plugin that generates Type objects from galagotype specification
 * files.  See the build.xml file and the galagotype files in the galago core
 * project to see some examples of this.
 * 
 * @phase generate-sources
 * @goal typebuilder
 * @author trevor
 */
public class TypeBuilderMojo extends AbstractMojo {

  /**
   * Maven project variable
   * @parameter expression="${project}
   * @required
   * @readonly
   */
  private MavenProject project;

  /**
   * @parameter expression="${basedir}/src/main/galagotype" default-value="y"
   * @readonly
   */
  private String sourceDirectory;
  /**
   * @parameter expression="${basedir}" default-value="y"
   * @readonly
   */
  private String baseDirectory;

  private String outputDirectory;

  public String getOutputFilename(String packageName, String typeName) {
    String[] packagePathComponents = packageName.split("\\.");
    String packageStringPath = outputDirectory;
    for (String component : packagePathComponents) {
      packageStringPath += File.separator + component;
    }
    // Create the filename:
    return packageStringPath + File.separator + typeName + ".java";
  }

  public void execute() throws MojoExecutionException {
    if (sourceDirectory == null) {
      return;
    }
    File[] files = new File(sourceDirectory).listFiles();
    if (files == null) {
      return;
    }

    // add outputDirectory
    outputDirectory = baseDirectory + File.separator + "target" + File.separator + "generated-sources";
    project.addCompileSourceRoot(outputDirectory);

    for (File f : files) {
      if (f.isFile() && f.getName().endsWith("galagotype")) {
        TypeSpecification spec = null;
        java.io.FileWriter writer = null;

        try {
          spec = ParserDriver.getTypeSpecification(f.getAbsolutePath());
        } catch (IOException ex) {
          throw new MojoExecutionException("Couldn't open file: " + f.getAbsolutePath(), ex);
        } catch (RecognitionException ex) {
          throw new MojoExecutionException("Parsing failed: " + f.getAbsolutePath(), ex);
        }

        String outputFilename =
                getOutputFilename(spec.getPackageName(), spec.getTypeName());
        File outputFile = new File(outputFilename);

        // always generate new types.
        if (!outputFile.exists() || f.lastModified() > outputFile.lastModified()) {
        System.err.println("Generating " + spec.getTypeName());
        new File(outputFile.getParent()).mkdirs();
        TemplateTypeBuilder builder = new TemplateTypeBuilder(spec);
        try {
          writer = new java.io.FileWriter(outputFilename);
        } catch (IOException ex) {
          throw new MojoExecutionException("Trouble creating " + outputFilename, ex);
        }
        String comment =
                "// This file was automatically generated with the by org.lemurproject.galago.tupleflow.typebuilder.TypeBuilderMojo ...\n";

        try {
          writer.write(comment);
          writer.write(builder.toString());
          writer.close();
        } catch (IOException e) {
          throw new MojoExecutionException("Trouble writing " + outputFilename);
        }
        }
      }
    }
  }
}
