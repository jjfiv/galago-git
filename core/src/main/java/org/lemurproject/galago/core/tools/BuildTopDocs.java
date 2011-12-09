// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tools;

import java.io.IOException;
import org.lemurproject.galago.core.index.AbstractModifier;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.TopDocsWriter;
import org.lemurproject.galago.core.parse.TopDocsScanner;
import org.lemurproject.galago.core.parse.VocabularySource;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.core.types.TopDocsEntry;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.ConnectionPointType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.StageConnectionPoint;
import org.lemurproject.galago.tupleflow.execution.Step;

/**
 *
 * @author marc
 */
public class BuildTopDocs {

  protected String indexPath;
  protected String partName;
  protected long topdocs_size;
  protected long list_min_size;

  public Stage getReadIndexStage() throws IOException {
    Stage stage = new Stage("readIndex");
    stage.add(new StageConnectionPoint(ConnectionPointType.Output, "terms",
            new KeyValuePair.KeyOrder()));

    Parameters p = new Parameters();
    p.set("filename", DiskIndex.getPartPath(this.indexPath, this.partName));
    stage.add(new Step(VocabularySource.class, p));
    stage.add(new OutputStep("terms"));
    return stage;
  }

  public Stage getIterateOverPostingListsStage() {
    Stage stage = new Stage("iterateOverPostingLists");
    stage.add(new StageConnectionPoint(ConnectionPointType.Input, "terms",
            new KeyValuePair.KeyOrder()));
    stage.add(new StageConnectionPoint(ConnectionPointType.Output, "topdocs",
            new TopDocsEntry.WordDescProbabilityDocumentOrder()));

    Parameters p = new Parameters();
    p.set("directory", this.indexPath);
    p.set("part", this.partName);
    p.set("size", this.topdocs_size);
    p.set("minlength", this.list_min_size);
    stage.add(new InputStep("terms"));
    stage.add(new Step(TopDocsScanner.class, p));
    stage.add(new OutputStep("topdocs"));
    return stage;
  }

  public Stage getWriteTopDocsStage() {
    Stage stage = new Stage("writeTopDocs");
    stage.add(new StageConnectionPoint(ConnectionPointType.Input, "topdocs",
            new TopDocsEntry.WordDescProbabilityDocumentOrder()));
    Parameters p = new Parameters();
    p.set("directory", this.indexPath);
    p.set("part", this.partName);
    p.set("name", "topdocs");
    p.set("filename", AbstractModifier.getModifierName(this.indexPath, this.partName, "topdocs"));
    stage.add(new InputStep("topdocs"));
    stage.add(new Step(TopDocsWriter.class, p));
    return stage;
  }

  public Job getIndexJob(Parameters p) throws IOException {
    Job job = new Job();
    this.indexPath = p.getString("index");
    this.partName = p.getString("part");
    this.topdocs_size = (int) p.get("size", Integer.MAX_VALUE);
    this.list_min_size = p.get("minlength", Long.MAX_VALUE);

    System.out.printf("Creating topdocs for part %s. Minimum list length: %s. Topdocs lists size: %s\n",
            this.partName, this.list_min_size, this.topdocs_size);

    job.add(getReadIndexStage());
    job.add(getIterateOverPostingListsStage());
    job.add(getWriteTopDocsStage());

    job.connect("readIndex", "iterateOverPostingLists", ConnectionAssignmentType.Each);
    job.connect("iterateOverPostingLists", "writeTopDocs", ConnectionAssignmentType.Combined);

    return job;
  }
}
