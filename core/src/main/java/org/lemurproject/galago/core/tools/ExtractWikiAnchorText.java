package org.lemurproject.galago.core.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.lemurproject.galago.core.nasty.AnchorTextWriter;
import org.lemurproject.galago.core.parse.AnchorTextCreator;
import org.lemurproject.galago.core.parse.DocumentNumberer;
import org.lemurproject.galago.core.parse.DocumentSource;
import org.lemurproject.galago.core.parse.LinkCombiner;
import org.lemurproject.galago.core.parse.LinkExtractor;
import org.lemurproject.galago.core.parse.NumberedDocumentDataExtractor;
import org.lemurproject.galago.core.types.AdditionalDocumentText;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.ExtractedLink;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ConnectionAssignmentType;
import org.lemurproject.galago.tupleflow.execution.InputStep;
import org.lemurproject.galago.tupleflow.execution.Job;
import org.lemurproject.galago.tupleflow.execution.MultiStep;
import org.lemurproject.galago.tupleflow.execution.OutputStep;
import org.lemurproject.galago.tupleflow.execution.Stage;
import org.lemurproject.galago.tupleflow.execution.Step;

/**
 * 
 * @author jdalton
 */
public class ExtractWikiAnchorText extends AppFunction {


	public Job getAnchorTextJob(Parameters buildParameters) throws IOException {
		Job job = new Job();

		List<String> inputPaths = buildParameters.getAsList("inputPath");

		job.add(BuildStageTemplates.getSplitStage(inputPaths, DocumentSource.class, buildParameters));
		job.add(passThroughStage());
		job.add(getParseLinksStage(buildParameters));
		job.add(getLinkCombineStage());

		job.connect("inputSplit", "parseLinks", ConnectionAssignmentType.Each);
		job.connect("parseLinks", "passthrough", ConnectionAssignmentType.Combined);
		job.connect("passthrough", "linkCombine", ConnectionAssignmentType.Combined);
		job.connect("parseLinks", "linkCombine", ConnectionAssignmentType.Combined);

		System.out.println(job.toDotString());
		
		return job;
	}
	
	public Stage getParseLinksStage(Parameters buildParameters) {
		Stage stage = new Stage("parseLinks");

		// Connections
		stage.addInput("splits", new DocumentSplit.FileIdOrder());
		stage.addOutput("links", new ExtractedLink.DestUrlOrder());
		stage.addOutput("documentUrls", new NumberedDocumentData.UrlOrder());

		// Steps
		stage.add(new InputStep("splits"));
		stage.add(BuildStageTemplates.getParserStep(buildParameters));
		stage.add(BuildStageTemplates.getTokenizerStep(buildParameters));
		stage.add(new Step(DocumentNumberer.class));

		MultiStep multi = new MultiStep();
		ArrayList<Step> links =
				BuildStageTemplates.getExtractionSteps("links", LinkExtractor.class, new ExtractedLink.DestUrlOrder());
		ArrayList<Step> data =
				BuildStageTemplates.getExtractionSteps("documentUrls", NumberedDocumentDataExtractor.class,
						new NumberedDocumentData.UrlOrder());

		multi.groups.add(links);
		multi.groups.add(data);
		stage.add(multi);

		return stage;
	}
	
	public Stage passThroughStage() {
		Stage stage = new Stage("passthrough");
		stage.addInput("documentUrls", new NumberedDocumentData.UrlOrder());
		stage.addOutput("documentUrls2", new NumberedDocumentData.UrlOrder());
		
		stage.add(new InputStep("documentUrls"));
		//stage.add(new Step(Identity.class));
		stage.add(new OutputStep("documentUrls2"));
		
		return stage;
	}

	public Stage getLinkCombineStage() {
		Stage stage = new Stage("linkCombine");

		// Connections
		stage.addInput("documentUrls2", new NumberedDocumentData.UrlOrder());
		stage.addInput("links", new ExtractedLink.DestUrlOrder());

		// Steps
		Parameters p = new Parameters();
		p.set("documentDatas", "documentUrls2");
		p.set("extractedLinks", "links");
		stage.add(new Step(LinkCombiner.class, p));
		stage.add(new Step(AnchorTextCreator.class));
		stage.add(Utility.getSorter(new AdditionalDocumentText.IdentifierOrder()));
	    stage.add(new Step(AnchorTextWriter.class, p));

		return stage;
	}

	@Override
	public void run(Parameters p, PrintStream output) throws Exception {
		// build-fast index input
		if (!p.isList("inputPath")) {
			output.println(getHelpString());
			return;
		}

		Job job;
		BuildIndex build = new BuildIndex();
		job = build.getIndexJob(p);

		if (job != null) {
			runTupleFlowJob(job, p, output);
		}
	}


	public static void main(String[] args) throws Exception {
		Parameters p = new Parameters();
		p.set("inputPath", args[0]);
		p.set("mode", "local");
		p.set("tokenizer", new Parameters());
		String[] fields = {"a"};
		p.getMap("tokenizer").set("fields", Arrays.asList(fields));
		ExtractWikiAnchorText textExtractor = new ExtractWikiAnchorText();
		Job job = textExtractor.getAnchorTextJob(p);
		runTupleFlowJob(job, p, System.err);
	}

  @Override
  public String getName(){
    return "extract-wiki-anchor-text";
  }
  
	@Override
	public String getHelpString() {
		// TODO Auto-generated method stub
		return null;
	}
}
