package fr.dechoux.bertrand.poc.cascading.multicount;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.regex.RegexSplitGenerator;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.TemplateTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

public class App {
	// hardcoded paths (this is an example)
	private static String OUT_PATH = "target/cascading/out";
	private static String IN_PATH = "src/test/resources/in.txt";

	public static void main(String... args) {
		// source is a tab separated local file
		Tap source = new Hfs(new TextDelimited(true, "\t"), IN_PATH);
		
		// sink is multiple tab separated local files
		Fields fields = new Fields( "groupId", "group", "count" );
		Hfs simpleSink = new Hfs(new TextDelimited(fields, "\t"), OUT_PATH, SinkMode.REPLACE);
		Tap sink = new TemplateTap( simpleSink, "%s", SinkMode.REPLACE );
		
		// split text into token
		RegexSplitGenerator splitter = new RegexSplitGenerator(new Fields("token"), "[ \\[\\]\\(\\),.]");
		Pipe pipe = new Each("main", new Fields("text"), splitter, Fields.ALL);
		
		// dispatch with different grouping strategies
		pipe = new Each(pipe, new GroupSplitter(new Fields("groupId", "group")), Fields.ALL);
		
		// count
		pipe = new CountBy(pipe, new Fields("groupId", "group"), new Fields("count"));

		// define and launch
		run(FlowDef.flowDef().addSource(pipe, source).addTailSink(pipe, sink));
	}

	private static void run(FlowDef flowDef) {
		// create connector
		Properties properties = new Properties();
		AppProps.setApplicationJarClass(properties, App.class);
		HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
		
		// create flow
		Flow flow = flowConnector.connect(flowDef);
		
		// write graph representation of the job
		flow.writeDOT(OUT_PATH + "_dot/job.dot");
		flow.writeStepsDOT(OUT_PATH + "_dot/job-steps.dot");
		
		// launch
		flow.complete();
	}
	
	private static class GroupSplitter extends BaseOperation implements Function {
		
		public GroupSplitter(Fields fields) {
			super(fields);
		}

		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			TupleEntry argumentTuples = functionCall.getArguments();
			Comparable token = argumentTuples.get("token");
			Comparable docId = argumentTuples.get("doc_id");
			
			Tuple tuple1 = new Tuple("countAll",new Tuple());
			Tuple tuple3 = new Tuple("countPerDoc",new Tuple(docId));
			Tuple tuple2 = new Tuple("countPerToken",new Tuple(token));
			Tuple tuple4 = new Tuple("countPerTokenAndDoc",new Tuple(token, docId));
			
			TupleEntryCollector outputCollector = functionCall.getOutputCollector();
			outputCollector.add(tuple1);
			outputCollector.add(tuple2);
			outputCollector.add(tuple3);
			outputCollector.add(tuple4);
		}
		
	}
}
