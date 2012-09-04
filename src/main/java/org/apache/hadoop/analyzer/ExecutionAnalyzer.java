package org.apache.hadoop.analyzer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.tools.rumen.Histogram;
import org.apache.hadoop.tools.rumen.LoggedDiscreteCDF;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedLocation;
import org.apache.hadoop.tools.rumen.LoggedSingleRelativeRanking;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants;
import org.codehaus.jackson.map.ObjectMapper;
import org.rosuda.JRI.Rengine;

//TODO Quais jobs tiveram falhas das máquinas?
//TODO Jobs com execução em nó que não tem o dado ocorreram devido a falha nos nós que tinham o dado?
//TODO Qual o impacto no tempo de execução causado por 2% das execuções serem com dados não locais?

public class ExecutionAnalyzer {

//	private static final String path = "/home/thiago/tmp/experiment/1st_experiment/";
		private static final String path = "/home/thiago/tmp/experiment/2nd_experiment/10x/";

	private static Rengine r_instance;

	private static final String CountUpDriver = "CountUpDriver";
	private static final String CountUp = "CountUp";
	private static final String JxtaSocketPerfDriver = "JxtaSocketPerfDriver";

	private static enum PrintMode {prettyPrint, inline, fullInline, disabled};

	private static List<LoggedJob> cup32MinJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> cup64MinJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> cup128MinJobs = new ArrayList<LoggedJob>();

	private static List<LoggedJob> cup32MaxJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> cup64MaxJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> cup128MaxJobs = new ArrayList<LoggedJob>();

	private static List<LoggedJob> cupd32MinJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> cupd64MinJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> cupd128MinJobs = new ArrayList<LoggedJob>();

	private static List<LoggedJob> cupd32MaxJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> cupd64MaxJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> cupd128MaxJobs = new ArrayList<LoggedJob>();

	private static List<LoggedJob> jxta32MinJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> jxta64MinJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> jxta128MinJobs = new ArrayList<LoggedJob>();

	private static List<LoggedJob> jxta32MaxJobs = new ArrayList<LoggedJob>();
	private static List<LoggedJob> jxta64MaxJobs = new ArrayList<LoggedJob>();	
	private static List<LoggedJob> jxta128MaxJobs = new ArrayList<LoggedJob>();

	static{
		String[] args = new String[1];
		args[0] = "--save";
		r_instance = new Rengine(args, false, null);
	}

	public static void main(String... args) throws Exception {
		// Data Files				
		File resultPath = new File(path + "results/jxta/");
		resultPath.mkdir();
		String traceFilePath = null;

		FileWriter minFile = new FileWriter(resultPath + "/JxtaMin.dat");
		BufferedWriter minFileWriter = new BufferedWriter(minFile);		
		FileWriter maxFile = new FileWriter(resultPath + "/JxtaMax.dat");
		BufferedWriter maxFileWriter = new BufferedWriter(maxFile);

		// Histogram
		Histogram[] jxta32MinTimes = new Histogram[2];
		jxta32MinTimes[0] = new Histogram();
		jxta32MinTimes[1] = new Histogram();
		Histogram[] jxta64MinTimes = new Histogram[2];
		jxta64MinTimes[0] = new Histogram();
		jxta64MinTimes[1] = new Histogram();
		Histogram[] jxta128MinTimes = new Histogram[2];
		jxta128MinTimes[0] = new Histogram();
		jxta128MinTimes[1] = new Histogram();
		Histogram[] jxta32MaxTimes = new Histogram[2];
		jxta32MaxTimes[0] = new Histogram();
		jxta32MaxTimes[1] = new Histogram();
		Histogram[] jxta64MaxTimes = new Histogram[2];
		jxta64MaxTimes[0] = new Histogram();
		jxta64MaxTimes[1] = new Histogram();
		Histogram[] jxta128MaxTimes = new Histogram[2];
		jxta128MaxTimes[0] = new Histogram();
		jxta128MaxTimes[1] = new Histogram();

		System.out.println("### Min");
		ObjectMapper mapper = new ObjectMapper();		
		for (int i = 3; i <= 30; i++) {
			try{
				if(i > 9){
					traceFilePath = path + "traces/" + i + "-job-trace.json";
				}else{
					traceFilePath = path + "traces/" + "0" + i + "-job-trace.json";
				}					

				Iterator<LoggedJob> jobs = mapper.reader(LoggedJob.class).readValues(new File(traceFilePath));
				while (jobs.hasNext()) {
					classifyJob(jobs.next());
				}

				System.out.print(i);
				minFileWriter.write(String.valueOf(i));

				evaluate(jxta32MinJobs, PrintMode.fullInline, minFileWriter, jxta32MinTimes);
				evaluate(jxta64MinJobs, PrintMode.fullInline, minFileWriter, jxta64MinTimes);
				evaluate(jxta128MinJobs, PrintMode.fullInline, minFileWriter, jxta128MinTimes);

				System.out.println();
				minFileWriter.newLine();

				jxta32MinJobs = new ArrayList<LoggedJob>();
				jxta64MinJobs = new ArrayList<LoggedJob>();
				jxta128MinJobs = new ArrayList<LoggedJob>();
			}catch(FileNotFoundException e1){
				continue;
			}catch(Exception e){
				e.printStackTrace();
				continue;
			}
		}		
		System.out.println();		
		try {
			minFileWriter.close();
			minFile.close();			
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		System.out.println("### Max");
		mapper = new ObjectMapper();		
		for (int i = 3; i <= 30; i++) {
			try{
				if(i > 9){
					traceFilePath = path + "traces/" + i + "-job-trace.json";
				}else{
					traceFilePath = path + "traces/" + "0" + i + "-job-trace.json";
				}					

				Iterator<LoggedJob> jobs = mapper.reader(LoggedJob.class).readValues(new File(traceFilePath));
				while (jobs.hasNext()) {
					classifyJob(jobs.next());
				}

				System.out.print(i);
				maxFileWriter.write(String.valueOf(i));

				evaluate(jxta32MaxJobs,PrintMode.fullInline, maxFileWriter, jxta32MaxTimes);
				evaluate(jxta64MaxJobs,PrintMode.fullInline, maxFileWriter, jxta64MaxTimes);
				evaluate(jxta128MaxJobs,PrintMode.fullInline, maxFileWriter, jxta128MaxTimes);

				System.out.println();
				maxFileWriter.newLine();

				jxta32MaxJobs = new ArrayList<LoggedJob>();
				jxta64MaxJobs = new ArrayList<LoggedJob>();
				jxta128MaxJobs = new ArrayList<LoggedJob>();
			}catch(Exception e){
				continue;
			}
		}
		System.out.println();
		try {
			maxFileWriter.close();
			maxFile.close();			
		} catch (IOException ex) {
			ex.printStackTrace();
		}					

		plotGraphs(resultPath);

		plotCDFs(resultPath, jxta32MinTimes, jxta64MinTimes, jxta32MaxTimes,jxta64MaxTimes);

		r_instance.end();
	}

	private static void plotCDFs(File resultPath, Histogram[] jxta32MinTimes,
			Histogram[] jxta64MinTimes, Histogram[] jxta32MaxTimes,
			Histogram[] jxta64MaxTimes) throws IOException {
		System.out.println("### JxtaMin32MapTimeCDF");
		plotCDF(jxta32MinTimes[0], resultPath.getAbsolutePath(), "32MinMapCDF");
		System.out.println();

		System.out.println("### JxtaMin32ReduceTimeCDF");
		plotCDF(jxta32MinTimes[1], resultPath.getAbsolutePath(), "32MinReduceCDF");
		System.out.println();

		System.out.println("\n### JxtaMin64MapTimeCDF");
		plotCDF(jxta64MinTimes[0], resultPath.getAbsolutePath(), "64MinMapCDF");
		System.out.println();

		System.out.println("\n### JxtaMin64ReduceTimeCDF");
		plotCDF(jxta64MinTimes[1], resultPath.getAbsolutePath(), "64MinReduceCDF");
		System.out.println();

		System.out.println("\n### JxtaMax32MapTimeCDF");
		plotCDF(jxta32MaxTimes[0], resultPath.getAbsolutePath(), "32MaxMapCDF");
		System.out.println();

		System.out.println("\n### JxtaMax32ReduceTimeCDF");
		plotCDF(jxta32MaxTimes[1], resultPath.getAbsolutePath(), "32MaxReduceCDF");
		System.out.println();

		System.out.println("\n### JxtaMax64MapTimeCDF");
		plotCDF(jxta64MaxTimes[0], resultPath.getAbsolutePath(), "64MaxMapCDF");
		System.out.println();

		System.out.println("\n### JxtaMax64ReduceTimeCDF");
		plotCDF(jxta64MaxTimes[1], resultPath.getAbsolutePath(), "64MaxReduceCDF");
		System.out.println();
	}

	private static void plotCDF(Histogram times, String resultPath, String name) throws IOException {
		LoggedDiscreteCDF cdf = new LoggedDiscreteCDF();
		int[] attemptTimesPercentiles = new int[99];
		for (int i = 0; i < 99; ++i) {
			attemptTimesPercentiles[i] = (i + 1) * 1;
		}	

		List<LoggedSingleRelativeRanking> lsrrList;
		FileWriter dataFile = new FileWriter(new File(resultPath + "/" + name + ".dat"));
		BufferedWriter writer = new BufferedWriter(dataFile);		
		cdf.setCDF(times, attemptTimesPercentiles, 100);		
		lsrrList = cdf.getRankings();
		if(lsrrList != null && lsrrList.size() > 0){
			for (LoggedSingleRelativeRanking lsrr : lsrrList) {
				String tmp = lsrr.getDatum() + " " + lsrr.getRelativeRanking();
				System.out.println(tmp);
				writer.write(tmp);
				writer.newLine();
			}
		}
		try {
			writer.close();		
			dataFile.close();				
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		try{
			r_instance.eval("data <- read.table(\"" + resultPath + "/" + name +  ".dat\", header=F, sep=\" \")");
			r_instance.eval("xs <- data$V1");
			r_instance.eval("xf <- data$V2");
			r_instance.eval("png(filename=\"" + resultPath + "/" + name +  ".png\")");
			r_instance.eval("plot(xs, xf, xlab=\"Runtime(ms)\", ylab=\"Cumulative frequency\")");
			r_instance.eval("dev.off()");
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void plotGraphs(File resultPath) {
		plotJobCompletionTime(resultPath, "JxtaMin", "JxtaMinJobCompletion");
		plotJobCompletionTime(resultPath, "JxtaMax", "JxtaMaxJobCompletion");

		plotJobLocality(resultPath, "JxtaMin", "JxtaMinJobLocality");
		plotJobLocality(resultPath, "JxtaMax", "JxtaMaxJobLocality");

		plotMapLocality(resultPath, "JxtaMin", "JxtaMinMapLocality");
		plotMapLocality(resultPath, "JxtaMax", "JxtaMaxMapLocality");

		plotMapReduceCompletionTime(resultPath, "JxtaMin", "JxtaMinMapReduceCompletion");
		plotMapReduceCompletionTime(resultPath, "JxtaMax", "JxtaMaxMapReduceCompletion");
	}

	private static void plotJobCompletionTime(File destPath, String sourceName, String destName){
		String strCommands = "reset;" + 
				"set yrange [0:450];" +
				"set xrange [0:32];" +
				"set ylabel \"Runtime(s)\";" +
				"set xlabel \"Nodes\";" +
				"set terminal png;" +
				"set output \"" + destName + ".png\";" +
				"plot '" + sourceName + ".dat' using 1:2:3 		axis x1y1 title \"Job Runtime(32MB)\" w yerrorlines, " +
				"'" + sourceName + ".dat' using 1:40:41 	axis x1y1 title \"Job Runtime(64MB)\" w yerrorlines;" +
				"set terminal x11;" +
				"set output;";		
		try {
			ProcessBuilder process = new ProcessBuilder("gnuplot", "-e", strCommands);
			process.directory(destPath);
			process.start();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void plotJobLocality(File destPath, String sourceName, String destName){
		String strCommands = "reset;" + 
				"set yrange [0:450];" + 
				"set xrange [0:32];" + 
				"set y2range [0:50];" + 
				"set ytics nomirror;" + 
				"set y2tics;" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"set y2label \"Non-locality\";" + 
				"set terminal png;" + 
				"set output \"" + destName + ".png\";" + 
				"plot '" + sourceName + ".dat' using 1:2:3 		axis x1y1 title \"Job Runtime(32MB)\" w yerrorlines," +
				" '" + sourceName + ".dat' using 1:40:41 	axis x1y1 title \"Job Runtime(64MB)\" w yerrorlines," +
				" '" + sourceName + ".dat' using 1:4:5 		axis x1y2 title \"Map Non-locality(32MB)\" w linesp," +
				" '" + sourceName + ".dat' using 1:42:43 	axis x1y2 title \"Map Non-locality(64MB)\" w linesp," +
				" '" + sourceName + ".dat' using 1:8:9 		axis x1y2 title \"Reduce Non-locality(32MB)\" w linesp," +
				" '" + sourceName + ".dat' using 1:46:47 	axis x1y2 title \"Reduce Non-locality(64MB)\" w linesp;" + 
				"set terminal x11;" + 
				"set output";		
		try {
			ProcessBuilder process = new ProcessBuilder("gnuplot", "-e", strCommands);
			process.directory(destPath);
			process.start();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void plotMapLocality(File destPath, String sourceName, String destName){
		String strCommands = "reset;" + 
				"set yrange [0:50];" +
				"set xrange [0:32];" +
				"set y2range [0:50];" +
				"set ytics nomirror;" + 
				"set y2tics;" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"set y2label \"Non-locality\";" + 
				"set key top left reverse Left;" + 
				"set terminal png;" + 
				"set output \"" + destName + ".png\";" + 
				"plot '" + sourceName + ".dat' using 1:14:15 axis x1y1 title \"Map Meantime(32MB)\" w yerrorlines," + 
				" '" + sourceName + ".dat' using 1:52:53 axis x1y1 title \"Map Meantime(64MB)\" w yerrorlines," + 
				" '" + sourceName + ".dat' using 1:16:17 axis x1y2 title \"Map Non-locality(32MB)\" w yerrorlines," + 
				" '" + sourceName + ".dat' using 1:54:55 axis x1y2 title \"Map Non-locality(64MB)\" w yerrorlines;" + 
				"set terminal x11;" + 
				"set output";		
		try {
			ProcessBuilder process = new ProcessBuilder("gnuplot", "-e", strCommands);
			process.directory(destPath);
			process.start();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void plotMapReduceCompletionTime(File destPath, String sourceName, String destName){
		String strCommands = "reset;" + 
				"set yrange [0:400];" + 
				"set xrange [0:32];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"set terminal png;" + 
				"set output \"" + destName + ".png\";" + 
				"plot '" + sourceName + ".dat' using 1:14:15 axis x1y1 title \"Map Meantime(32MB)\" w yerrorlines," + 
				" '" + sourceName + ".dat' using 1:52:53 axis x1y1 title \"Map Meantime(64MB)\" w yerrorlines," +  
				" '" + sourceName + ".dat' using 1:28:29 axis x1y1 title \"Reduce Meantime(32MB)\" w yerrorlines," + 
				" '" + sourceName + ".dat' using 1:66:67 axis x1y1 title \"Reduce Meantime(64MB)\" w yerrorlines;" + 
				"set terminal x11;" + 
				"set output";
		try {
			ProcessBuilder process = new ProcessBuilder("gnuplot", "-e", strCommands);
			process.directory(destPath);
			process.start();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void evaluate(List<LoggedJob> jobs, PrintMode printMode, BufferedWriter writer, Histogram[] histograms){
		DescriptiveStatistics jobRuntimeStats = new DescriptiveStatistics();
		DescriptiveStatistics jobNotLocalMapAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics jobTotalMapAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics jobNotLocalReduceAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics jobTotalReduceAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics mapTasksStats = new DescriptiveStatistics();
		DescriptiveStatistics mapTaskTimeStats = new DescriptiveStatistics();		
		DescriptiveStatistics mapNotLocalAttemptStats = new DescriptiveStatistics();		
		DescriptiveStatistics mapSuccessAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics mapFailedAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics mapKilledAttemptStats = new DescriptiveStatistics();		
		DescriptiveStatistics mapAttemptStats = new DescriptiveStatistics();		
		DescriptiveStatistics reduceTasksStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceTaskTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceNotLocalAttemptStats = new DescriptiveStatistics();		
		DescriptiveStatistics reduceSuccessAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceFailedAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceKilledAttemptStats = new DescriptiveStatistics();		
		DescriptiveStatistics reduceAttemptStats = new DescriptiveStatistics();

		for (LoggedJob job : jobs) {			
			jobRuntimeStats.addValue(job.getFinishTime() - job.getLaunchTime());

			// Map
			List<LoggedTask> mapTasks = job.getMapTasks();			
			getTaskStatistics(jobNotLocalMapAttemptStats, jobTotalMapAttemptStats,
					mapTasksStats, mapTaskTimeStats, mapNotLocalAttemptStats,
					mapSuccessAttemptStats, mapFailedAttemptStats,
					mapKilledAttemptStats, mapAttemptStats, histograms[0], mapTasks);

			// Reduce
			List<LoggedTask> reduceTasks = job.getReduceTasks();			
			getTaskStatistics(jobNotLocalReduceAttemptStats, jobTotalReduceAttemptStats,
					reduceTasksStats, reduceTaskTimeStats, reduceNotLocalAttemptStats,
					reduceSuccessAttemptStats, reduceFailedAttemptStats,
					reduceKilledAttemptStats, reduceAttemptStats, histograms[1], reduceTasks);		
		}

		if(printMode == PrintMode.inline){
			printInlineStats(jobRuntimeStats, jobNotLocalMapAttemptStats, jobTotalMapAttemptStats, jobNotLocalReduceAttemptStats, 
					jobTotalReduceAttemptStats, mapTasksStats, mapTaskTimeStats, mapNotLocalAttemptStats, mapSuccessAttemptStats, 
					mapFailedAttemptStats, mapKilledAttemptStats, mapAttemptStats, reduceTasksStats, reduceTaskTimeStats, 
					reduceNotLocalAttemptStats, reduceSuccessAttemptStats, reduceFailedAttemptStats, reduceKilledAttemptStats, reduceAttemptStats, 
					writer);	
		} else if(printMode == PrintMode.fullInline){
			printInlineFullStats(jobRuntimeStats, jobNotLocalMapAttemptStats, jobTotalMapAttemptStats, jobNotLocalReduceAttemptStats, 
					jobTotalReduceAttemptStats, mapTasksStats, mapTaskTimeStats, mapNotLocalAttemptStats, mapSuccessAttemptStats, 
					mapFailedAttemptStats, mapKilledAttemptStats, mapAttemptStats, reduceTasksStats, reduceTaskTimeStats, 
					reduceNotLocalAttemptStats, reduceSuccessAttemptStats, reduceFailedAttemptStats, reduceKilledAttemptStats, reduceAttemptStats, 
					writer);
		}

	}

	private static void getTaskStatistics(
			DescriptiveStatistics jobNotLocalAttemptStats,
			DescriptiveStatistics jobTotalAttemptStats,
			DescriptiveStatistics numTasksStats,
			DescriptiveStatistics taskTimeStats,
			DescriptiveStatistics notLocalAttemptStats,
			DescriptiveStatistics successAttemptStats,
			DescriptiveStatistics failedAttemptStats,
			DescriptiveStatistics killedAttemptStats,
			DescriptiveStatistics totalAttemptStats, 
			Histogram attemptTimes,			
			List<LoggedTask> tasks) {

		double jobNotLocalAttempt = 0;
		double jobTotalAttempt = 0;

		if(tasks != null){
			numTasksStats.addValue(tasks.size());
			for (LoggedTask mapTask : tasks) {					
				if(mapTask.getTaskStatus() != Pre21JobHistoryConstants.Values.SUCCESS){
					System.out.println(mapTask.getTaskID() + " - " +  mapTask.getTaskStatus());
				}					
				double mapNotLocalAttempt = 0;
				double mapSuccessAttempt = 0;
				double mapFailedAttempt = 0;
				double mapKilledAttempt = 0;
				taskTimeStats.addValue(mapTask.getFinishTime() - mapTask.getStartTime());										
				List<LoggedLocation> dataLocations = mapTask.getPreferredLocations();
				List<LoggedTaskAttempt> attemptList = mapTask.getAttempts();
				// Attempts
				if(attemptList != null){
					totalAttemptStats.addValue(attemptList.size());
					jobTotalAttempt += attemptList.size();
					for (LoggedTaskAttempt mapAttempt : attemptList) {

						if(mapAttempt.getResult() == Pre21JobHistoryConstants.Values.SUCCESS){
							mapSuccessAttempt++;
						} else if(mapAttempt.getResult() == Pre21JobHistoryConstants.Values.KILLED){
							mapKilledAttempt++;
						} else if(mapAttempt.getResult() == Pre21JobHistoryConstants.Values.FAILED){
							mapFailedAttempt++;
						}

						if(mapAttempt.getLocation() != null){
							boolean isLocal = false;
							for (LoggedLocation location : dataLocations) {
								isLocal = isLocal || location.getLayers().equals(mapAttempt.getLocation().getLayers());								
							} 
							if(!isLocal){
								mapNotLocalAttempt++;
								jobNotLocalAttempt++;
							}
						}else{
							if(mapAttempt.getResult() != Pre21JobHistoryConstants.Values.KILLED){
								System.out.println("### Map Attempt Location Null: " + mapAttempt.getAttemptID());
							}
						}
						if((mapAttempt.getFinishTime() - mapAttempt.getStartTime()) > 0){
							attemptTimes.enter(mapAttempt.getFinishTime() - mapAttempt.getStartTime());
						}
					}						
				}					
				notLocalAttemptStats.addValue(mapNotLocalAttempt);
				successAttemptStats.addValue(mapSuccessAttempt);
				killedAttemptStats.addValue(mapKilledAttempt);
				failedAttemptStats.addValue(mapFailedAttempt);					
			}
		}
		jobNotLocalAttemptStats.addValue(jobNotLocalAttempt);
		jobTotalAttemptStats.addValue(jobTotalAttempt);
	}	

	private static void classifyJob(LoggedJob job){

		try{
			StringTokenizer jobJame = new StringTokenizer(job.getJobName()," ");		
			String jobType = (String)jobJame.nextElement();
			jobJame.nextElement();
			jobJame.nextElement();
			jobJame.nextElement();
			String jobSize = (String) jobJame.nextElement();

			if(jobType.equals("JxtaSocketPerfDriver")){
				if(jobSize.equals("/home/thiago/input/jxta-socket/max/128-90/")){
					jxta128MaxJobs.add(job);
				} else if(jobSize.equals("/home/thiago/input/jxta-socket/max/64-180/")){
					jxta64MaxJobs.add(job);
				} else if(jobSize.equals("/home/thiago/input/jxta-socket/max/32-360/")){
					jxta32MaxJobs.add(job);
				} else if(jobSize.equals("/home/thiago/input/jxta-socket/min/128-30/")){
					jxta128MinJobs.add(job);
				} else if(jobSize.equals("/home/thiago/input/jxta-socket/min/64-60/")){
					jxta64MinJobs.add(job);
				} else if(jobSize.equals("/home/thiago/input/jxta-socket/min/32-120/")){
					jxta32MinJobs.add(job);
				}else {
					System.out.println("### JxtaSocketPerfDriver size unexpected: " + jobSize);				
					JobExecutionViewer.jobPrettyPrint(job);
				}
			} else {
				System.out.println("### Job type unknown!");
				JobExecutionViewer.jobPrettyPrint(job);
			}
		} catch(Exception e){
			oldClassifier(job);
		}
	}

	private static void oldClassifier(LoggedJob job) {
		List<LoggedTask> mapTasks = job.getMapTasks();
		if(mapTasks != null){
			long inputSize = mapTasks.get(0).getInputBytes();
			if(inputSize > 0){
				if(inputSize > 31000000 && inputSize < 33000000){
					if(mapTasks.size() == 120){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							jxta32MinJobs.add(job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							cupd32MinJobs.add(job);	
						}else if(job.getJobName().equals(CountUp)){
							cup32MinJobs.add(job);	
						}							
					}else if(mapTasks.size() == 360){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							jxta32MaxJobs.add(job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							cupd32MaxJobs.add(job);	
						}else if(job.getJobName().equals(CountUp)){
							cup32MaxJobs.add(job);	
						}
					}else {
						System.out.println("### Number of Tasks Error: " + job.getJobID());
					}
				}else if(inputSize > 63000000 && inputSize < 65000000){
					if(mapTasks.size() == 60){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							jxta64MinJobs.add(job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							cupd64MinJobs.add(job);	
						}else if(job.getJobName().equals(CountUp)){
							cup64MinJobs.add(job);	
						}
					}else if(mapTasks.size() == 180){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							jxta64MaxJobs.add(job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							cupd64MaxJobs.add(job);	
						}else if(job.getJobName().equals(CountUp)){
							cup64MaxJobs.add(job);	
						}
					}else{
						System.out.println("### Number of Tasks Error: " + job.getJobID());
					}
				}else if(inputSize > 127000000 && inputSize < 129000000){
					if(mapTasks.size() == 30){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							jxta128MinJobs.add(job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							cupd128MinJobs.add(job);	
						}else if(job.getJobName().equals(CountUp)){
							cup128MinJobs.add(job);	
						}
					}else if(mapTasks.size() == 90){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							jxta128MaxJobs.add(job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							cupd128MaxJobs.add(job);	
						}else if(job.getJobName().equals(CountUp)){
							cup128MaxJobs.add(job);	
						}
					}else{
						System.out.println("### Number of Tasks Error: " + job.getJobID());
					}
				}else {
					System.out.println("### Map Input Size Error: " + job.getJobID());
					System.out.println(inputSize);
				}
			}else{
				System.out.println("### Map Input Error : " + job.getJobID());
			}
		}
	}

	private static void printInlineStats(
			DescriptiveStatistics jobRuntimeStats,
			DescriptiveStatistics jobNotLocalMapAttemptStats,
			DescriptiveStatistics jobTotalMapAttemptStats,
			DescriptiveStatistics jobNotLocalReduceAttemptStats,
			DescriptiveStatistics jobTotalReduceAttemptStats,
			DescriptiveStatistics mapTasksStats,
			DescriptiveStatistics mapTaskTimeStats,		
			DescriptiveStatistics mapNotLocalAttemptStats,		
			DescriptiveStatistics mapSuccessAttemptStats,
			DescriptiveStatistics mapFailedAttemptStats,
			DescriptiveStatistics mapKilledAttemptStats,		
			DescriptiveStatistics mapAttemptStats,		
			DescriptiveStatistics reduceTasksStats,
			DescriptiveStatistics reduceTaskTimeStats,
			DescriptiveStatistics reduceNotLocalAttemptStats,		
			DescriptiveStatistics reduceSuccessAttemptStats,
			DescriptiveStatistics reduceFailedAttemptStats,
			DescriptiveStatistics reduceKilledAttemptStats,		
			DescriptiveStatistics reduceAttemptStats, 
			BufferedWriter writer) {

		StringBuilder output = new StringBuilder();
		DecimalFormat df2 = new DecimalFormat( "#,###,###,##0.00" );

		output.append("\t" + df2.format(jobRuntimeStats.getMean()));
		output.append("\t" + df2.format(jobNotLocalMapAttemptStats.getMean()));
		output.append("\t" + df2.format(jobTotalMapAttemptStats.getMean()));
		output.append("\t" + df2.format(jobNotLocalReduceAttemptStats.getMean()));
		output.append("\t" + df2.format(jobTotalReduceAttemptStats.getMean()));

		output.append("\t" + df2.format(mapTasksStats.getMean()));
		output.append("\t" + df2.format(mapTaskTimeStats.getMean()));
		output.append("\t" + df2.format(mapNotLocalAttemptStats.getMean()));		
		output.append("\t" + df2.format(mapSuccessAttemptStats.getMean()));
		output.append("\t" + df2.format(mapKilledAttemptStats.getMean()));
		output.append("\t" + df2.format(mapFailedAttemptStats.getMean()));		
		output.append("\t" + df2.format(mapAttemptStats.getMean()));		

		output.append("\t" + df2.format(reduceTasksStats.getMean()));
		output.append("\t" + df2.format(reduceTaskTimeStats.getMean()));
		output.append("\t" + df2.format(reduceNotLocalAttemptStats.getMean()));
		output.append("\t" + df2.format(reduceSuccessAttemptStats.getMean()));
		output.append("\t" + df2.format(reduceKilledAttemptStats.getMean()));
		output.append("\t" + df2.format(reduceFailedAttemptStats.getMean()));		
		output.append("\t" + df2.format(reduceAttemptStats.getMean()));		

		System.out.print(output);

		if(writer != null){			
			try {				
				writer.write(output.toString());
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
	}

	private static void printInlineFullStats(DescriptiveStatistics jobRuntimeStats,
			DescriptiveStatistics jobNotLocalMapAttemptStats,
			DescriptiveStatistics jobTotalMapAttemptStats,
			DescriptiveStatistics jobNotLocalReduceAttemptStats,
			DescriptiveStatistics jobTotalReduceAttemptStats,
			DescriptiveStatistics mapTasksStats,
			DescriptiveStatistics mapTaskTimeStats,		
			DescriptiveStatistics mapNotLocalAttemptStats,		
			DescriptiveStatistics mapSuccessAttemptStats,
			DescriptiveStatistics mapFailedAttemptStats,
			DescriptiveStatistics mapKilledAttemptStats,		
			DescriptiveStatistics mapAttemptStats,		
			DescriptiveStatistics reduceNumTasksStats,
			DescriptiveStatistics reduceTaskTimeStats,
			DescriptiveStatistics reduceNotLocalAttemptStats,		
			DescriptiveStatistics reduceSuccessAttemptStats,
			DescriptiveStatistics reduceFailedAttemptStats,
			DescriptiveStatistics reduceKilledAttemptStats,		
			DescriptiveStatistics reduceAttemptStats, 
			BufferedWriter writer) {

		StringBuilder output = new StringBuilder();
		DecimalFormat df2 = new DecimalFormat( "#,###,###,##0.00" );

		output.append("\t" + df2.format(jobRuntimeStats.getMean()) + "\t" + df2.format((jobRuntimeStats.getStandardDeviation()/jobRuntimeStats.getMean()) * 100));
		output.append("\t" + df2.format((jobNotLocalMapAttemptStats.getMean()/jobTotalMapAttemptStats.getMean())*100) + "\t" + df2.format((jobNotLocalMapAttemptStats.getStandardDeviation()/jobNotLocalMapAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format(jobTotalMapAttemptStats.getMean()) + "\t" + df2.format((jobTotalMapAttemptStats.getStandardDeviation()/jobTotalMapAttemptStats.getMean()) * 100));		
		output.append("\t" + df2.format((jobNotLocalReduceAttemptStats.getMean()/jobTotalReduceAttemptStats.getMean())*100) + "\t" + df2.format((jobNotLocalReduceAttemptStats.getStandardDeviation()/jobNotLocalReduceAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format(jobTotalReduceAttemptStats.getMean()) + "\t" + df2.format((jobTotalReduceAttemptStats.getStandardDeviation()/jobTotalReduceAttemptStats.getMean()) * 100));		

		output.append("\t" + df2.format(mapTasksStats.getMean()) + "\t" + df2.format((mapTasksStats.getStandardDeviation()/mapTasksStats.getMean()) * 100));
		output.append("\t" + df2.format(mapTaskTimeStats.getMean()) + "\t" + df2.format((mapTaskTimeStats.getStandardDeviation()/mapTaskTimeStats.getMean()) * 100));
		output.append("\t" + df2.format((mapNotLocalAttemptStats.getMean()/mapAttemptStats.getMean())*100) + "\t" + df2.format((mapNotLocalAttemptStats.getStandardDeviation()/mapNotLocalAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format((mapSuccessAttemptStats.getMean()/mapAttemptStats.getMean())*100) + "\t" + df2.format((mapSuccessAttemptStats.getStandardDeviation()/mapSuccessAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format((mapKilledAttemptStats.getMean()/mapAttemptStats.getMean())*100) + "\t" + df2.format((mapKilledAttemptStats.getStandardDeviation()/mapKilledAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format((mapFailedAttemptStats.getMean()/mapAttemptStats.getMean())*100) + "\t" + df2.format((mapFailedAttemptStats.getStandardDeviation()/mapFailedAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format(mapAttemptStats.getMean()) + "\t" + df2.format((mapAttemptStats.getStandardDeviation()/mapAttemptStats.getMean()) * 100));

		output.append("\t" + df2.format(reduceNumTasksStats.getMean()) + "\t" + df2.format((reduceNumTasksStats.getStandardDeviation()/reduceNumTasksStats.getMean()) * 100));
		output.append("\t" + df2.format(reduceTaskTimeStats.getMean()) + "\t" + df2.format((reduceTaskTimeStats.getStandardDeviation()/reduceTaskTimeStats.getMean()) * 100));		
		output.append("\t" + df2.format((reduceNotLocalAttemptStats.getMean()/reduceAttemptStats.getMean())*100) + "\t" + df2.format((reduceNotLocalAttemptStats.getStandardDeviation()/reduceNotLocalAttemptStats.getMean()) * 100));		
		output.append("\t" + df2.format((reduceSuccessAttemptStats.getMean()/reduceAttemptStats.getMean())*100) + "\t" + df2.format((reduceSuccessAttemptStats.getStandardDeviation()/reduceSuccessAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format((reduceKilledAttemptStats.getMean()/reduceAttemptStats.getMean())*100) + "\t" + df2.format((reduceKilledAttemptStats.getStandardDeviation()/reduceKilledAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format((reduceFailedAttemptStats.getMean()/reduceAttemptStats.getMean())*100) + "\t" + df2.format((reduceFailedAttemptStats.getStandardDeviation()/reduceFailedAttemptStats.getMean()) * 100));		
		output.append("\t" + df2.format(reduceAttemptStats.getMean()) + "\t" + df2.format(reduceAttemptStats.getStandardDeviation()));

		System.out.print(output);

		if(writer != null){
			try {
				writer.write(output.toString());
				writer.flush();
			} catch (IOException e) {
				e.printStackTrace();
			} 		
		}
	}
}