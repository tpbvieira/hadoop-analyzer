package org.apache.hadoop.analyzer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

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

//TODO Quais as attempts que apresentam valores fora do comum? Por quê?

public class ExecutionAnalyzer {

	//	private static final String path = "/home/thiago/tmp/experiment/1st_experiment/10x/";
	//	private static final String path = "/home/thiago/tmp/experiment/2nd_experiment/10x/";
	private static final String path = "/home/thiago/tmp/experiment/2nd_experiment/20x/";

	private static final String tracePath = path + "traces/";
	private static final String resultPath = path + "results/";

	private static Rengine r_instance;
	private static final String gnuplotTerminal = "postscript eps enhanced";
	private static final String gnuplotOutType = ".eps";

	private static final String CountUpDriver = "CountUpDriver";
	private static final String CountUpText = "CountUpText";
	private static final String CountUp = "CountUp";
	private static final String JxtaSocketPerfDriver = "JxtaSocketPerfDriver";

	private static enum PrintMode {prettyPrint, inline, fullInline, disabled};

	private static SortedMap<Integer,List<LoggedJob>> cup32MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cup64MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cup128MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();

	private static SortedMap<Integer,List<LoggedJob>> cup32MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cup64MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cup128MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();

	private static SortedMap<Integer,List<LoggedJob>> cupd32MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cupd64MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cupd128MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();

	private static SortedMap<Integer,List<LoggedJob>> cupd32MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cupd64MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cupd128MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();

	private static SortedMap<Integer,List<LoggedJob>> cupt32MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cupt64MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cupt128MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();

	private static SortedMap<Integer,List<LoggedJob>> cupt32MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cupt64MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> cupt128MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();

	private static SortedMap<Integer,List<LoggedJob>> jxta32MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> jxta64MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> jxta128MinJobsMap = new TreeMap<Integer,List<LoggedJob>>();

	private static SortedMap<Integer,List<LoggedJob>> jxta32MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();
	private static SortedMap<Integer,List<LoggedJob>> jxta64MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();	
	private static SortedMap<Integer,List<LoggedJob>> jxta128MaxJobsMap = new TreeMap<Integer,List<LoggedJob>>();

	private static double jobTime = 0;
	private static LoggedJob currJob;

	// R default arguments
	static{
		String[] args = new String[1];
		args[0] = "--save";
		r_instance = new Rengine(args, false, null);
	}

	public static void main(String... args) throws Exception {		 
		File resultDir = new File(resultPath);
		resultDir.mkdir();

		SortedMap<Integer,File> traceFiles = selectTraceFiles(tracePath);		
		classifyJobs(traceFiles);

		String prefix = "JxtaMin";
		System.out.println("### " + prefix);
		analyzeJobs(jxta32MinJobsMap, jxta64MinJobsMap, jxta128MinJobsMap, resultPath, prefix);

		prefix = "JxtaMax";
		System.out.println("### " + prefix);
		analyzeJobs(jxta32MaxJobsMap, jxta64MaxJobsMap, jxta128MaxJobsMap, resultPath, prefix);

		prefix = "CountUpMin";
		System.out.println("### " + prefix);
		analyzeJobs(cup32MinJobsMap, cup64MinJobsMap, cup128MinJobsMap, resultPath, prefix);

		prefix = "CountUpMax";
		System.out.println("### " + prefix);
		analyzeJobs(cup32MaxJobsMap, cup64MaxJobsMap, cup128MaxJobsMap, resultPath, prefix);

		prefix = "CountUpDriverMin";
		System.out.println("### " + prefix);
		analyzeJobs(cupd32MinJobsMap, cupd64MinJobsMap, cupd128MinJobsMap, resultPath, prefix);

		prefix = "CountUpDriverMax";
		System.out.println("### " + prefix);
		analyzeJobs(cupd32MaxJobsMap, cupd64MaxJobsMap, cupd128MaxJobsMap, resultPath, prefix);

		prefix = "CountUpTextMin";
		System.out.println("### " + prefix);
		analyzeJobs(cupt32MinJobsMap, cupt64MinJobsMap, cupt128MinJobsMap, resultPath, prefix);

		prefix = "CountUpTextMax";
		System.out.println("### " + prefix);
		analyzeJobs(cupt32MaxJobsMap, cupt64MaxJobsMap, cupt128MaxJobsMap, resultPath, prefix);

		r_instance.end();
	}

	private static void analyzeJobs(SortedMap<Integer,List<LoggedJob>> jobs32Map, SortedMap<Integer,List<LoggedJob>> jobs64Map, 
			SortedMap<Integer,List<LoggedJob>> jobs128Map, String resultPath, String prefix) throws IOException {

		Histogram[] attempt32Times = new Histogram[2];
		attempt32Times[0] = new Histogram();
		attempt32Times[1] = new Histogram();		
		Histogram[] attempt64Times = new Histogram[2];
		attempt64Times[0] = new Histogram();
		attempt64Times[1] = new Histogram();		
		Histogram[] attempt128Times = new Histogram[2];
		attempt128Times[0] = new Histogram();
		attempt128Times[1] = new Histogram();

		generateJobsStatistics(jobs32Map, jobs64Map, jobs128Map, attempt32Times, attempt64Times, attempt128Times, resultPath, prefix);

		plotGraphs(resultPath, prefix, prefix);

		List<Histogram[]> histogramList = new ArrayList<Histogram[]>();
		List<String> fileNameList = new ArrayList<String>();		
		if(attempt32Times[0].getTotalCount() > 0 || attempt32Times[1].getTotalCount() > 0){
			histogramList.add(attempt32Times);
			fileNameList.add(prefix + "32CDF");
		}
		if(attempt64Times[0].getTotalCount() > 0 || attempt64Times[1].getTotalCount() > 0){
			histogramList.add(attempt64Times);
			fileNameList.add(prefix + "64CDF");
		}
		if(attempt128Times[0].getTotalCount() > 0 || attempt128Times[1].getTotalCount() > 0){
			histogramList.add(attempt128Times);
			fileNameList.add(prefix + "128CDF");
		}

		plotCDFs(r_instance, resultPath, histogramList, fileNameList, PrintMode.disabled, prefix);
	}

	private static SortedMap<Integer,File> selectTraceFiles(String tracePath) {

		SortedMap<Integer,File> traceFiles = new TreeMap<Integer,File>();

		File traceDir = new File(tracePath);
		File[] files = traceDir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith("-job-trace.json");
			}
		});

		try{
			for (File file : files) {
				StringTokenizer fileName = new StringTokenizer(file.getName(),"-");			
				Integer index = Integer.parseInt(fileName.nextToken());			
				traceFiles.put(index, file);
			}
		}catch(NumberFormatException e){
			e.printStackTrace();
		}

		return traceFiles;
	}

	private static void classifyJobs(SortedMap<Integer,File> traces) {
		ObjectMapper mapper = new ObjectMapper();
		Set<Integer> traceKeys = traces.keySet();
		for (Integer traceKey : traceKeys) {			
			try {
				File traceFile = traces.get(traceKey);
				Iterator<LoggedJob> jobs = mapper.reader(LoggedJob.class).readValues(traceFile);
				while (jobs.hasNext()) {
					classifyJob(traceKey, jobs.next());
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}	
	}

	private static void classifyJob(Integer key, LoggedJob job){
		try{
			StringTokenizer jobJame = new StringTokenizer(job.getJobName()," ");		
			String jobType = (String)jobJame.nextElement();
			jobJame.nextElement();
			jobJame.nextElement();
			jobJame.nextElement();
			String jobSize = (String) jobJame.nextElement();

			if(jobType.equals(JxtaSocketPerfDriver)){
				if(jobSize.endsWith("128-90/")){
					updateJobsMap(jxta128MaxJobsMap,key,job);
				} else if(jobSize.endsWith("64-180/")){
					updateJobsMap(jxta64MaxJobsMap,key,job);
				} else if(jobSize.endsWith("32-360/")){
					updateJobsMap(jxta32MaxJobsMap,key,job);
				} else if(jobSize.endsWith("128-30/")){
					updateJobsMap(jxta128MinJobsMap,key,job);
				} else if(jobSize.endsWith("64-60/")){
					updateJobsMap(jxta64MinJobsMap,key,job);
				} else if(jobSize.endsWith("32-120/")){
					updateJobsMap(jxta32MinJobsMap,key,job);
				}else {
					System.out.println("### JxtaSocketPerfDriver size unexpected: " + jobSize);				
					JobExecutionViewer.jobPrettyPrint(job);
				}
			}else if(jobType.equals(CountUpDriver)){
				if(jobSize.endsWith("128-90/")){
					updateJobsMap(cupd128MaxJobsMap,key,job);
				} else if(jobSize.endsWith("64-180/")){
					updateJobsMap(cupd64MaxJobsMap,key,job);
				} else if(jobSize.endsWith("32-360/")){
					updateJobsMap(cupd32MaxJobsMap,key,job);
				} else if(jobSize.endsWith("128-30/")){
					updateJobsMap(cupd128MinJobsMap,key,job);
				} else if(jobSize.endsWith("64-60/")){
					updateJobsMap(cupd64MinJobsMap,key,job);
				} else if(jobSize.endsWith("32-120/")){
					updateJobsMap(cupd32MinJobsMap,key,job);
				}else {
					System.out.println("### CountUpDriver size unexpected: " + jobSize);				
					JobExecutionViewer.jobPrettyPrint(job);
				}
			}else if(jobType.equals(CountUpText)){
				if(jobSize.endsWith("128-90/")){
					updateJobsMap(cupt128MaxJobsMap,key,job);
				} else if(jobSize.endsWith("64-180/")){
					updateJobsMap(cupt64MaxJobsMap,key,job);
				} else if(jobSize.endsWith("32-360/")){
					updateJobsMap(cupt32MaxJobsMap,key,job);
				} else if(jobSize.endsWith("128-30/")){
					updateJobsMap(cupt128MinJobsMap,key,job);
				} else if(jobSize.endsWith("64-60/")){
					updateJobsMap(cupt64MinJobsMap,key,job);
				} else if(jobSize.endsWith("32-120/")){
					updateJobsMap(cupt32MinJobsMap,key,job);
				}else {
					System.out.println("### CountUpDriver size unexpected: " + jobSize);				
					JobExecutionViewer.jobPrettyPrint(job);
				}
			}else if(jobType.equals(CountUp)){
				if(jobSize.endsWith("128-90/")){
					updateJobsMap(cup128MaxJobsMap,key,job);
				} else if(jobSize.endsWith("64-180/")){
					updateJobsMap(cup64MaxJobsMap,key,job);
				} else if(jobSize.endsWith("32-360/")){
					updateJobsMap(cup32MaxJobsMap,key,job);
				} else if(jobSize.endsWith("128-30/")){
					updateJobsMap(cup128MinJobsMap,key,job);
				} else if(jobSize.endsWith("64-60/")){
					updateJobsMap(cup64MinJobsMap,key,job);
				} else if(jobSize.endsWith("32-120/")){
					updateJobsMap(cup32MinJobsMap,key,job);
				}else {
					System.out.println("### CountUp size unexpected: " + jobSize);				
					JobExecutionViewer.jobPrettyPrint(job);
				}
			} else {
				System.out.println("### Job type unknown: " + job.getJobName());
				JobExecutionViewer.jobPrettyPrint(job);
			}
		} catch(Exception e){
			oldClassifier(key, job);
		}
	}

	private static void oldClassifier(Integer key, LoggedJob job) {
		List<LoggedTask> mapTasks = job.getMapTasks();
		if(mapTasks != null){
			double inputSize = mapTasks.get(0).getInputBytes();
			if(inputSize > 0){
				if(inputSize > 31000000 && inputSize < 34000000){
					if(mapTasks.size() == 120){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							updateJobsMap(jxta32MinJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							updateJobsMap(cupd32MinJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUp)){
							updateJobsMap(cup32MinJobsMap,key,job);	
						}							
					}else if(mapTasks.size() == 360){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							updateJobsMap(jxta32MaxJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							updateJobsMap(cupd32MaxJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUp)){
							updateJobsMap(cup32MaxJobsMap,key,job);	
						}
					}else {
						System.out.println("### Number of Tasks Error: " + job.getJobID());
						JobExecutionViewer.jobPrettyPrint(job);
					}
				}else if(inputSize > 63000000 && inputSize < 68000000){
					if(mapTasks.size() == 60){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							updateJobsMap(jxta64MinJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							updateJobsMap(cupd64MinJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUp)){
							updateJobsMap(cup64MinJobsMap,key,job);	
						}
					}else if(mapTasks.size() == 180){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							updateJobsMap(jxta64MaxJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							updateJobsMap(cupd64MaxJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUp)){
							updateJobsMap(cup64MaxJobsMap,key,job);	
						}
					}else{
						System.out.println("### Number of Tasks Error: " + job.getJobID());
						JobExecutionViewer.jobPrettyPrint(job);
					}
				}else if(inputSize > 127000000 && inputSize < 135000000){
					if(mapTasks.size() == 30){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							updateJobsMap(jxta128MinJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							updateJobsMap(cupd128MinJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUp)){
							updateJobsMap(cup128MinJobsMap,key,job);	
						}
					}else if(mapTasks.size() == 90){
						if(job.getJobName().equals(JxtaSocketPerfDriver)){
							updateJobsMap(jxta128MaxJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUpDriver)){
							updateJobsMap(cupd128MaxJobsMap,key,job);	
						}else if(job.getJobName().equals(CountUp)){
							updateJobsMap(cup128MaxJobsMap,key,job);	
						}
					}else{
						System.out.println("### Number of Tasks Error: " + mapTasks.size());
						JobExecutionViewer.jobPrettyPrint(job);
					}
				}else {
					System.out.println("### Map Input Size Error: " + inputSize);
					JobExecutionViewer.jobPrettyPrint(job);
				}
			}else{
				System.out.println("### Map Input Error : " + inputSize);
				JobExecutionViewer.jobPrettyPrint(job);
			}
		}
	}

	private static void generateJobsStatistics( 
			SortedMap<Integer,List<LoggedJob>> jobs32Map, SortedMap<Integer,List<LoggedJob>> jobs64Map, SortedMap<Integer,List<LoggedJob>> jobs128Map, 
			Histogram[] attempts32Times, Histogram[] attempts64Times, Histogram[] attempts128Times,
			String resultPath, String prefix) {

		try {
			File resultDir = new File(resultPath + "/" + prefix);
			resultDir.mkdir();
			BufferedWriter writer = new BufferedWriter(new FileWriter(resultDir + "/" + prefix + ".dat"));

			SortedSet<Integer> keys = new TreeSet<Integer>();
			keys.addAll(jobs32Map.keySet());
			keys.addAll(jobs64Map.keySet());
			keys.addAll(jobs128Map.keySet());

			for (Integer key : keys) {// for each cluster size

				//				if (key.intValue() == 3 || key.intValue() == 5 || key.intValue() == 7 || key.intValue() == 9 || key.intValue() == 15 || key.intValue() == 20 || key.intValue() == 28){
				System.out.print(key);
				writer.write(String.valueOf(key));

				generateStatistics(jobs32Map.get(key),PrintMode.fullInline, writer, attempts32Times);
				generateStatistics(jobs64Map.get(key),PrintMode.fullInline, writer, attempts64Times);
				generateStatistics(jobs128Map.get(key),PrintMode.fullInline, writer, attempts128Times);

				//				plotIndividualCDF(key, resultPath, prefix, jobs32Map, jobs64Map, jobs128Map);

				System.out.println();
				writer.newLine();
				//				}
			}

			writer.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	private static void plotIndividualCDF(Integer key, String resultPath, String prefix, 
			SortedMap<Integer,List<LoggedJob>> jobs32Map, SortedMap<Integer,List<LoggedJob>> jobs64Map, 
			SortedMap<Integer,List<LoggedJob>> jobs128Map) throws IOException{

		Histogram[] tmp32Times = new Histogram[2];
		tmp32Times[0] = new Histogram();
		tmp32Times[1] = new Histogram();		
		Histogram[] tmp64Times = new Histogram[2];
		tmp64Times[0] = new Histogram();
		tmp64Times[1] = new Histogram();		
		Histogram[] tmp128Times = new Histogram[2];
		tmp128Times[0] = new Histogram();
		tmp128Times[1] = new Histogram();

		generateStatistics(jobs32Map.get(key),PrintMode.disabled, null, tmp32Times);
		generateStatistics(jobs64Map.get(key),PrintMode.disabled, null, tmp64Times);
		generateStatistics(jobs128Map.get(key),PrintMode.disabled, null, tmp128Times);

		List<Histogram[]> histogramList = new ArrayList<Histogram[]>();
		List<String> fileNameList = new ArrayList<String>();		
		if(tmp32Times[0].getTotalCount() > 0 || tmp32Times[1].getTotalCount() > 0){
			histogramList.add(tmp32Times);
			fileNameList.add(prefix + "32CDF" + key);
		}
		if(tmp64Times[0].getTotalCount() > 0 || tmp64Times[1].getTotalCount() > 0){
			histogramList.add(tmp64Times);
			fileNameList.add(prefix + "64CDF" + key);
		}
		if(tmp128Times[0].getTotalCount() > 0 || tmp128Times[1].getTotalCount() > 0){
			histogramList.add(tmp128Times);
			fileNameList.add(prefix + "128CDF" + key);
		}

		plotCDFs(r_instance, resultPath, histogramList, fileNameList, PrintMode.disabled, prefix);
	}

	private static void updateJobsMap(SortedMap<Integer,List<LoggedJob>> jobsMap, Integer key, LoggedJob job) {
		List<LoggedJob> jobs = jobsMap.get(key);
		if(jobs != null){
			jobsMap.get(key).add(job);
		}else{
			jobs = new ArrayList<LoggedJob>();
			jobs.add(job);
			jobsMap.put(key,jobs);
		}
	}

	private static void generateStatistics(List<LoggedJob> jobs, PrintMode printMode, BufferedWriter writer, Histogram[] histograms){
		DescriptiveStatistics jobRuntimeStats = new DescriptiveStatistics();
		DescriptiveStatistics jobSetupTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics jobCleanupTimeStats = new DescriptiveStatistics();

		DescriptiveStatistics mapTasksStats = new DescriptiveStatistics();
		DescriptiveStatistics mapTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics mapShuffleTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics mapNotLocalAttemptStats = new DescriptiveStatistics();		
		DescriptiveStatistics mapOutInStats = new DescriptiveStatistics();
		DescriptiveStatistics mapSuccessAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics mapFailedAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics mapKilledAttemptStats = new DescriptiveStatistics();		
		DescriptiveStatistics mapAttemptStats = new DescriptiveStatistics();		

		DescriptiveStatistics reduceTasksStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceTaskTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceShuffleTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceSortTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceNotLocalAttemptStats = new DescriptiveStatistics();		
		DescriptiveStatistics reduceSuccessAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceFailedAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceKilledAttemptStats = new DescriptiveStatistics();		
		DescriptiveStatistics reduceAttemptStats = new DescriptiveStatistics();

		if(jobs != null){
			// Get Job's statistics
			for (LoggedJob job : jobs) {

				currJob = job;
				jobTime = (job.getFinishTime() - job.getLaunchTime())/1000; 
				if(jobTime > 0){
					jobRuntimeStats.addValue(jobTime);
				
					// Setup and Cleanup
					List<LoggedTask> otherTasks = job.getOtherTasks();
					getJobStatistics(otherTasks,jobSetupTimeStats,jobCleanupTimeStats);
					
					// Map
					List<LoggedTask> mapTasks = job.getMapTasks();			
					double mapMaxTime = getMapStatistics(mapTasks, mapTasksStats, mapTimeStats, mapNotLocalAttemptStats, mapOutInStats, mapSuccessAttemptStats,
							mapKilledAttemptStats, mapFailedAttemptStats, mapAttemptStats, histograms[0]);

					// Reduce
					List<LoggedTask> reduceTasks = job.getReduceTasks();			
					getReduceStatistics(mapMaxTime, reduceTasks, reduceTasksStats, mapShuffleTimeStats, reduceShuffleTimeStats, reduceSortTimeStats,
							reduceTimeStats, reduceTaskTimeStats, reduceNotLocalAttemptStats, reduceSuccessAttemptStats, reduceKilledAttemptStats,
							reduceFailedAttemptStats, reduceAttemptStats, histograms[1]);
				}

			}

			// Print Statistics
			if(printMode == PrintMode.fullInline){
				printInlineFullStats(
						jobRuntimeStats, jobSetupTimeStats, jobCleanupTimeStats, mapTasksStats, mapTimeStats, mapShuffleTimeStats, mapNotLocalAttemptStats, mapOutInStats, mapSuccessAttemptStats, 
						mapKilledAttemptStats, mapFailedAttemptStats, mapAttemptStats, reduceTasksStats, reduceTaskTimeStats, reduceShuffleTimeStats, 
						reduceSortTimeStats, reduceTimeStats, reduceNotLocalAttemptStats, reduceSuccessAttemptStats, reduceKilledAttemptStats, 
						reduceFailedAttemptStats, reduceAttemptStats,  writer);
			}
		}
	}

	private static void plotGraphs(String resultPath, String fileName, String prefix) {
		File resultDir = new File(resultPath + "/" + prefix +  "/");
		resultDir.mkdir();

		plotPhasesEvaluation(resultDir, fileName, prefix + "Phases");
		plotPhasesPercentEvaluation(resultDir, fileName, prefix + "PhasesPercent");

		if(fileName.startsWith("CountUpDriver")){			
			String fileName2 = fileName.replaceFirst("CountUpDriver", "CountUp");			
			plotJobTimeComp(resultDir, "../" + fileName + "/" + fileName, "../" + fileName2 + "/" + fileName2,
					"CountUpDriver", "P3", prefix + "JobTimeComp");
		}

		if(fileName.equals("JxtaMax")){
			plotJobSpeedupBarLines(resultDir, fileName, prefix + "SpeedupBarLines", 1745, 1755, 1756);			
		}else if(fileName.equals("CountUpDriverMax") || fileName.equals("CountUpMax")){			
			plotJobSpeedupBarLines(resultDir, fileName, prefix + "SpeedupBarLines", 811, 571, 745);
		}if(fileName.equals("JxtaMin")){			
			plotJobSpeedupBarLines(resultDir, fileName, prefix + "SpeedupBarLines", 584, 587, 606);			
		}else if(fileName.equals("CountUpDriverMin") || fileName.equals("CountUpMin")){			
			plotJobSpeedupBarLines(resultDir, fileName, prefix + "SpeedupBarLines", 86, 91, 94);
		}
	}	

	private static void plotCDFs(Rengine rengine, String resultPath, List<Histogram[]> times, List<String> fileNames, PrintMode printMode, String prefix) throws IOException {		
		int index = 0;
		for (String fileName : fileNames) {			
			Histogram[] histogram = times.get(index);
			index++;

			if(printMode.equals(PrintMode.prettyPrint)){
				System.out.println("### " + fileName + "Map");
			}
			plotCDF(rengine, histogram[0], resultPath + "/" + prefix + "/", fileName + "Map", printMode);
			if(printMode.equals(PrintMode.prettyPrint)){
				System.out.println();
				System.out.println("### " + fileName + "Reduce");
			}
			plotCDF(rengine, histogram[1], resultPath + "/" + prefix + "/", fileName + "Reduce", printMode);
			if(printMode.equals(PrintMode.prettyPrint)){
				System.out.println();
			}
		}
	}

	// Public Methods ------------------------------------------------------------------------------------------------------------------------------------

	public static void getJobStatistics( 
			List<LoggedTask> tasks,
			DescriptiveStatistics jobSetupTimeStats,
			DescriptiveStatistics jobCleanupTimeStats) {

		if(tasks != null){

			for (LoggedTask task : tasks) {

				if(task.getTaskType().equals(Pre21JobHistoryConstants.Values.SETUP)){
					double setupTime = task.getFinishTime() - task.getStartTime();
					if(setupTime > 0){
						jobSetupTimeStats.addValue(setupTime);
					}
				}else if(task.getTaskType().equals(Pre21JobHistoryConstants.Values.CLEANUP)){
					double cleanupTime = task.getFinishTime() - task.getStartTime();
					if(cleanupTime > 0){
						jobCleanupTimeStats.addValue(cleanupTime);
					}
				}
				
			}
			
		}
	}

	public static double getMapStatistics(List<LoggedTask> tasks,
			DescriptiveStatistics numTasksStats,
			DescriptiveStatistics mapTaskTimeStats,
			DescriptiveStatistics notLocalAttemptStats,
			DescriptiveStatistics outInStats,
			DescriptiveStatistics successAttemptStats,
			DescriptiveStatistics killedAttemptStats,
			DescriptiveStatistics failedAttemptStats,			
			DescriptiveStatistics totalAttemptStats, 
			Histogram attemptTimes) {

		double minTime = Double.MAX_VALUE;
		double maxTime = Double.MIN_VALUE;

		if(tasks != null){
			numTasksStats.addValue(tasks.size());
			for (LoggedTask mapTask : tasks) {
				double mapNotLocalAttempt = 0;
				double mapSuccessAttempt = 0;
				double mapFailedAttempt = 0;
				double mapKilledAttempt = 0;				
				double outInRate = (mapTask.getOutputBytes() / mapTask.getInputBytes()) * 100;				
				outInStats.addValue(outInRate);

				if(mapTask.getFinishTime() > maxTime){
					maxTime = mapTask.getFinishTime();
				}									
				if(mapTask.getStartTime() < minTime){
					minTime = mapTask.getStartTime();
				}

				List<LoggedLocation> dataLocations = mapTask.getPreferredLocations();
				List<LoggedTaskAttempt> attemptList = mapTask.getAttempts();
				// Attempts
				if(attemptList != null){
					totalAttemptStats.addValue(attemptList.size());
					for (LoggedTaskAttempt mapAttempt : attemptList) {

						if(mapAttempt.getResult() == Pre21JobHistoryConstants.Values.SUCCESS){
							mapSuccessAttempt++;														
						} else if(mapAttempt.getResult() == Pre21JobHistoryConstants.Values.KILLED){
							mapKilledAttempt++;
							continue;
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
		mapTaskTimeStats.addValue((maxTime - minTime)/1000);		

		return maxTime;
	}	

	public static void getReduceStatistics(
			double maxMapTaskTime, 
			List<LoggedTask> tasks,
			DescriptiveStatistics numTasksStats,
			DescriptiveStatistics mapShuffleTimeStats,
			DescriptiveStatistics shuffleTimeStats,
			DescriptiveStatistics sortTimeStats,
			DescriptiveStatistics reduceTimeStats,
			DescriptiveStatistics reduceTaskTimeStats,
			DescriptiveStatistics notLocalAttemptStats,
			DescriptiveStatistics successAttemptStats,
			DescriptiveStatistics killedAttemptStats,
			DescriptiveStatistics failedAttemptStats,			
			DescriptiveStatistics totalAttemptStats, 
			Histogram attemptTimes) {

		double minReduceTaskTime = Double.MAX_VALUE;
		double maxReduceTaskTime = Double.MIN_VALUE;		

		double minReduceTime = Double.MAX_VALUE;
		double maxShuffleTime = Double.MIN_VALUE;
		double maxSortTime = Double.MIN_VALUE;
		double maxReduceTime = Double.MIN_VALUE;

		if(tasks != null){
			numTasksStats.addValue(tasks.size());
			for (LoggedTask reduceTask : tasks) {
				double notLocalAttempts = 0;
				double successAttempts = 0;
				double failedAttempts = 0;
				double killedAttempts = 0;

				if(reduceTask.getStartTime() < minReduceTaskTime){
					minReduceTaskTime = reduceTask.getStartTime();
				}
				if(reduceTask.getFinishTime() > maxReduceTaskTime){
					maxReduceTaskTime = reduceTask.getFinishTime();
				}

				List<LoggedLocation> dataLocations = reduceTask.getPreferredLocations();
				List<LoggedTaskAttempt> attemptList = reduceTask.getAttempts();
				// Attempts
				if(attemptList != null){
					totalAttemptStats.addValue(attemptList.size());
					for (LoggedTaskAttempt reduceAttempt : attemptList) {

						if(reduceAttempt.getResult() == Pre21JobHistoryConstants.Values.SUCCESS){
							successAttempts++;
							if((reduceAttempt.getFinishTime() - reduceAttempt.getStartTime()) > 0){
								if(reduceAttempt.getStartTime() < minReduceTime){
									minReduceTime = reduceAttempt.getStartTime();
								}
								if(reduceAttempt.getShuffleFinished() > maxShuffleTime){
									maxShuffleTime = reduceAttempt.getShuffleFinished();
								}									
								if(reduceAttempt.getSortFinished() > maxSortTime){
									maxSortTime = reduceAttempt.getSortFinished();
								}		
								if(reduceAttempt.getFinishTime() > maxReduceTime){
									maxReduceTime = reduceAttempt.getFinishTime();
								}								
							}

						} else if(reduceAttempt.getResult() == Pre21JobHistoryConstants.Values.KILLED){
							killedAttempts++;
							continue;
						} else if(reduceAttempt.getResult() == Pre21JobHistoryConstants.Values.FAILED){
							failedAttempts++;
						}

						if(reduceAttempt.getLocation() != null){
							boolean isLocal = false;
							for (LoggedLocation location : dataLocations) {
								isLocal = isLocal || location.getLayers().equals(reduceAttempt.getLocation().getLayers());								
							} 
							if(!isLocal){
								notLocalAttempts++;
							}
						}

						if((reduceAttempt.getFinishTime() - reduceAttempt.getStartTime()) > 0){
							attemptTimes.enter(reduceAttempt.getFinishTime() - reduceAttempt.getStartTime());
						}
					}						
				}					
				notLocalAttemptStats.addValue(notLocalAttempts);
				successAttemptStats.addValue(successAttempts);
				killedAttemptStats.addValue(killedAttempts);
				failedAttemptStats.addValue(failedAttempts);					
			}
		}

		double reduceAttemptTime = maxReduceTime - minReduceTime;
		double shufflePerc = (maxShuffleTime - minReduceTime)/reduceAttemptTime;
		double sortPerc = (maxSortTime - maxShuffleTime)/reduceAttemptTime;
		double reducePerc = (maxReduceTime - maxSortTime)/reduceAttemptTime;

		double reduceTaskTime = (maxReduceTaskTime - minReduceTaskTime)/1000;
		double mapShuffleTime = (maxMapTaskTime - minReduceTaskTime)/1000;		
		double shuffleTime = (shufflePerc * reduceTaskTime) - mapShuffleTime;
		double sortTime = sortPerc * reduceTaskTime;
		double reduceTime = reducePerc * reduceTaskTime;

		mapShuffleTimeStats.addValue(mapShuffleTime);
		shuffleTimeStats.addValue(shuffleTime);
		sortTimeStats.addValue(sortTime);
		reduceTimeStats.addValue(reduceTime);
		reduceTaskTimeStats.addValue(reduceTaskTime);
	}

	public static void printInlineFullStats(
			DescriptiveStatistics jobRuntimeStats,
			DescriptiveStatistics jobSetupTimeStats,
			DescriptiveStatistics jobCleanupTimeStats,
			DescriptiveStatistics mapTasksStats,
			DescriptiveStatistics mapTaskTimeStats,	
			DescriptiveStatistics mapShuffleTimeStats,
			DescriptiveStatistics mapNotLocalAttemptStats,
			DescriptiveStatistics mapOutInStats,
			DescriptiveStatistics mapSuccessAttemptStats,
			DescriptiveStatistics mapKilledAttemptStats,
			DescriptiveStatistics mapFailedAttemptStats,					
			DescriptiveStatistics mapAttemptStats,		
			DescriptiveStatistics reduceNumStats,
			DescriptiveStatistics reduceTaskTimeStats,
			DescriptiveStatistics reduceShuffleTimeStats,
			DescriptiveStatistics reduceSortTimeStats,
			DescriptiveStatistics reduceTimeStats,
			DescriptiveStatistics reduceNotLocalAttemptStats,		
			DescriptiveStatistics reduceSuccessAttemptStats,
			DescriptiveStatistics reduceKilledAttemptStats,
			DescriptiveStatistics reduceFailedAttemptStats,					
			DescriptiveStatistics reduceAttemptStats, 
			BufferedWriter writer) {

		StringBuilder output = new StringBuilder();
		DecimalFormat df2 = new DecimalFormat( "#,###,###,##0.00" );

		jobRuntimeStats = removeOutliers(jobRuntimeStats);
		jobRuntimeStats = removeOutliers(jobRuntimeStats);

		mapTaskTimeStats = removeOutliers(mapTaskTimeStats);
		mapShuffleTimeStats = removeOutliers(mapShuffleTimeStats);
		reduceTaskTimeStats = removeOutliers(reduceTaskTimeStats);
		reduceShuffleTimeStats = removeOutliers(reduceShuffleTimeStats);
		reduceSortTimeStats = removeOutliers(reduceSortTimeStats);
		reduceTimeStats = removeOutliers(reduceTimeStats);

		output.append("\t" + df2.format(jobRuntimeStats.getMean()) + "\t" + df2.format(jobRuntimeStats.getStandardDeviation()));
		output.append("\t" + df2.format(jobSetupTimeStats.getMean()) + "\t" + df2.format(jobSetupTimeStats.getStandardDeviation()));
		output.append("\t" + df2.format(jobCleanupTimeStats.getMean()) + "\t" + df2.format(jobCleanupTimeStats.getStandardDeviation()));

		output.append("\t" + df2.format(mapTasksStats.getMean()) + "\t" + df2.format((mapTasksStats.getStandardDeviation()/mapTasksStats.getMean()) * 100));
		output.append("\t" + df2.format(mapTaskTimeStats.getMean()) + "\t" + df2.format(mapTaskTimeStats.getStandardDeviation()));
		output.append("\t" + df2.format((mapNotLocalAttemptStats.getMean()/mapAttemptStats.getMean())*100) + "\t" + df2.format((mapNotLocalAttemptStats.getStandardDeviation()/mapNotLocalAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format(mapOutInStats.getMean()) + "\t" + df2.format((mapOutInStats.getStandardDeviation()/mapOutInStats.getMean()) * 100));
		output.append("\t" + df2.format((mapSuccessAttemptStats.getMean()/mapAttemptStats.getMean())*100) + "\t" + df2.format((mapSuccessAttemptStats.getStandardDeviation()/mapSuccessAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format((mapKilledAttemptStats.getMean()/mapAttemptStats.getMean())*100) + "\t" + df2.format((mapKilledAttemptStats.getStandardDeviation()/mapKilledAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format((mapFailedAttemptStats.getMean()/mapAttemptStats.getMean())*100) + "\t" + df2.format((mapFailedAttemptStats.getStandardDeviation()/mapFailedAttemptStats.getMean()) * 100));
		output.append("\t" + df2.format(mapAttemptStats.getMean()) + "\t" + df2.format((mapAttemptStats.getStandardDeviation()/mapAttemptStats.getMean()) * 100));

		output.append("\t" + df2.format(reduceNumStats.getMean()) + "\t" + df2.format((reduceNumStats.getStandardDeviation()/reduceNumStats.getMean()) * 100));
		output.append("\t" + df2.format(reduceTaskTimeStats.getMean()) + "\t" + df2.format(reduceTaskTimeStats.getStandardDeviation()));		
		output.append("\t" + df2.format(mapShuffleTimeStats.getMean()) + "\t" + df2.format(mapShuffleTimeStats.getStandardDeviation()));
		output.append("\t" + df2.format(reduceShuffleTimeStats.getMean()) + "\t" + df2.format(reduceShuffleTimeStats.getStandardDeviation()));
		output.append("\t" + df2.format(reduceSortTimeStats.getMean()) + "\t" + df2.format(reduceSortTimeStats.getStandardDeviation()));
		output.append("\t" + df2.format(reduceTimeStats.getMean()) + "\t" + df2.format(reduceTimeStats.getStandardDeviation()));
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

	private static DescriptiveStatistics removeOutliers(DescriptiveStatistics statistic) {		
		double[] values = statistic.getValues(); 
		double max = statistic.getMax();
		double min = statistic.getMin();

		statistic = new DescriptiveStatistics();
		for (double value : values) {
			if(value != max && value != min){
				statistic.addValue(value);
			}
		}
		return statistic;
	}

	public static void plotJobSpeedupBarLines(File destPath, String sourceFileName, String destFileName, int mono32, int mono64, int mono128){
		String strCommands = "reset;" +
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +
				"set yrange [0:310];" + 
				"set ytics nomirror;" + 
				"set ylabel \"Runtime(s)\";" + 
				"set y2range [0:12];" +
				"set y2tics;" + 
				"set y2label \"Speedup\";" + 
				"set xlabel \"Nodes\";" + 
				"set key top right;" +				 
				"f(x) = " + mono32 + " / x;" + 
				"g(x) = " + mono64 + " / x;" + 
				"h(x) = " + mono128 + " / x;" + 
				"set style data histogram;" +
				"set style histogram errorbars gap 1.5 lw 3;" +
				"set style fill pattern border;" + 	
				"plot " +	
				"'" + sourceFileName + ".dat' u 2:3:xtic(1) lt 1 axis x1y1 t \"JobTime(32MB)\"," +
				"'" + sourceFileName + ".dat' u 46:47:xtic(1) lt 1 axis x1y1 t \"JobTime(64MB)\"," +
				"'" + sourceFileName + ".dat' u 90:91:xtic(1) lt 1 axis x1y1 t \"JobTime(128MB)\"," +
				"'" + sourceFileName + ".dat' u ( f($2) ):xtic(1) w lp lt 0  pt 5 axis x1y2 t \"Speedup(32MB)\"," +
				"'" + sourceFileName + ".dat' u ( g($46) ):xtic(1) w lp lt 5 pt 7 axis x1y2 t \"Speedup(64MB)\"," +
				"'" + sourceFileName + ".dat' u ( h($90) ):xtic(1) w lp lt 7 pt 9 axis x1y2 t \"Speedup(128MB)\";";
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

	public static void plotPhasesEvaluation(File destPath, String sourceFileName, String destFileName){
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +  
				"set boxwidth 0.8;" + 
				"set style fill pattern 1;" + 
				"set grid nopolar;" + 
				"set grid noxtics nomxtics ytics nomytics noztics nomztics nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics;" + 
				"set grid layerdefault linetype 0 linewidth 1.000, linetype 0 linewidth 1.000;" + 
				"set key top right;" + 
				"set style histogram rowstacked title offset character 0, 0, 0;" + 
				"set datafile missing '-';" + 
				"set style data histograms;" + 
				"set xtics rotate by -90 ;" + 
				"set xlabel \" \" ;" + 
				"set xlabel offset character 0, -1, 0 font \"\" textcolor lt -1 norotate;" + 
				"set ylabel \"Time(s)\" ;" + 
				"set yrange [0:310] noreverse nowriteback;" + 
				"plot " +
				"newhistogram \"32MB\" fs pattern 1, '" + sourceFileName + ".dat' " +
				"u ($10 - $28):xtic(1) t \"Map\" lt 1, '' u 28 t \"Map and Shuffle\" lt 1, '' u 30 t \"Shuffle\" lt 1, '' u 32 t \"Sort\" lt 1, '' u 34 t \"Reduce\" lt 1,'' u 4 t \"Setup\" lt 1, '' u 6 t \"Cleanup\" lt 1," +
				"newhistogram \"64MB\" fs pattern 1, '' " +
				"u ($54 - $72):xtic(1) notitle lt 1, '' u 72 notitle lt 1, '' u 74 notitle lt 1, '' u 76 notitle lt 1, '' u 78 notitle lt 1,'' u 48 notitle lt 1, '' u 50 notitle lt 1," +
				"newhistogram \"128MB\" fs pattern 1, '' " +
				"u ($98 - $116):xtic(1) notitle lt 1, '' u 116 notitle lt 1, '' u 118 notitle lt 1, '' u 120 notitle lt 1, '' u 122 notitle lt 1,'' u 92 notitle lt 1, '' u 94 notitle lt 1;";
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

	public static void plotPhasesPercentEvaluation(File destPath, String sourceFileName, String destFileName){
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +  
				"set boxwidth 0.8;" + 
				"set style fill pattern 1;" + 
				"set grid noxtics nomxtics noztics nomztics nox2tics nomx2tics noy2tics nomy2tics nocbtics nomcbtics;" + 
				"set grid layerdefault linetype 0 linewidth 1.000, linetype 0 linewidth 1.000;" + 
				"set key tmargin horizontal;" + 
				"set style histogram rowstacked title offset character 0, 0, 0;" + 
				"set datafile missing '-';" + 
				"set style data histograms;" + 
				"set xtics rotate by -90;" + 
				"set xlabel \" \";" + 
				"set xlabel offset character 0, -1, 0 font \"\" textcolor lt -1 norotate;" + 
				"set ylabel \"% of Job Time\";" + 
				"set yrange [0:100] noreverse nowriteback;" +
				"perc(x,y) = (x/y)*100;" +
				"percRemain(a,b,c,d,e,f,g,h) = ((a - b - c - d - e - f - g - h)/a)*100;" +
				"plot " +				 
				"newhistogram \"32MB\" fs pattern 1, '" + sourceFileName + ".dat' " +
				"u ( perc($10 - $28, $2) ):xtic(1) t \"Map\" lt 1, '' u ( perc($28, $2) ) t \"Map and Shuffle\" lt 1, '' u ( perc($30, $2) ) t \"Shuffle\" lt 1, " +
				"'' u ( perc($32, $2) ) t \"Sort\" lt 1, '' u ( perc($34, $2) ) t \"Reduce\" lt 1, '' u ( perc($4, $2) ) t \"Setup\" lt 1, " +
				"'' u ( perc($6, $2) ) t \"Cleanup\" lt 1, '' u ( percRemain($2, $10 - $28, $28, $30, $32, $34, $4, $6) ) t \"Others\" lt 1, " +
				"newhistogram \"64MB\" fs pattern 1, " +
				"'' u ( perc($54 - $72, $46) ):xtic(1) notitle lt 1, '' u ( perc($72, $46) ) notitle lt 1, '' u ( perc($74, $46) ) notitle lt 1, " +
				"'' u ( perc($76, $46) ) notitle lt 1, '' u ( perc($78, $46) ) notitle lt 1, '' u ( perc($48, $46) ) notitle lt 1, " +
				"'' u ( perc($50, $46) ) notitle lt 1, '' u ( percRemain($46, $54 - $72, $72, $74, $76, $78, $48, $50) ) notitle lt 1, " +
				"newhistogram \"128MB\" fs pattern 1, " +
				"'' u ( perc($98 - $116, $90) ):xtic(1) notitle lt 1, '' u ( perc($116, $90) ) notitle lt 1, '' u ( perc($118, $90) ) notitle lt 1, " +
				"'' u ( perc($120, $90) ) notitle lt 1, '' u ( perc($122, $90) ) notitle lt 1, '' u ( perc($92, $90) ) notitle lt 1, " +
				"'' u ( perc($94, $90) ) notitle lt 1, '' u ( percRemain($90, $98 - $116, $116, $118, $120, $122, $92, $94) ) notitle lt 1;";		
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

	public static void plotJobTimeComp(File destPath, String sourceFileName1, String sourceFileName2, String label1, String label2, String destFileName){
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +  
				"set yrange [0:550];" +
				"set style data histogram;" +				
				"set style histogram errorbars gap 2 lw 3;" +
				"set style fill pattern border;" + 	
				"set boxwidth 0.9;" +
				"plot " +
				"'" + sourceFileName1 + ".dat' u 2:3:xtic(1) lt 1 t \"" + label1 + "(32MB)\", " +
				"'" + sourceFileName2 + ".dat' u 2:3 lt 1 t \"" + label2 + "(32MB)\", " +
				"'" + sourceFileName1 + ".dat' u 46:47:xtic(1) lt 1 t \"" + label1 + "(64MB)\"," +
				"'" + sourceFileName2 + ".dat' u 46:47 lt 1 t \"" + label2 + "(64MB)\", " +
				"'" + sourceFileName1 + ".dat' u 90:91:xtic(1) lt 1 t \"" + label1 + "(128MB)\"," +
				"'" + sourceFileName2 + ".dat' u 90:91 lt 1 t \"" + label2 + "(128MB)\""; 
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
	
	
	
	
	
	public static void plotJobSpeedupLines(File destPath, String sourceFileName, String destFileName, int mono32, int mono64, int mono128){
		String strCommands = "reset;" +
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +
				"set yrange [0:850];" + 
				"set ytics nomirror;" + 
				"set ylabel \"Runtime(s)\";" + 
				"set y2range [0:2.5];" +
				"set y2tics;" + 
				"set y2label \"Speedup\";" + 
				"set xrange [0:30];" + 
				"set xlabel \"Nodes\";" + 
				"set key top right;" + 
				"f(x) = " + mono32 + " / x;" + 
				"g(x) = " + mono64 + " / x;" + 
				"h(x) = " + mono128 + " / x;" + 
				"plot '" + sourceFileName + ".dat' using 1:2:3 axis x1y1 title \"JobRuntime(32MB)\" w yerrorlines, " +
				"'" + sourceFileName + ".dat' using 1:46:47 axis x1y1 title \"JobRuntime(64MB)\" w yerrorlines, " +
				"'" + sourceFileName + ".dat' using 1:90:91 axis x1y1 title \"JobRuntime(128MB)\" w yerrorlines," +
				"'" + sourceFileName + ".dat' using 1:( f($2) ) w lp lt 5 pt 5 axis x1y2 title \"Speedup(32MB)\", " +
				"'" + sourceFileName + ".dat' using 1:( g($46) ) w lp lt 7 pt 7 axis x1y2 title \"Speedup(64MB)\", " +
				"'" + sourceFileName + ".dat' using 1:( h($90) ) w lp lt 12 pt 9 axis x1y2 title \"Speedup(128MB)\";";	
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

	public static void plotJobTimeLines(File destPath, String sourceFileName, String monoFileName, String destFileName){
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +
				"set yrange [0:1800];" +
				"set xrange [1:30];" +
				"set ylabel \"Runtime(s)\";" +
				"set xlabel \"Nodes\";" +
				"set key center right;" +
				"plot '" + sourceFileName + ".dat' using 1:2:3 axis x1y1 title \"JobRuntime(32MB)\" w yerrorlines," +
				"'" + sourceFileName + ".dat' using 1:40:41 axis x1y1 title \"JobRuntime(64MB)\" w yerrorlines," +
				"'" + sourceFileName + ".dat' using 1:78:79 axis x1y1 title \"JobRuntime(128MB)\" w yerrorlines," +
				"'" + monoFileName + ".dat' using (0):3:(1):(0) axes x2y1 with vector nohead filled lt 3 title \"MonoRuntime\";";		
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

	public static void plotJobReduceCompletionTime(File destPath, String sourceFileName, String destFileName){
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +
				"set yrange [0:600];" +
				"set xrange [0:32];" +
				"set ylabel \"Runtime(s)\";" +
				"set xlabel \"Nodes\";" +
				"plot '" + sourceFileName + ".dat' using 1:2:3 		axis x1y1 title \"Job Meantime(32MB)\" w yerrorlines," +
				"'" + sourceFileName + ".dat' using 1:40:41 	axis x1y1 title \"Job Meantime(64MB)\" w yerrorlines," +
				"'" + sourceFileName + ".dat' using 1:78:79 	axis x1y1 title \"Job Meantime(128MB)\" w yerrorlines," +
				"'" + sourceFileName + ".dat' using 1:28:29 	axis x1y1 title \"Reduce Meantime(32MB)\" w yerrorlines," + 
				"'" + sourceFileName + ".dat' using 1:66:67 	axis x1y1 title \"Reduce Meantime(64MB)\" w yerrorlines," + 
				"'" + sourceFileName + ".dat' using 1:104:105 	axis x1y1 title \"Reduce Meantime(128MB)\" w yerrorlines;";
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

	public static void plotJobLocality(File destPath, String sourceFileName, String destFileName){
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +
				"set yrange [0:600];" + 
				"set xrange [0:32];" + 
				"set y2range [0:50];" + 
				"set ytics nomirror;" + 
				"set y2tics;" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"set y2label \"Non-locality\";" +
				"plot '" + sourceFileName + ".dat' using 1:2:3 		axis x1y1 title \"Job Runtime(32MB)\" w yerrorlines," +
				" '" + sourceFileName + ".dat' using 1:40:41 	axis x1y1 title \"Job Runtime(64MB)\" w yerrorlines," +
				" '" + sourceFileName + ".dat' using 1:78:79 	axis x1y1 title \"Job Runtime(128MB)\" w yerrorlines," +
				" '" + sourceFileName + ".dat' using 1:4:5 		axis x1y2 title \"Map Non-locality(32MB)\" w linesp," +
				" '" + sourceFileName + ".dat' using 1:46:47 	axis x1y2 title \"Map Non-locality(64MB)\" w linesp," +
				" '" + sourceFileName + ".dat' using 1:80:81 	axis x1y2 title \"Map Non-locality(128MB)\" w linesp," +
				" '" + sourceFileName + ".dat' using 1:8:9 		axis x1y2 title \"Reduce Non-locality(32MB)\" w linesp," +
				" '" + sourceFileName + ".dat' using 1:46:47 	axis x1y2 title \"Reduce Non-locality(64MB)\" w linesp," + 
				" '" + sourceFileName + ".dat' using 1:84:85 	axis x1y2 title \"Reduce Non-locality(128MB)\" w linesp;";
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

	public static void plotMapLocality(File destPath, String sourceFileName, String destFileName){
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +
				"set yrange [0:50];" +
				"set xrange [0:32];" +
				"set y2range [0:50];" +
				"set ytics nomirror;" + 
				"set y2tics;" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"set y2label \"Non-locality\";" + 
				"set key top left reverse Left;" + 
				"plot '" + sourceFileName + ".dat' using 1:14:15 axis x1y1 title \"Map Meantime(32MB)\" w yerrorlines," + 
				" '" + sourceFileName + ".dat' using 1:52:53 axis x1y1 title \"Map Meantime(64MB)\" w yerrorlines," +
				" '" + sourceFileName + ".dat' using 1:90:91 axis x1y1 title \"Map Meantime(128MB)\" w yerrorlines," +
				" '" + sourceFileName + ".dat' using 1:16:17 axis x1y2 title \"Map Non-locality(32MB)\" w yerrorlines," + 
				" '" + sourceFileName + ".dat' using 1:54:55 axis x1y2 title \"Map Non-locality(64MB)\" w yerrorlines," + 
				" '" + sourceFileName + ".dat' using 1:92:93 axis x1y2 title \"Map Non-locality(64MB)\" w yerrorlines;";
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

	public static void plotMapCompletionTime(File destPath, String sourceFileName, String destFileName){
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +
				"set yrange [0:80];" + 
				"set xrange [0:32];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" +
				"plot '" + sourceFileName + ".dat' using 1:14:15 axis x1y1 title \"Map Meantime(32MB)\" w yerrorlines," + 
				" '" + sourceFileName + ".dat' using 1:52:53 axis x1y1 title \"Map Meantime(64MB)\" w yerrorlines," +
				" '" + sourceFileName + ".dat' using 1:90:91 axis x1y1 title \"Map Meantime(128MB)\" w yerrorlines;";
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

	public static void plotReduceCompletionTime(File destPath, String sourceFileName, String destFileName){
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +
				"set yrange [0:600];" + 
				"set xrange [0:32];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"plot '" + sourceFileName + ".dat' using 1:28:29 axis x1y1 title \"Reduce Meantime(32MB)\" w yerrorlines," + 
				" '" + sourceFileName + ".dat' using 1:66:67 axis x1y1 title \"Reduce Meantime(64MB)\" w yerrorlines," + 
				" '" + sourceFileName + ".dat' using 1:104:105 axis x1y1 title \"Reduce Meantime(128MB)\" w yerrorlines;";
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

	public static void plotMapReduceCompletionTime(File destPath, String sourceFileName, String destFileName){
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" +
				"set yrange [0:600];" + 
				"set xrange [0:32];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"plot '" + sourceFileName + ".dat' using 1:14:15 axis x1y1 title \"Map Meantime(32MB)\" w yerrorlines," + 
				" '" + sourceFileName + ".dat' using 1:52:53 axis x1y1 title \"Map Meantime(64MB)\" w yerrorlines," +
				" '" + sourceFileName + ".dat' using 1:90:91 axis x1y1 title \"Map Meantime(128MB)\" w yerrorlines," +
				" '" + sourceFileName + ".dat' using 1:28:29 axis x1y1 title \"Reduce Meantime(32MB)\" w yerrorlines," + 
				" '" + sourceFileName + ".dat' using 1:66:67 axis x1y1 title \"Reduce Meantime(64MB)\" w yerrorlines," + 
				" '" + sourceFileName + ".dat' using 1:104:105 axis x1y1 title \"Reduce Meantime(128MB)\" w yerrorlines;";
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

	public static void plotCDF(Rengine r_engine, Histogram times, String resultPath, String fileName, PrintMode printMode) throws IOException {
		File resultDir = new File(resultPath);
		resultDir.mkdir();

		LoggedDiscreteCDF cdf = new LoggedDiscreteCDF();
		int[] attemptTimesPercentiles = new int[99];
		for (int i = 0; i < 99; ++i) {
			attemptTimesPercentiles[i] = (i + 1) * 1;
		}	

		FileWriter dataFile = new FileWriter(new File(resultPath + "/" + fileName + ".dat"));
		BufferedWriter writer = new BufferedWriter(dataFile);		
		cdf.setCDF(times, attemptTimesPercentiles, 100);		
		List<LoggedSingleRelativeRanking> lsrrList = cdf.getRankings();
		if(lsrrList != null && lsrrList.size() > 0){
			for (LoggedSingleRelativeRanking lsrr : lsrrList) {
				String tmp = lsrr.getDatum() + " " + lsrr.getRelativeRanking();
				if(printMode.equals(PrintMode.prettyPrint)){
					System.out.println(tmp);
				}
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
			r_engine.eval("data <- read.table(\"" + resultPath + "/" + fileName +  ".dat\", header=F, sep=\" \")");
			r_engine.eval("xs <- data$V1");
			r_engine.eval("xf <- data$V2");
			r_engine.eval("png(filename=\"" + resultPath + "/" + fileName + "\")");
			r_engine.eval("plot(xs, xf, xlab=\"Runtime(ms)\", ylab=\"Cumulative frequency\")");
			r_engine.eval("dev.off()");
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}