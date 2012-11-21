package org.apache.hadoop.analyzer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.types.GraphParameters;
import org.apache.hadoop.types.MapTime;
import org.apache.hadoop.types.PhaseTime;
import org.apache.hadoop.types.ReduceTime;
import org.apache.hadoop.view.JobExecutionViewer;
import org.codehaus.jackson.map.ObjectMapper;
import org.rosuda.JRI.Rengine;

//TODO Quais as attempts que apresentam valores fora do comum? Por quê?
//TODO Retirar o agrupamento por tamanho do cluster e criar parâmetro para isto
//TODO Rever os tempos de execucao das tarefas
//TODO analisar dados colhidos
//TODO rever os tempos das fases
//TODO Os valores das ondas não estao sendo corretos, exemplo: desc_1.00, tem NL_Map terminando depois do reduce começar
//TODO Há uma certa distância entre algumas atividades de reduce. Acontece apenas para os casos sem MapShuffle?
//TODO por que o reduce apresenta tão pouco tempo?
//TODO Plotar uma CDF com a conclusão das tarefas, junto com a da nao-localidade
//TODO Salvar no github

public class ExecutionAnalyzer {

	//	private static final String path = "/home/thiago/tmp/experiment/1st_experiment/10x/";
	// private static final String path = "/home/thiago/tmp/experiment/2nd_experiment/10x/";
	private static final String path = "/home/thiago/tmp/experiment/2nd_experiment/20x/";
	//	private static final String path = "/home/thiago/tmp/experiment/3rd_experiment/desc_1.00/";

	private static final String tracePath = path + "traces/";
	private static final String resultPath = path + "results/";
	private static final String dataDir = "data/";

	private static Rengine r_engine;
	static {
		String[] args = new String[1];
		args[0] = "--save";
		r_engine = new Rengine(args, false, null);
	}

	private static final String gnuplotTerminal = "postscript eps enhanced";
	private static final String gnuplotOutType = ".eps";
	//	private static final String gnuplotTerminal = "png";
	//	private static final String gnuplotOutType = ".png";

	private static enum PrintMode {
		prettyPrint, inline, fullInline, disabled
	};

	private static enum GraphTypes {
		JobTimeLines, JobTimeBarComp, JobSpeedupLines, JobSpeedupBarLines, JobReduceTimeLines, JobReduceTimeBars, PhasesEvaluation, PhasesPercentEvaluation,
		MapLocality, MapNLScatter, MapNLECDF, MapCompletionTime, MapReduceCompletionTime, ReduceTimeLines, ReduceTimeBars, WaveLines, CDF
	};

	public static void main(String... args) throws Exception {

		// Dummy parameters
		// String[] jobNames = {"CountUpDriver","CountUpText","CountUp","JxtaSocketPerfDriver"};
		// String[][] groups = {{"128-90","64-180","32-360"},{"128-30","64-60","32-120"}};//{Max,Min}
		// String[] groupNames = {"Max","Min"};
		
		Map<GraphTypes, GraphParameters> graphTypes = new HashMap<GraphTypes,GraphParameters>();		
		Map<Integer,String> labels = new HashMap<Integer,String>();
		labels.put(0,"CountUpDriver");
		labels.put(1,"P3");
		Map<Integer,String> sources = new HashMap<Integer,String>();
		sources.put(0,"CountUpDriverMax");
		sources.put(1,"P3Max");
		graphTypes.put(GraphTypes.JobTimeBarComp, new GraphParameters(0, 0, 0, 550, sources, labels));		
		Map<Integer,Integer> consts = new HashMap<Integer,Integer>();
		consts.put(0,872);
		consts.put(1,571);
		consts.put(2,745);		
		graphTypes.put(GraphTypes.JobSpeedupBarLines, new GraphParameters(0, 0, 0, 550, 0, 16, consts));		
		graphTypes.put(GraphTypes.JobReduceTimeLines, new GraphParameters(0, 30, 0, 550));
		graphTypes.put(GraphTypes.JobReduceTimeBars, new GraphParameters(0, 30, 0, 550));		
		graphTypes.put(GraphTypes.PhasesEvaluation, new GraphParameters(0, 0, 0, 550));
		graphTypes.put(GraphTypes.PhasesPercentEvaluation, null);
		graphTypes.put(GraphTypes.MapNLScatter, new GraphParameters(0, 1, 0, 30));
		graphTypes.put(GraphTypes.MapNLECDF, null);
		graphTypes.put(GraphTypes.ReduceTimeBars, new GraphParameters(0, 0, 0, 350));
		graphTypes.put(GraphTypes.ReduceTimeLines, new GraphParameters(0, 30, 0, 350));
		graphTypes.put(GraphTypes.WaveLines, new GraphParameters(-1, 550, -1, 400));		

		String[] jobNames = { "CountUpDriver", "JxtaSocketPerfDriver", "P3"};
		String[][] groups = {{"32-360", "64-180", "128-90"},{"32-120","64-60","128-30"}};
		String[] groupNames = { "Max","Min" };

		// Creates the job keys for classifications, composed by jobName and sufix
		// <String,SortedMap<String,List<LoggedJob>>> == <groupItem<clusterSize,jobs>>
		SortedMap<String, SortedMap<String, List<LoggedJob>>> jobs = new TreeMap<String, SortedMap<String, List<LoggedJob>>>();
		for (String jobName : jobNames) {
			for (String[] group : groups) {
				for (String groupSufix : group) {
					jobs.put(jobName + "_" + groupSufix, new TreeMap<String, List<LoggedJob>>());
				}
			}
		}

		classifyJobsByNameSufix(getTraceFiles(tracePath), jobs);

		// Organizes jobs in groups and plots graphs for each group
		for (String jobName : jobNames) {// [P3|CountUpDriver|Jxta]
			
			int groupIndex = 0;
			for (String[] group : groups) {// [Max|Min]

				SortedMap<String, SortedMap<String, List<LoggedJob>>> groupedJobs = new TreeMap<String, SortedMap<String, List<LoggedJob>>>();
				for (String groupSufix : group) {// 32 || 64 || 128
					Set<String> jobIDs = jobs.keySet();
					for (String jobID : jobIDs) {
						if (jobID.contains(jobName) && jobID.contains(groupSufix)) {							
							groupedJobs.put(jobID, jobs.get(jobID));
						}
					}
				}

				// plots graphs for each group
				if (groupedJobs.size() > 0) {
					String jobGroupName = jobName + groupNames[groupIndex];
					System.out.println("### " + jobGroupName);
					SortedMap<String, Histogram[]> attemptTimes = generateJobsStatistics(groupedJobs, resultPath, jobGroupName);
					plotGraphs(groupedJobs, attemptTimes, resultPath, jobGroupName, jobGroupName, graphTypes);
				}

				groupIndex++;
			}
		}

		r_engine.end();
	}

	/**
	 * Generate a SortedMap with all trace files from the tracePath. The first token of each file is the key 
	 * 
	 * @param tracePath
	 * @return
	 */
	private static SortedMap<String, File> getTraceFiles(String tracePath) {
		SortedMap<String, File> traceFiles = new TreeMap<String, File>();

		File traceDir = new File(tracePath);
		File[] files = traceDir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith("-job-trace.json");
			}
		});

		try {
			for (File file : files) {
				StringTokenizer fileName = new StringTokenizer(file.getName(), "-");
				String index = fileName.nextToken();
				traceFiles.put(index, file);
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}

		return traceFiles;
	}

	private static SortedMap<String, File> getNLDFiles(String nldPath) {
		// NameExample: CountUpDriver_128-90_25_MapNLDispersion
		SortedMap<String, File> nldFiles = new TreeMap<String, File>();

		File nldDir = new File(nldPath);
		File[] files = nldDir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.endsWith("MapNLDispersion.dat");
			}
		});

		try {
			for (File file : files) {
				StringTokenizer tokenizer = new StringTokenizer(file.getName(), "_");
				tokenizer.nextToken();
				tokenizer.nextToken();
				String index = tokenizer.nextToken();
				nldFiles.put(index, file);
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}

		return nldFiles;
	}

	private static void classifyJobsByNameSufix(SortedMap<String, File> traceFiles, SortedMap<String, SortedMap<String, List<LoggedJob>>> jobs) {
		ObjectMapper mapper = new ObjectMapper();
		Set<String> traceKeys = traceFiles.keySet();
		for (String traceKey : traceKeys) {
			try {
				File traceFile = traceFiles.get(traceKey);
				Iterator<LoggedJob> parsedJobs = mapper.reader(LoggedJob.class).readValues(traceFile);
				while (parsedJobs.hasNext()) {
					classifyJobByNameSufix(traceKey, parsedJobs.next(), jobs);
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	private static void countUpClassifier(String newJobKey,LoggedJob newJob, SortedMap<String, SortedMap<String, List<LoggedJob>>> jobs) {
		List<LoggedTask> mapTasks = newJob.getMapTasks();
		String sufix = null;
		
		if(mapTasks != null){
			long inputSize = mapTasks.get(0).getInputBytes();
			
			if(inputSize > 0){
				if(inputSize > 31000000 && inputSize < 34000000){
					if(mapTasks.size() == 120){
						sufix = "32-120";							
					}else if(mapTasks.size() == 360){
						sufix = "32-360";
					}	
				}else if(inputSize > 63000000 && inputSize < 68000000){
					if(mapTasks.size() == 60){
						sufix = "64-60";
					}else if(mapTasks.size() == 180){
						sufix = "64-180";
					}
				}else if(inputSize > 127000000 && inputSize < 135000000){
					if(mapTasks.size() == 30){
						sufix = "128-30";
					}else if(mapTasks.size() == 90){
						sufix = "128-90";
					}
				}
			}
			
			if(sufix != null){
				String jobName = "P3";

				Set<String> jobIDS = jobs.keySet();
				for (String jobID : jobIDS) {					
					StringTokenizer jobIDTokenizer = new StringTokenizer(jobID, "_");
					String jobIDName = (String) jobIDTokenizer.nextElement();
					String jobIDSufix = (String) jobIDTokenizer.nextElement();					
					if (jobIDName.equals(jobName) && jobIDSufix.equals(sufix)) {
						updateJobsMap(jobs.get(jobID), newJobKey, newJob);
						break;
					}
				}
			}else{
				System.out.println("### Number of Tasks Error: " + newJob.getJobID());
				JobExecutionViewer.jobPrettyPrint(newJob);
			}
		}
	}

	private static void classifyJobByNameSufix(String newJobKey, LoggedJob newJob, SortedMap<String, SortedMap<String, List<LoggedJob>>> jobs) {	
		String newJobName = newJob.getJobName();
		
		if(newJobName.equals("CountUp")){
			countUpClassifier(newJobKey,newJob, jobs);	
		}else{
			Set<String> jobIDS = jobs.keySet();
			for (String jobID : jobIDS) {
				StringTokenizer jobIDTokenizer = new StringTokenizer(jobID, "_");
				String jobIDName = (String) jobIDTokenizer.nextElement();
				String jobIDSufix = (String) jobIDTokenizer.nextElement();
				if (newJobName.contains(jobIDName) && newJobName.contains(jobIDSufix)) {
					updateJobsMap(jobs.get(jobID), newJobKey, newJob);
					break;
				}
			}
		}
	}

	private static SortedMap<String, Histogram[]> generateJobsStatistics(SortedMap<String, SortedMap<String, List<LoggedJob>>> jobGroup, String resultPath,
			String jobGroupName) {

		Set<String> jobGroupKeys = jobGroup.keySet();		
		List<String> keyList = new ArrayList<String>(jobGroupKeys);
		Collections.sort(keyList, new Comparator<String>() {
			public int compare(String a, String b) {
				
				StringTokenizer tokensA = new StringTokenizer(a,"_");
				tokensA.nextElement();				
				Integer left =  Integer.parseInt(new StringTokenizer(tokensA.nextElement().toString(),"-").nextElement().toString());
				
				StringTokenizer tokensB = new StringTokenizer(b,"_");
				tokensB.nextElement();				
				Integer right =  Integer.parseInt(new StringTokenizer(tokensB.nextElement().toString(),"-").nextElement().toString());
				
				return left.compareTo(right);
			}
		});		
		
		SortedMap<String, Histogram[]> attemptTimes = new TreeMap<String, Histogram[]>();
		SortedSet<String> clusterSizes = new TreeSet<String>();
		for (String jobKey : keyList) {
			Histogram[] histograms = new Histogram[2];
			histograms[0] = new Histogram();
			histograms[1] = new Histogram();
			attemptTimes.put(jobKey, histograms);
			clusterSizes.addAll(jobGroup.get(jobKey).keySet());
		}

		try {
			File dataPath = new File(resultPath + "/" + jobGroupName + "/" + dataDir + "/");
			File wavesPath = new File(resultPath + "/" + jobGroupName + "/waves/");

			dataPath.mkdirs();
			wavesPath.mkdirs();

			BufferedWriter fullWriter = new BufferedWriter(new FileWriter(dataPath + "/" + jobGroupName + ".dat"));
			BufferedWriter mapNLECDFWriter = new BufferedWriter(new FileWriter(dataPath + "/" + jobGroupName + "MapNLECDF.dat"));

			for (String clusterSize : clusterSizes) {

				System.out.print(clusterSize);
				fullWriter.write(String.valueOf(clusterSize));

				for (String jobKey : keyList) {
					SortedMap<String, List<LoggedJob>> jobs = jobGroup.get(jobKey);
					if (jobs.containsKey(clusterSize)) {
						String dataSourcePrefix = dataPath + "/" + jobKey + "_" + clusterSize;
						BufferedWriter mapLocalWavesWriter = new BufferedWriter(new FileWriter(dataSourcePrefix + "_Map.dat"));
						BufferedWriter mapNLWavesWriter = new BufferedWriter(new FileWriter(dataSourcePrefix + "_MapNL.dat"));
						BufferedWriter mapNLDispersionWriter = new BufferedWriter(new FileWriter(dataSourcePrefix + "_MapNLDispersion.dat"));
						BufferedWriter reduceWavesWriter = new BufferedWriter(new FileWriter(dataSourcePrefix + "_Reduce.dat"));
						generateStatisticData(jobs.get(clusterSize), PrintMode.fullInline, fullWriter, mapLocalWavesWriter, mapNLWavesWriter,
								mapNLDispersionWriter, reduceWavesWriter, attemptTimes.get(jobKey), Integer.parseInt(clusterSize), mapNLECDFWriter);
						mapLocalWavesWriter.close();
						mapNLWavesWriter.close();
						reduceWavesWriter.close();
					}
				}

				System.out.println();
				fullWriter.newLine();
			}

			fullWriter.close();
			mapNLECDFWriter.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return attemptTimes;
	}

	private static void updateJobsMap(SortedMap<String, List<LoggedJob>> jobsMap, String key, LoggedJob job) {
		List<LoggedJob> jobs = jobsMap.get(key);
		if (jobs != null) {
			jobsMap.get(key).add(job);
		} else {
			jobs = new ArrayList<LoggedJob>();
			jobs.add(job);
			jobsMap.put(key, jobs);
		}
	}

	private static Integer[] generateStatisticData(List<LoggedJob> jobs, PrintMode printMode, BufferedWriter fullDataWriter,
			BufferedWriter mapLocalWavesWriter, BufferedWriter mapNLWavesWriter, BufferedWriter mapNLDispersionWriter, BufferedWriter reduceWavesWriter,
			Histogram[] histograms, Integer clusterSize, BufferedWriter mapNLECDFWriter) {
		DescriptiveStatistics jobRuntimeStats = new DescriptiveStatistics();
		DescriptiveStatistics jobSetupTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics jobCleanupTimeStats = new DescriptiveStatistics();

		DescriptiveStatistics mapNumStats = new DescriptiveStatistics();
		DescriptiveStatistics mapTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics mapShuffleTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics mapNLStats = new DescriptiveStatistics();
		DescriptiveStatistics mapOutInStats = new DescriptiveStatistics();
		DescriptiveStatistics mapSuccessAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics mapFailedAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics mapKilledAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics mapAttemptStats = new DescriptiveStatistics();
		SortedMap<Integer, DescriptiveStatistics[]> mapLocalWaves = new TreeMap<Integer, DescriptiveStatistics[]>();
		SortedMap<Integer, DescriptiveStatistics[]> mapNLWaves = new TreeMap<Integer, DescriptiveStatistics[]>();

		DescriptiveStatistics reduceNumStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceTaskTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceShuffleTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceSortTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceTimeStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceNotLocalAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceSuccessAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceFailedAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceKilledAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics reduceAttemptStats = new DescriptiveStatistics();
		SortedMap<Integer, DescriptiveStatistics[]> reduceWaves = new TreeMap<Integer, DescriptiveStatistics[]>();

		List<Double> mapNLDispersion = new ArrayList<Double>();

		for (LoggedJob job : jobs) {
			double jobTime = (job.getFinishTime() - job.getLaunchTime()) / 1000;

			if (jobTime > 0) {
				List<Long> mapNLStartTimes = new ArrayList<Long>();
				jobRuntimeStats.addValue(jobTime);

				// Setup and Cleanup
				getJobStatistics(job.getOtherTasks(), jobSetupTimeStats, jobCleanupTimeStats);

				// Map
				PhaseTime mapPhaseTimes = getMapStatistics(job.getMapTasks(), mapNumStats, mapTimeStats, mapNLStats, mapOutInStats, mapSuccessAttemptStats,
						mapKilledAttemptStats, mapFailedAttemptStats, mapAttemptStats, histograms[0], mapLocalWaves, mapNLWaves, mapNLStartTimes);

				// Reduce
				getReduceStatistics(mapPhaseTimes.getMinFinishTime(), mapPhaseTimes.getMaxFinishTime(), job.getReduceTasks(), reduceNumStats,
						mapShuffleTimeStats, reduceShuffleTimeStats, reduceSortTimeStats, reduceTimeStats, reduceTaskTimeStats, reduceNotLocalAttemptStats,
						reduceSuccessAttemptStats, reduceKilledAttemptStats, reduceFailedAttemptStats, reduceAttemptStats, histograms[1], reduceWaves);

				// Not Local Map Dispersion
				for (Long mapNLStartTime : mapNLStartTimes) {
					double nlStart = mapNLStartTime - job.getLaunchTime();
					double mapStartTime = mapPhaseTimes.getMaxStartTime() - job.getLaunchTime();
					mapNLDispersion.add(nlStart / mapStartTime);
				}
			}
		}

		// Print Statistics
		if (printMode == PrintMode.fullInline) {
			serializeStats(jobRuntimeStats, jobSetupTimeStats, jobCleanupTimeStats, mapNumStats, mapTimeStats, mapShuffleTimeStats, mapNLStats,
					mapOutInStats, mapSuccessAttemptStats, mapKilledAttemptStats, mapFailedAttemptStats, mapAttemptStats, reduceNumStats,
					reduceTaskTimeStats, reduceShuffleTimeStats, reduceSortTimeStats, reduceTimeStats, reduceNotLocalAttemptStats, reduceSuccessAttemptStats,
					reduceKilledAttemptStats, reduceFailedAttemptStats, reduceAttemptStats, fullDataWriter, mapLocalWavesWriter, mapLocalWaves,
					mapNLWavesWriter, mapNLDispersionWriter, mapNLWaves, reduceWavesWriter, reduceWaves, mapNLDispersion, mapNLECDFWriter);
		}

		Integer[] axes = new Integer[2];
		axes[0] = ((Double) jobRuntimeStats.getMean()).intValue();
		axes[1] = ((Double) mapNumStats.getMean()).intValue() + ((Double) reduceNumStats.getMean()).intValue();		

		return axes;
	}

	private static void plotGraphs(SortedMap<String, SortedMap<String, List<LoggedJob>>> groupedJobs, SortedMap<String, Histogram[]> attemptTimes,
			String resultPath, String sourceFileName, String jobGroupName, Map<GraphTypes, GraphParameters> graphTypes) throws IOException {
		File resultDir = new File(resultPath + "/" + jobGroupName + "/");
		resultDir.mkdirs();

		Set<GraphTypes> keys = graphTypes.keySet();

		for (GraphTypes key : keys) {
			switch (key) {

			case JobTimeLines:
				plotJobTimeLines(resultDir, sourceFileName, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;
				
			case JobTimeBarComp:
				plotJobTimeBarComp(resultDir, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case JobSpeedupLines:
				plotJobSpeedupLines(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;
				
			case JobSpeedupBarLines:
				plotJobSpeedupBarLines(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case JobReduceTimeLines:
				plotJobReduceTimeLines(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case JobReduceTimeBars:
				plotJobReduceTimeBars(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case PhasesEvaluation:
				plotPhasesEvaluation(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case PhasesPercentEvaluation:
				plotPhasesPercentEvaluation(resultDir, sourceFileName, jobGroupName + key.name());
				break;

			case MapLocality:
				plotMapLocality(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case MapNLScatter:
				SortedMap<String, File> NLDFiles = getNLDFiles(resultPath + "/" + jobGroupName + "/" + dataDir + "/"); 
				plotMapNLScatter(resultDir, NLDFiles, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case MapNLECDF:
				plotMapNLECDF(r_engine, resultDir, dataDir, jobGroupName + key.name());
				break;

			case MapCompletionTime:
				plotMapCompletionTime(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case ReduceTimeBars:
				plotReduceTimeBars(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case ReduceTimeLines:
				plotReduceTimeLines(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case MapReduceCompletionTime:
				plotMapReduceCompletionTime(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case WaveLines:
				plotWaves(groupedJobs, resultDir, jobGroupName, graphTypes.get(key));				
				break;

			case CDF:
				plotCDFs(r_engine, attemptTimes, jobGroupName, resultPath, dataDir);
				break;

			default:
				System.out.println("Graph type not found!");
				break;
			}
		}

	}

	private static void plotWaves(SortedMap<String, SortedMap<String, List<LoggedJob>>> groupedJobs, File resultPath, String jobGroupName, GraphParameters params) {
		Set<String> jobGroupKeys = groupedJobs.keySet();
		SortedSet<String> clusterSizes = new TreeSet<String>();

		for (String jobKey : jobGroupKeys) {
			clusterSizes.addAll(groupedJobs.get(jobKey).keySet());
		}

		try {

			for (String clusterSize : clusterSizes) {
				for (String jobKey : jobGroupKeys) {
					SortedMap<String, List<LoggedJob>> jobs = groupedJobs.get(jobKey);
					if (jobs.containsKey(clusterSize)) {
						String sourcePrefix = jobKey + "_" + clusterSize;
						plotWaveLines(resultPath, sourcePrefix, sourcePrefix, params);
					}
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private static void plotCDFs(Rengine rengine, SortedMap<String, Histogram[]> attemptTimes, String jobGroupName, String resultPath, String dataPath)
			throws IOException {

		Set<String> names = attemptTimes.keySet();
		for (String fileName : names) {

			String resultPathStr = resultPath + "/" + jobGroupName + "/";
			String dataPathStr = resultPath + "/" + jobGroupName + "/" + dataPath;
			Histogram[] histogram = attemptTimes.get(fileName);

			// Plots Map CDF
			plotCDF(rengine, histogram[0], resultPathStr, dataPathStr, fileName + "_CDFMap");

			// Plots Reduce CDF
			plotCDF(rengine, histogram[1], resultPathStr, dataPathStr, fileName + "_CDFReduce");
		}

	}

	// Public Methods
	// ------------------------------------------------------------------------------------------------------------------------------------

	public static void getJobStatistics(List<LoggedTask> tasks, DescriptiveStatistics jobSetupTimeStats, DescriptiveStatistics jobCleanupTimeStats) {

		if (tasks != null) {

			for (LoggedTask task : tasks) {

				if (task.getTaskType().equals(Pre21JobHistoryConstants.Values.SETUP)) {
					double setupTime = (task.getFinishTime() - task.getStartTime()) / 1000;
					if (setupTime > 0) {
						jobSetupTimeStats.addValue(setupTime);
					}
				} else if (task.getTaskType().equals(Pre21JobHistoryConstants.Values.CLEANUP)) {
					double cleanupTime = (task.getFinishTime() - task.getStartTime()) / 1000;
					if (cleanupTime > 0) {
						jobCleanupTimeStats.addValue(cleanupTime);
					}
				}

			}

		}
	}

	public static PhaseTime getMapStatistics(List<LoggedTask> tasks, DescriptiveStatistics numTasksStats, DescriptiveStatistics mapTaskTimeStats,
			DescriptiveStatistics mapNLStats, DescriptiveStatistics outInStats, DescriptiveStatistics successAttemptStats,
			DescriptiveStatistics killedAttemptStats, DescriptiveStatistics failedAttemptStats, DescriptiveStatistics totalAttemptStats,
			Histogram attemptTimes, SortedMap<Integer, DescriptiveStatistics[]> mapLocalWaves, SortedMap<Integer, DescriptiveStatistics[]> mapNLWaves,
			List<Long> mapNLStartTime) {

		double minStartTime = Double.MAX_VALUE;
		double maxStartTime = Double.MIN_VALUE;

		double minFinishTime = Double.MAX_VALUE;
		double maxFinishTime = Double.MIN_VALUE;

		List<MapTime> mapTimes = new ArrayList<MapTime>();
		List<MapTime> nlMapTimes = new ArrayList<MapTime>();

		if (tasks != null) {
			numTasksStats.addValue(tasks.size());
			for (LoggedTask mapTask : tasks) {
				double mapNotLocalAttempt = 0;
				double mapSuccessAttempt = 0;
				double mapFailedAttempt = 0;
				double mapKilledAttempt = 0;
				double outInRate = (mapTask.getOutputBytes() / mapTask.getInputBytes()) * 100;
				outInStats.addValue(outInRate);

				if (mapTask.getStartTime() < minStartTime) {
					minStartTime = mapTask.getStartTime();
				}
				if (mapTask.getStartTime() > maxStartTime) {
					maxStartTime = mapTask.getStartTime();
				}

				if (mapTask.getFinishTime() < minFinishTime) {
					minFinishTime = mapTask.getFinishTime();
				}
				if (mapTask.getFinishTime() > maxFinishTime) {
					maxFinishTime = mapTask.getFinishTime();
				}

				boolean isLocal = false;
				List<LoggedLocation> dataLocations = mapTask.getPreferredLocations();
				List<LoggedTaskAttempt> attemptList = mapTask.getAttempts();
				// Attempts
				if (attemptList != null) {
					totalAttemptStats.addValue(attemptList.size());
					for (LoggedTaskAttempt mapAttempt : attemptList) {

						if (mapAttempt.getResult() == Pre21JobHistoryConstants.Values.SUCCESS) {
							mapSuccessAttempt++;
						} else if (mapAttempt.getResult() == Pre21JobHistoryConstants.Values.KILLED) {
							mapKilledAttempt++;
							continue;
						} else if (mapAttempt.getResult() == Pre21JobHistoryConstants.Values.FAILED) {
							mapFailedAttempt++;
						}

						if (mapAttempt.getLocation() != null) {
							for (LoggedLocation location : dataLocations) {
								isLocal = isLocal || location.getLayers().equals(mapAttempt.getLocation().getLayers());
							}
							if (!isLocal) {
								mapNotLocalAttempt++;
							}
						}

						if ((mapAttempt.getFinishTime() - mapAttempt.getStartTime()) > 0) {
							attemptTimes.enter(mapAttempt.getFinishTime() - mapAttempt.getStartTime());
						}
					}
				}
				mapNLStats.addValue(mapNotLocalAttempt);
				successAttemptStats.addValue(mapSuccessAttempt);
				killedAttemptStats.addValue(mapKilledAttempt);
				failedAttemptStats.addValue(mapFailedAttempt);

				MapTime mapTime = new MapTime(mapTask.getStartTime(), mapTask.getFinishTime());
				mapTimes.add(mapTime);
				if (!isLocal) {
					nlMapTimes.add(mapTime);
					mapNLStartTime.add(mapTask.getStartTime());
				}
			}
		}
		mapTaskTimeStats.addValue((maxFinishTime - minStartTime) / 1000);

		Collections.sort(mapTimes);
		long firstStartTime = mapTimes.get(0).getStartTime();
		int index = 0;

		for (MapTime mapTime : mapTimes) {
			DescriptiveStatistics[] stats = mapLocalWaves.get(index);
			if (stats == null) {
				stats = new DescriptiveStatistics[2];
				stats[0] = new DescriptiveStatistics();
				stats[0].addValue(mapTime.getStartTime() - firstStartTime);
				stats[1] = new DescriptiveStatistics();
				stats[1].addValue(mapTime.getFinishTime() - mapTime.getStartTime());
				mapLocalWaves.put(index, stats);
			} else {
				stats[0].addValue(mapTime.getStartTime() - firstStartTime);
				stats[1].addValue(mapTime.getFinishTime() - mapTime.getStartTime());
			}
			index++;
		}

		index = 0;
		Collections.sort(nlMapTimes);
		for (MapTime nlMapTime : nlMapTimes) {
			DescriptiveStatistics[] stats = mapNLWaves.get(index);
			if (stats == null) {
				stats = new DescriptiveStatistics[2];
				stats[0] = new DescriptiveStatistics();
				stats[0].addValue(nlMapTime.getStartTime() - firstStartTime);
				stats[1] = new DescriptiveStatistics();
				stats[1].addValue(nlMapTime.getFinishTime() - nlMapTime.getStartTime());
				mapNLWaves.put(index, stats);
			} else {
				stats[0].addValue(nlMapTime.getStartTime() - firstStartTime);
				stats[1].addValue(nlMapTime.getFinishTime() - nlMapTime.getStartTime());
			}
			index++;
		}

		return new PhaseTime(minStartTime, maxStartTime, minFinishTime, maxFinishTime);
	}

	public static void getReduceStatistics(Double firstMapStartTime, Double lastMapEndTime, List<LoggedTask> tasks, DescriptiveStatistics numTasksStats,
			DescriptiveStatistics mapShuffleTimeStats, DescriptiveStatistics shuffleTimeStats, DescriptiveStatistics sortTimeStats,
			DescriptiveStatistics reduceTimeStats, DescriptiveStatistics reduceTaskTimeStats, DescriptiveStatistics notLocalAttemptStats,
			DescriptiveStatistics successAttemptStats, DescriptiveStatistics killedAttemptStats, DescriptiveStatistics failedAttemptStats,
			DescriptiveStatistics totalAttemptStats, Histogram attemptTimes, SortedMap<Integer, DescriptiveStatistics[]> reduceWaves) {

		double minReduceTaskTime = Double.MAX_VALUE;
		double maxReduceTaskTime = Double.MIN_VALUE;

		double minReduceTime = Double.MAX_VALUE;
		double maxShuffleTime = Double.MIN_VALUE;
		double maxSortTime = Double.MIN_VALUE;
		double maxReduceTime = Double.MIN_VALUE;

		SortedSet<ReduceTime> reduceTimes = new TreeSet<ReduceTime>();

		if (tasks != null) {
			numTasksStats.addValue(tasks.size());
			for (LoggedTask reduceTask : tasks) {
				double notLocalAttempts = 0;
				double successAttempts = 0;
				double failedAttempts = 0;
				double killedAttempts = 0;

				if (reduceTask.getStartTime() < minReduceTaskTime) {
					minReduceTaskTime = reduceTask.getStartTime();
				}
				if (reduceTask.getFinishTime() > maxReduceTaskTime) {
					maxReduceTaskTime = reduceTask.getFinishTime();
				}

				Double relativeStartTime = reduceTask.getStartTime() - firstMapStartTime;
				Double mapShuffleTime = reduceTask.getStartTime() - lastMapEndTime;

				List<LoggedLocation> dataLocations = reduceTask.getPreferredLocations();
				List<LoggedTaskAttempt> attemptList = reduceTask.getAttempts();
				// Attempts
				if (attemptList != null) {
					totalAttemptStats.addValue(attemptList.size());
					for (LoggedTaskAttempt reduceAttempt : attemptList) {

						if (reduceAttempt.getResult() == Pre21JobHistoryConstants.Values.SUCCESS) {
							successAttempts++;
							if ((reduceAttempt.getFinishTime() - reduceAttempt.getStartTime()) > 0) {
								if (reduceAttempt.getStartTime() < minReduceTime) {
									minReduceTime = reduceAttempt.getStartTime();
								}
								if (reduceAttempt.getShuffleFinished() > maxShuffleTime) {
									maxShuffleTime = reduceAttempt.getShuffleFinished();
								}
								if (reduceAttempt.getSortFinished() > maxSortTime) {
									maxSortTime = reduceAttempt.getSortFinished();
								}
								if (reduceAttempt.getFinishTime() > maxReduceTime) {
									maxReduceTime = reduceAttempt.getFinishTime();
								}

								ReduceTime reduceTime = new ReduceTime(reduceAttempt.getStartTime(), reduceAttempt.getStartTime()
										+ mapShuffleTime.longValue(), reduceAttempt.getShuffleFinished(), reduceAttempt.getSortFinished(),
										reduceAttempt.getFinishTime(), relativeStartTime.longValue());
								reduceTimes.add(reduceTime);
							}

						} else if (reduceAttempt.getResult() == Pre21JobHistoryConstants.Values.KILLED) {
							killedAttempts++;
							continue;
						} else if (reduceAttempt.getResult() == Pre21JobHistoryConstants.Values.FAILED) {
							failedAttempts++;
						}

						if (reduceAttempt.getLocation() != null) {
							boolean isLocal = false;
							for (LoggedLocation location : dataLocations) {
								isLocal = isLocal || location.getLayers().equals(reduceAttempt.getLocation().getLayers());
							}
							if (!isLocal) {
								notLocalAttempts++;
							}
						}

						if ((reduceAttempt.getFinishTime() - reduceAttempt.getStartTime()) > 0) {
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
		double shufflePerc = (maxShuffleTime - minReduceTime) / reduceAttemptTime;
		double sortPerc = (maxSortTime - maxShuffleTime) / reduceAttemptTime;
		double reducePerc = (maxReduceTime - maxSortTime) / reduceAttemptTime;

		double reduceTaskTime = (maxReduceTaskTime - minReduceTaskTime) / 1000;
		double mapShuffleTime = (lastMapEndTime - minReduceTaskTime) / 1000;
		double shuffleTime = (shufflePerc * reduceTaskTime) - mapShuffleTime;
		double sortTime = sortPerc * reduceTaskTime;
		double reduceOnlyTime = reducePerc * reduceTaskTime;

		mapShuffleTimeStats.addValue(mapShuffleTime);
		shuffleTimeStats.addValue(shuffleTime);
		sortTimeStats.addValue(sortTime);
		reduceTimeStats.addValue(reduceOnlyTime);
		reduceTaskTimeStats.addValue(reduceTaskTime);

		int index = 0;
		for (ReduceTime reduceTime : reduceTimes) {
			DescriptiveStatistics[] stats = reduceWaves.get(index);
			if (stats != null) {
				stats[0].addValue(reduceTime.getRelativeStartTime());
				stats[1].addValue(reduceTime.getShuffleTime() - reduceTime.getStartTime());
				stats[2].addValue(reduceTime.getSortTime() - reduceTime.getShuffleTime());
				stats[3].addValue(reduceTime.getFinishTime() - reduceTime.getSortTime());
			} else {
				stats = new DescriptiveStatistics[4];
				stats[0] = new DescriptiveStatistics();
				stats[0].addValue(reduceTime.getRelativeStartTime());
				stats[1] = new DescriptiveStatistics();
				stats[1].addValue(reduceTime.getShuffleTime() - reduceTime.getStartTime());
				stats[2] = new DescriptiveStatistics();
				stats[2].addValue(reduceTime.getSortTime() - reduceTime.getShuffleTime());
				stats[3] = new DescriptiveStatistics();
				stats[3].addValue(reduceTime.getFinishTime() - reduceTime.getSortTime());
				reduceWaves.put(index, stats);
			}
			index++;
		}
	}

	public static void serializeStats(DescriptiveStatistics jobRuntimeStats, DescriptiveStatistics jobSetupTimeStats,
			DescriptiveStatistics jobCleanupTimeStats, DescriptiveStatistics mapTasksStats, DescriptiveStatistics mapTimeStats,
			DescriptiveStatistics mapShuffleTimeStats, DescriptiveStatistics mapNLStats, DescriptiveStatistics mapOutInStats,
			DescriptiveStatistics mapSuccessAttemptStats, DescriptiveStatistics mapKilledAttemptStats, DescriptiveStatistics mapFailedAttemptStats,
			DescriptiveStatistics mapAttemptStats, DescriptiveStatistics reduceNumStats, DescriptiveStatistics reduceTaskTimeStats,
			DescriptiveStatistics reduceShuffleTimeStats, DescriptiveStatistics reduceSortTimeStats, DescriptiveStatistics reduceTimeStats,
			DescriptiveStatistics reduceNotLocalAttemptStats, DescriptiveStatistics reduceSuccessAttemptStats,
			DescriptiveStatistics reduceKilledAttemptStats, DescriptiveStatistics reduceFailedAttemptStats, DescriptiveStatistics reduceAttemptStats,
			BufferedWriter fullDataWriter, BufferedWriter mapLocalWavesWriter, SortedMap<Integer, DescriptiveStatistics[]> mapLocalWaves,
			BufferedWriter mapNLWavesWriter, BufferedWriter mapNLDispersionWriter, SortedMap<Integer, DescriptiveStatistics[]> mapNLWaves,
			BufferedWriter reduceWavesWriter, SortedMap<Integer, DescriptiveStatistics[]> reduceWaves, List<Double> mapNLDispersion, 
			BufferedWriter mapNLECDFWriter) {

		StringBuilder output = new StringBuilder();
		DecimalFormat df2 = new DecimalFormat("#,###,###,##0.00");

		jobRuntimeStats = removeOutliers(jobRuntimeStats);
		mapTimeStats = removeOutliers(mapTimeStats);
		mapShuffleTimeStats = removeOutliers(mapShuffleTimeStats);
		reduceTaskTimeStats = removeOutliers(reduceTaskTimeStats);
		reduceShuffleTimeStats = removeOutliers(reduceShuffleTimeStats);
		reduceSortTimeStats = removeOutliers(reduceSortTimeStats);
		reduceTimeStats = removeOutliers(reduceTimeStats);

		// if(getConfidenceInterval(jobRuntimeStats) >= 6)
		// return;

		output.append("\t" + df2.format(jobRuntimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(jobRuntimeStats)));
		output.append("\t" + df2.format(jobSetupTimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(jobSetupTimeStats)));
		output.append("\t" + df2.format(jobCleanupTimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(jobCleanupTimeStats)));

		output.append("\t" + df2.format(mapTasksStats.getMean()) + "\t" + df2.format(getConfidenceInterval(mapTasksStats)));
		output.append("\t" + df2.format(mapTimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(mapTimeStats)));
		output.append("\t" + df2.format((mapNLStats.getMean() / mapNLStats.getMean()) * 100) + "\t" + df2.format(getConfidenceInterval(mapNLStats)));
		output.append("\t" + df2.format(mapOutInStats.getMean()) + "\t" + df2.format(getConfidenceInterval(mapOutInStats)));
		output.append("\t" + df2.format((mapSuccessAttemptStats.getMean() / mapAttemptStats.getMean()) * 100) + "\t" + 
				df2.format(getConfidenceInterval(mapSuccessAttemptStats)));
		output.append("\t" + df2.format((mapKilledAttemptStats.getMean() / mapAttemptStats.getMean()) * 100) + "\t" + 
				df2.format(getConfidenceInterval(mapKilledAttemptStats)));
		output.append("\t" + df2.format((mapFailedAttemptStats.getMean() / mapAttemptStats.getMean()) * 100) + "\t" + 
				df2.format(getConfidenceInterval(mapFailedAttemptStats)));
		output.append("\t" + df2.format(mapAttemptStats.getMean()) + "\t" + df2.format(getConfidenceInterval(mapAttemptStats)));

		output.append("\t" + df2.format(reduceNumStats.getMean()) + "\t" + df2.format(getConfidenceInterval(reduceNumStats)));
		output.append("\t" + df2.format(reduceTaskTimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(reduceTaskTimeStats)));
		output.append("\t" + df2.format(mapShuffleTimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(mapShuffleTimeStats)));
		output.append("\t" + df2.format(reduceShuffleTimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(reduceShuffleTimeStats)));
		output.append("\t" + df2.format(reduceSortTimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(reduceSortTimeStats)));
		output.append("\t" + df2.format(reduceTimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(reduceTimeStats)));
		output.append("\t" + df2.format((reduceNotLocalAttemptStats.getMean() / reduceAttemptStats.getMean()) * 100) + "\t" + 
				df2.format(getConfidenceInterval(reduceNotLocalAttemptStats)));
		output.append("\t" + df2.format((reduceSuccessAttemptStats.getMean() / reduceAttemptStats.getMean()) * 100) + "\t" + 
				df2.format(getConfidenceInterval(reduceSuccessAttemptStats)));
		output.append("\t" + df2.format((reduceKilledAttemptStats.getMean() / reduceAttemptStats.getMean()) * 100) + "\t" + 
				df2.format(getConfidenceInterval(reduceKilledAttemptStats)));
		output.append("\t" + df2.format((reduceFailedAttemptStats.getMean() / reduceAttemptStats.getMean()) * 100) + "\t" + 
				df2.format(getConfidenceInterval(reduceFailedAttemptStats)));
		output.append("\t" + df2.format(reduceAttemptStats.getMean()) + "\t" + df2.format(getConfidenceInterval(reduceAttemptStats)));

		System.out.print(output);

		if (fullDataWriter != null) {
			try {
				fullDataWriter.write(output.toString());
				fullDataWriter.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		int index = 1;

		if (mapLocalWavesWriter != null) {
			try {
				Set<Integer> keys = mapLocalWaves.keySet();
				for (Integer key : keys) {
					DescriptiveStatistics[] mapTimes = mapLocalWaves.get(key);
					String line = index + "\t" + ((Double) (mapTimes[0].getMean() / 1000)).intValue() + "\t"
							+ ((Double) (mapTimes[1].getMean() / 1000)).intValue();
					mapLocalWavesWriter.write(line);
					mapLocalWavesWriter.newLine();
					mapLocalWavesWriter.flush();
					index++;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (mapNLWavesWriter != null) {
			try {
				Set<Integer> keys = mapNLWaves.keySet();
				for (Integer key : keys) {
					DescriptiveStatistics[] mapTimes = mapNLWaves.get(key);
					String line = index + "\t" + ((Double) (mapTimes[0].getMean() / 1000)).intValue() + "\t"
							+ ((Double) (mapTimes[1].getMean() / 1000)).intValue();
					mapNLWavesWriter.write(line);
					mapNLWavesWriter.newLine();
					mapNLWavesWriter.flush();
					index++;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		if (mapNLDispersionWriter != null) {
			try {
				for (Double startTime : mapNLDispersion) {
					mapNLDispersionWriter.write(startTime.toString());
					mapNLDispersionWriter.newLine();
					mapNLDispersionWriter.flush();					
					mapNLECDFWriter.write(startTime.toString());
					mapNLECDFWriter.newLine();
					mapNLECDFWriter.flush();
					index++;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		index = mapLocalWaves.keySet().size() + mapNLWaves.keySet().size();
		if (reduceWavesWriter != null) {
			try {
				Set<Integer> keys = reduceWaves.keySet();
				for (Integer key : keys) {
					DescriptiveStatistics[] reduceTimes = reduceWaves.get(key);
					String line = index + "\t" + ((Double) (reduceTimes[0].getMean() / 1000)).intValue() + "\t"
							+ ((Double) (reduceTimes[1].getMean() / 1000)).intValue() + "\t" + ((Double) (reduceTimes[2].getMean() / 1000)).intValue() + "\t"
							+ ((Double) (reduceTimes[3].getMean() / 1000)).intValue();
					reduceWavesWriter.write(line);
					reduceWavesWriter.newLine();
					reduceWavesWriter.flush();
					index++;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static double getConfidenceInterval(DescriptiveStatistics stats) {

		double stdv = stats.getStandardDeviation();
		double n = stats.getN();
		double mean = stats.getMean();
		double ci = (1.96 * (stdv / Math.sqrt(n)));
		double rci = (ci/mean) * 100;

		return rci;
	}

	private static DescriptiveStatistics removeOutliers(DescriptiveStatistics statistic) {
		double[] values = statistic.getValues();
		double max = statistic.getMax();
		double min = statistic.getMin();

		statistic = new DescriptiveStatistics();
		for (double value : values) {
			if (value != max && value != min) {
				statistic.addValue(value);
			}
		}
		return statistic;
	}

	public static void plotJobTimeLines(File resultDir, String sourceFileName, String monoFileName, String destFileName, GraphParameters params) {
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";"	+ 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" +
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" +				 
				"set xlabel \"Nodes\";" + 
				"set ylabel \"Runtime(s)\";" +
				"set key center right;" + 
				"plot 'data/" + sourceFileName + ".dat' using 1:2:3 title \"JobRuntime(32MB)\" w yerrorlines," + "'data/" + sourceFileName + 
				".dat' using 1:40:41 title \"JobRuntime(64MB)\" w yerrorlines," + "'data/" + sourceFileName + 
				".dat' using 1:78:79 title \"JobRuntime(128MB)\" w yerrorlines," + "'data/" + monoFileName + 
				".dat' using (0):3:(1):(0) axes x2y1 with vector nohead filled lt 3 title \"MonoRuntime\";";
		try {
			ProcessBuilder process = new ProcessBuilder("gnuplot", "-e", strCommands);
			process.directory(resultDir);
			process.start();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void plotJobTimeBarComp(File destPath, String destFileName, GraphParameters parameters) {
		
		Map<Integer,String> sources = parameters.getSources();
		Map<Integer,String> labels = parameters.getLabels();
		
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";"	+ 
				"set yrange [" + parameters.getyMin() + ":" + parameters.getyMax() + "];" + 
				"set style data histogram;" + 
				"set style histogram errorbars gap 2 lw 3;" + 
				"set style fill pattern border;" + 
				"set boxwidth 0.9;" + 
				"plot " + 
				"'data/" + sources.get(0) + ".dat' u 2:3:xtic(1) lt 1 t \"" + labels.get(0) + "(32MB)\", " + 
				"'../" + sources.get(1) + "/data/" + sources.get(1) + ".dat' u 2:3 lt 1 t \"" + labels.get(1) + "(32MB)\", " + 
				"'data/" + sources.get(0) + ".dat' u 46:47:xtic(1) lt 1 t \"" + labels.get(0) + "(64MB)\"," + 
				"'../" + sources.get(1) + "/data/" + sources.get(1) + ".dat' u 46:47 lt 1 t \"" + labels.get(1) + "(64MB)\", " + 
				"'data/" + sources.get(0) + ".dat' u 90:91:xtic(1) lt 1 t \"" + labels.get(0) + "(128MB)\"," + 
				"'../" + sources.get(1) + "/data/" + sources.get(1) + ".dat' u 90:91 lt 1 t \"" + labels.get(1) + "(128MB)\"";
		try {
			//TODO
			System.out.println(strCommands);
			ProcessBuilder process = new ProcessBuilder("gnuplot", "-e", strCommands);
			process.directory(destPath);
			process.start();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void plotJobSpeedupLines(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		
		Map<Integer,Integer> consts = params.getConstants();
		
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";"	+ 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" +
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" +
				"set y2range [" + params.getyMin2() + ":" + params.getyMax2() + "];" + "set y2tics;" +
				"set xlabel \"Nodes\";" + 
				"set ytics nomirror;" + 
				"set ylabel \"Runtime(s)\";" +				 
				"set y2label \"Speedup\";" +				
				"set key top right;" + 
				"f(x) = " + consts.get(0) + " / x;" + 
				"g(x) = " + consts.get(1) + " / x;" + 
				"h(x) = " + consts.get(2) + " / x;" + 
				"plot 'data/" + sourceFileName + 
				".dat' using 1:2:3 axis x1y1 title \"JobRuntime(32MB)\" w yerrorlines, " + "'data/" + sourceFileName + 
				".dat' using 1:46:47 axis x1y1 title \"JobRuntime(64MB)\" w yerrorlines, " + "'data/" + sourceFileName + 
				".dat' using 1:90:91 axis x1y1 title \"JobRuntime(128MB)\" w yerrorlines," + "'data/" + sourceFileName + 
				".dat' using 1:( f($2) ) w lp lt 5 pt 5 axis x1y2 title \"Speedup(32MB)\", " + "'data/" + sourceFileName + 
				".dat' using 1:( g($46) ) w lp lt 7 pt 7 axis x1y2 title \"Speedup(64MB)\", " + "'data/" + sourceFileName + 
				".dat' using 1:( h($90) ) w lp lt 12 pt 9 axis x1y2 title \"Speedup(128MB)\";";
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
	
	public static void plotJobSpeedupBarLines(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		
		Map<Integer,Integer> consts = params.getConstants();
		
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";"	+ 
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set ytics nomirror;" + 
				"set ylabel \"Runtime(s)\";" + 
				"set y2range [" + params.getyMin2() + ":" + params.getyMax2() + "];" + 
				"set y2tics;" + 
				"set y2label \"Speedup\";" + 
				"set xlabel \"Nodes\";" + 
				"set key top right;" + "f(x) = " + consts.get(0) + " / x;" + "g(x) = " + consts.get(1) + " / x;" + "h(x) = " + consts.get(2) + " / x;" + 
				"set style data histogram;" + 
				"set style histogram errorbars gap 1.5 lw 3;" + 
				"set style fill pattern border;" + 
				"plot " + 
				"'data/" + sourceFileName + ".dat' u 2:3:xtic(1) lt 1 axis x1y1 t \"JobTime(32MB)\"," + 
				"'data/" + sourceFileName + ".dat' u 46:47:xtic(1) lt 1 axis x1y1 t \"JobTime(64MB)\"," + 
				"'data/" + sourceFileName + ".dat' u 90:91:xtic(1) lt 1 axis x1y1 t \"JobTime(128MB)\"," + 
				"'data/" + sourceFileName + ".dat' u ( f($2) ):xtic(1) w lp lt 0  pt 5 axis x1y2 t \"Speedup(32MB)\"," + 
				"'data/" + sourceFileName + ".dat' u ( g($46) ):xtic(1) w lp lt 5 pt 7 axis x1y2 t \"Speedup(64MB)\"," + 
				"'data/" + sourceFileName + ".dat' u ( h($90) ):xtic(1) w lp lt 7 pt 9 axis x1y2 t \"Speedup(128MB)\";";
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

	public static void plotJobReduceTimeLines(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" + 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" +
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"plot " +
				"'data/" + sourceFileName + ".dat' using 1:2 title \"Job\" w linesp," + 
				"'data/" + sourceFileName + ".dat' using 1:28 title \"MapShuffle\" w linesp," + 
				"'data/" + sourceFileName + ".dat' using 1:30 title \"Shuffle\" w linesp," + 
				"'data/" + sourceFileName + ".dat' using 1:37 title \"Reduce\" w linesp;";
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
	
	public static void plotJobReduceTimeBars(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" + 
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"set style data histogram;" + 
				"set style histogram errorbars gap 2 lw 3;" + "set style fill pattern border;" + 
				"set boxwidth 0.9;" + 
				"plot 'data/" + sourceFileName + ".dat' u 2:3:xtic(1) title \"Job\"," + 
				" 'data/" + sourceFileName + ".dat' u 28:29:xtic(1) title \"MapShuflle\"," + 
				" 'data/" + sourceFileName + ".dat' u 30:31:xtic(1) title \"Shuflle\"," + 
				" 'data/" + sourceFileName + ".dat' u 34:35:xtic(1) title \"Reduce\";";
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
	
	public static void plotPhasesEvaluation(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		String strCommands = "reset;" + 
				"set terminal "	+ gnuplotTerminal + ";"	+ 
				"set output \"" + destFileName + gnuplotOutType	+ "\";"	+ 
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
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "] noreverse nowriteback;" + 
				"plot " + 
				"newhistogram \"32MB\" fs pattern 1, 'data/" + sourceFileName	+ ".dat' " + 
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

	public static void plotPhasesPercentEvaluation(File destPath, String sourceFileName, String destFileName) {
		String strCommands = "reset;" + "set terminal " + gnuplotTerminal + ";" + 
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
				"newhistogram \"32MB\" fs pattern 1, 'data/" + sourceFileName +	".dat' " + 
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

	public static void plotMapNLScatter(File destPath, SortedMap<String, File> nldFiles, String destFileName, GraphParameters params) {
		StringBuilder str = new StringBuilder();
		str.append("reset;");
		str.append("set terminal ");
		str.append(gnuplotTerminal + ";");
		str.append("set output \"" + destFileName + gnuplotOutType + "\";");
		str.append("set xrange [" + params.getxMin() + ":" + params.getxMax() + "];");
		str.append("set yrange [" + params.getyMin() + ":" + params.getyMax() + "];");		
		str.append("set ylabel \"Number of Nodes\";");
		str.append("set xlabel \"Task Assignment(%)\";");
		str.append("plot ");
		int index = 1;
		Set<String> sizes = nldFiles.keySet();
		for (String size : sizes) {
			str.append("'data/" + nldFiles.get(size).getName() + "' using 1:(" + Integer.parseInt(size) + ") notitle pt 2");
			if (index < sizes.size())
				str.append(", ");
			else
				str.append(";");
			index++;
		}
		try {
			ProcessBuilder process = new ProcessBuilder("gnuplot", "-e", str.toString());
			process.directory(destPath);
			process.start();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void plotMapLocality(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";"	+ 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" +
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set y2range [" + params.getyMin2() + ":" + params.getyMax2() + "];" + 
				"set ytics nomirror;" + 
				"set y2tics;" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"set y2label \"Non-locality\";" + 
				"set key top left reverse Left;" + 
				"plot" +
				" 'data/" + sourceFileName + ".dat' using 1:14:15 axis x1y1 title \"Map Meantime(32MB)\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:52:53 axis x1y1 title \"Map Meantime(64MB)\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:90:91 axis x1y1 title \"Map Meantime(128MB)\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:16:17 axis x1y2 title \"Map Non-locality(32MB)\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:54:55 axis x1y2 title \"Map Non-locality(64MB)\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:92:93 axis x1y2 title \"Map Non-locality(64MB)\" w yerrorlines;";
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

	public static void plotMapCompletionTime(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" + 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" +
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"plot 'data/" + sourceFileName + ".dat' using 1:14:15 title \"Map Meantime(32MB)\" w yerrorlines," + 
				"'data/" + sourceFileName + ".dat' using 1:52:53 title \"Map Meantime(64MB)\" w yerrorlines," + 
				"'data/" + sourceFileName + ".dat' using 1:90:91 title \"Map Meantime(128MB)\" w yerrorlines;";
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

	public static void plotReduceTimeBars(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" + 
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"set style data histogram;"	+ 
				"set style histogram errorbars gap 2 lw 3;" + 
				"set style fill pattern border;" + 
				"set boxwidth 0.9;" + 
				"plot" +
				" 'data/" + sourceFileName + ".dat' u 28:29:xtic(1) title \"MapShuflle\"," + 
				" 'data/" + sourceFileName + ".dat' u 30:31:xtic(1) title \"Shuflle\"," + 
				" 'data/" + sourceFileName + ".dat' u 34:35:xtic(1) title \"Reduce\";";
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

	public static void plotReduceTimeLines(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";" + 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" +
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"plot" +
				" 'data/" + sourceFileName + ".dat' using 1:28 title \"MapShuffle\" w linesp," + 
				" 'data/" + sourceFileName + ".dat' using 1:30 title \"Shuffle\" w linesp," + 
				" 'data/" + sourceFileName + ".dat' using 1:34 title \"Reduce\" w linesp;";
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

	public static void plotMapReduceCompletionTime(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		String strCommands = "reset;" + 
				"set terminal " + gnuplotTerminal + ";" + 
				"set output \"" + destFileName + gnuplotOutType + "\";"	+ 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" +
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"plot " + 
				" 'data/" + sourceFileName + ".dat' using 1:14:15 title \"Map Meantime(32MB)\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:52:53 title \"Map Meantime(64MB)\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:90:91 title \"Map Meantime(128MB)\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:28:29 title \"Reduce Meantime(32MB)\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:66:67 title \"Reduce Meantime(64MB)\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:104:105 title \"Reduce Meantime(128MB)\" w yerrorlines;";
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
	
	public static void plotWaveLines(File destPath, String sourcePrefix, String destFileName, GraphParameters params) {
		String strCommands = "reset;"  + 
				"set terminal " + gnuplotTerminal + ";"  + 
				"set output \"waves/" + destFileName + gnuplotOutType + "\";" + 
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" + 
				"set ylabel \"Tasks\";" + "set xlabel \"Time(s)\";"  + 
				"plot 'data/"	+ sourcePrefix + "_Map.dat' using 2:1:3:(0) with vector nohead filled lt 6 title \"Map\","  + 
				"'data/" + sourcePrefix + "_MapNL.dat' using 2:1:3:(0) with vector nohead filled lt 5 title \"NLMap\","  + 
				"'data/" + sourcePrefix + "_Reduce.dat' using 2:1:3:(0) with vector nohead filled lt 2 title \"Shuffle\","  + 
				"'data/" + sourcePrefix + "_Reduce.dat' using ($2 + $3):1:4:(0) with vector nohead filled lt 3 title \"Sort\","  + 
				"'data/" + sourcePrefix + "_Reduce.dat' using ($2 + $3 + $4):1:5:(0) with vector nohead filled lt 1 title \"Reduce\";";
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

	public static void plotCDF(Rengine r_engine, Histogram times, String resultPath, String dataPath, String fileName) throws IOException {
		File resultDir = new File(dataPath);
		resultDir.mkdirs();

		LoggedDiscreteCDF cdf = new LoggedDiscreteCDF();
		int[] attemptTimesPercentiles = new int[99];
		for (int i = 0; i < 99; ++i) {
			attemptTimesPercentiles[i] = (i + 1) * 1;
		}

		FileWriter dataFile = new FileWriter(new File(dataPath + fileName + ".dat"));
		BufferedWriter writer = new BufferedWriter(dataFile);
		cdf.setCDF(times, attemptTimesPercentiles, 100);
		List<LoggedSingleRelativeRanking> lsrrList = cdf.getRankings();
		if (lsrrList != null && lsrrList.size() > 0) {
			for (LoggedSingleRelativeRanking lsrr : lsrrList) {
				String tmp = lsrr.getDatum() + " " + lsrr.getRelativeRanking();
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

		try {
			r_engine.eval("data <- read.table(\"" + dataPath + "/" + fileName + ".dat\", header=F, sep=\" \")");
			r_engine.eval("xs <- data$V1");
			r_engine.eval("xf <- data$V2");
			r_engine.eval("png(\"" + resultPath + "/" + fileName + ".png\")");
			r_engine.eval("plot(xs, xf, xlab=\"Runtime(ms)\", ylab=\"Cumulative frequency\")");
			r_engine.eval("dev.off()");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void plotMapNLECDF(Rengine r_engine, File resultPath, String dataPath, String fileName) throws IOException {
		try {
			r_engine.eval("setwd(\"" + resultPath + "/" + dataPath + "\")");
			r_engine.eval("data <- read.table(\"" + fileName + ".dat\")");			
			r_engine.eval("cdf <- ecdf(data$V1)");
			r_engine.eval("setwd(\"" + resultPath + "\")");
			r_engine.eval("png(\"" + fileName + ".png\")");
			r_engine.eval("plot(cdf, main=\"Map Non-Locality\", xlab=\"Task Assignment (%)\", ylab=\"CDF\", xlim=c(0, 1), ylim=c(0, 1))");
			r_engine.eval("dev.off()");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}