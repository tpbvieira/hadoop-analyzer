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
//TODO Rever os Times de execucao das tarefas
//TODO analisar dados colhidos
//TODO rever os Times das fases
//TODO Os valores das ondas não estao sendo corretos, exemplo: desc_1.00, tem NL_Map terminando depois do reduce começar
//TODO Há uma certa distância entre algumas atividades de reduce. Acontece apenas para os casos sem MapShuffle?
//TODO por que o reduce apresenta tão pouco Time?
//TODO Plotar uma ECDF com a conclusão das tarefas, junto com a da nao-localidade

public class ExecutionAnalyzer {

	private static final String path = "/home/thiago/tmp/experiments/03.lab.20x/";

	private static final String tracePath = path + "traces/";
	private static final String resultPath = path + "results/";
	private static final String dataDir = "data/";

	private static Rengine r_engine;
	static {
		String[] args = new String[1];
		args[0] = "--save";
		r_engine = new Rengine(args, false, null);
	}

	private static final String gnuplotTerminal_std = "postscript eps enhanced";
	private static final String gnuplotOutType_std = ".eps";
	private static final String gnuplotTerminal_waves = "png";
	private static final String gnuplotOutType_waves = ".png";

	private static enum PrintMode {
		prettyPrint, inline, fullInline, disabled
	};

	public static enum GraphTypes {
		JobTimeBarComp, JobProcessingLines, JobSpeedupLines, JobSpeedupBarLines, JobReduceTimeLines, JobReduceTimeBars, 
		PhasesEvaluation, PhasesPercentEvaluation,
		MapLocality, MapNLScatter, MapNLAssignmentECDF, MapCompletionTime, MapReduceCompletionTime, 
		ReduceTimeLines, ReduceTimeBars, WaveLines, ECDF
	};
	
	public static enum Consts {
		block_32MB, block_64MB, block_128MB, dataset_90Gb, dataset_30Gb
	};

	public static void main(String... args) throws Exception {		
		String[] jobNames = { "JxtaSocketPerfDriver", "P3", "CountUpDriver" };
		String[][] groups = {{"32-360", "64-180", "128-90"},{"32-120","64-60","128-30"}};
		String[] groupNames = { "Max","Min" };

		Map<Consts,Integer> consts = new HashMap<Consts,Integer>();
		
		// jxtamax
//		consts.put(Consts.block_32MB,1745);
//		consts.put(Consts.block_64MB,1755);
//		consts.put(Consts.block_128MB,1765);	
//		consts.put(Consts.dataset_90Gb,92160);
		
		// jxtamin		
//		consts.put(Consts.block_32MB,584);
//		consts.put(Consts.block_64MB,587);
//		consts.put(Consts.block_128MB,606);
//		consts.put(Consts.dataset_30Gb,30720);
		
		// countupmax
//		consts.put(Consts.block_32MB,872);
//		consts.put(Consts.block_64MB,571);
//		consts.put(Consts.block_128MB,745);		
//		consts.put(Consts.dataset_90Gb,92160);

		// countupmin
		consts.put(Consts.block_32MB,86);
		consts.put(Consts.block_64MB,91);
		consts.put(Consts.block_128MB,94);		
		consts.put(Consts.dataset_30Gb,30720);
		
		Map<GraphTypes, GraphParameters> graphTypes = new HashMap<GraphTypes,GraphParameters>();		
		graphTypes.put(GraphTypes.JobProcessingLines, new GraphParameters(0, 0, 0, 0, 0, 0, consts));
		graphTypes.put(GraphTypes.JobSpeedupBarLines, new GraphParameters(0, 0, 0, 0, 0, 0, consts));
		graphTypes.put(GraphTypes.PhasesEvaluation, null);
		graphTypes.put(GraphTypes.PhasesPercentEvaluation, null);
		
		Map<Integer,String> labels = new HashMap<Integer,String>();
		labels.put(0,"CountUpDriver");
		labels.put(1,"P3");
		Map<Integer,String> sources = new HashMap<Integer,String>();
		sources.put(0,"CountUpDriverMax");
		sources.put(1,"P3Max");
		graphTypes.put(GraphTypes.JobTimeBarComp, new GraphParameters(0, 0, 0, 0, sources, labels));
				
//		labels = new HashMap<Integer,String>();
//		labels.put(0,"Assignment");
//		labels.put(1,"NL Assignment");
//		labels.put(2,"Done");
//		sources = new HashMap<Integer,String>();
//		sources.put(0,"P3MaxMapNLAssignmentECDF");
//		sources.put(1,"P3MaxMapDoneECDF");
//		sources.put(2,"P3MaxMapAssignmentECDF");
//		graphTypes.put(GraphTypes.MapNLAssignmentECDF, new GraphParameters(0, 0, 0, 0, sources, labels));
//		graphTypes.put(GraphTypes.MapNLScatter, new GraphParameters(0, 1, 0, 30));
//		graphTypes.put(GraphTypes.WaveLines, new GraphParameters(-1, 350, -1, 400));

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
			BufferedWriter mapAssignmentECDFWriter = new BufferedWriter(new FileWriter(dataPath + "/" + jobGroupName + "MapAssignmentECDF.dat"));
			BufferedWriter mapNLAssignmentECDFWriter = new BufferedWriter(new FileWriter(dataPath + "/" + jobGroupName + "MapNLAssignmentECDF.dat"));
			BufferedWriter mapDoneECDFWriter = new BufferedWriter(new FileWriter(dataPath + "/" + jobGroupName + "MapDoneECDF.dat"));						

			for (String clusterSize : clusterSizes) {

				System.out.print(clusterSize);
				fullWriter.write(String.valueOf(clusterSize));

				for (String jobKey : keyList) {
					SortedMap<String, List<LoggedJob>> jobs = jobGroup.get(jobKey);
					if (jobs.containsKey(clusterSize)) {
						String dataSourcePrefix = dataPath + "/" + jobKey + "_" + clusterSize;
						BufferedWriter mapLocalWavesWriter = new BufferedWriter(new FileWriter(dataSourcePrefix + "_Map.dat"));
						BufferedWriter mapNLWavesWriter = new BufferedWriter(new FileWriter(dataSourcePrefix + "_MapNL.dat"));
						BufferedWriter mapNLAssignmentDispersionWriter = new BufferedWriter(new FileWriter(dataSourcePrefix + "_MapNLDispersion.dat"));
						BufferedWriter reduceWavesWriter = new BufferedWriter(new FileWriter(dataSourcePrefix + "_Reduce.dat"));
						generateStatisticData(jobs.get(clusterSize), PrintMode.fullInline, fullWriter, mapLocalWavesWriter, mapNLWavesWriter,
								mapNLAssignmentDispersionWriter, reduceWavesWriter, attemptTimes.get(jobKey), Integer.parseInt(clusterSize), 
								mapNLAssignmentECDFWriter, mapDoneECDFWriter,mapAssignmentECDFWriter);
						mapLocalWavesWriter.close();
						mapNLWavesWriter.close();
						reduceWavesWriter.close();
					}
				}

				System.out.println();
				fullWriter.newLine();
			}

			fullWriter.close();
			mapNLAssignmentECDFWriter.close();
			mapDoneECDFWriter.close();
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
			BufferedWriter mapLocalWavesWriter, BufferedWriter mapNLWavesWriter, BufferedWriter mapNLAssignmentDispersionWriter, BufferedWriter reduceWavesWriter,
			Histogram[] histograms, Integer clusterSize, BufferedWriter mapNLAssignmentECDFWriter, BufferedWriter mapDoneECDFWriter, 
			BufferedWriter mapAssignmentECDFWriter) {
		
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

		List<Double> mapNLAssignmentDispersion = new ArrayList<Double>();
		List<Double> mapAssignmentDispersion = new ArrayList<Double>();
		List<Double> mapDoneDispersion = new ArrayList<Double>();

		for (LoggedJob job : jobs) {
			double jobTime = (job.getFinishTime() - job.getLaunchTime()) / 1000;

			if (jobTime > 0) {
				List<Long> mapNLAssignmentTimes = new ArrayList<Long>();
				List<Long> mapAssignmentTimes = new ArrayList<Long>();
				List<Long> mapDoneTimes = new ArrayList<Long>();
				jobRuntimeStats.addValue(jobTime);

				// Setup and Cleanup
				getJobStatistics(job.getOtherTasks(), jobSetupTimeStats, jobCleanupTimeStats);

				// Map
				PhaseTime mapPhaseTimes = getMapStatistics(job.getMapTasks(), mapNumStats, mapTimeStats, mapNLStats, mapOutInStats, mapSuccessAttemptStats,
						mapKilledAttemptStats, mapFailedAttemptStats, mapAttemptStats, histograms[0], mapLocalWaves, mapNLWaves, mapNLAssignmentTimes, 
						mapDoneTimes, mapAssignmentTimes);

				// Reduce
				getReduceStatistics(mapPhaseTimes.getMinFinishTime(), mapPhaseTimes.getMaxFinishTime(), job.getReduceTasks(), reduceNumStats,
						mapShuffleTimeStats, reduceShuffleTimeStats, reduceSortTimeStats, reduceTimeStats, reduceTaskTimeStats, reduceNotLocalAttemptStats,
						reduceSuccessAttemptStats, reduceKilledAttemptStats, reduceFailedAttemptStats, reduceAttemptStats, histograms[1], reduceWaves);

				// Map Assignment Dispersion
				for (Long mapAssignmentTime : mapAssignmentTimes) {
					double assignmentTime = mapAssignmentTime - job.getLaunchTime();
					double mapMaxFinishTime = mapPhaseTimes.getMaxFinishTime() - job.getLaunchTime();
					mapAssignmentDispersion.add(assignmentTime/mapMaxFinishTime);
				}
				
				// Not Local Map Dispersion
				for (Long mapNLStartTime : mapNLAssignmentTimes) {
					double nlStart = mapNLStartTime - job.getLaunchTime();
					double mapMaxFinishTime = mapPhaseTimes.getMaxFinishTime() - job.getLaunchTime();
					mapNLAssignmentDispersion.add(nlStart/mapMaxFinishTime);
				}
				
				//  MapDone Dispersion
				for (Long mapDoneTime : mapDoneTimes) {
					double mapDone = mapDoneTime - job.getLaunchTime();
					double mapMaxFinishTime = mapPhaseTimes.getMaxFinishTime() - job.getLaunchTime();
					mapDoneDispersion.add(mapDone/mapMaxFinishTime);
				}
			}
		}

		// Print Statistics
		if (printMode == PrintMode.fullInline) {
			serializeStats(jobRuntimeStats, jobSetupTimeStats, jobCleanupTimeStats, mapNumStats, mapTimeStats, mapShuffleTimeStats, mapNLStats,
					mapOutInStats, mapSuccessAttemptStats, mapKilledAttemptStats, mapFailedAttemptStats, mapAttemptStats, reduceNumStats,
					reduceTaskTimeStats, reduceShuffleTimeStats, reduceSortTimeStats, reduceTimeStats, reduceNotLocalAttemptStats, reduceSuccessAttemptStats,
					reduceKilledAttemptStats, reduceFailedAttemptStats, reduceAttemptStats, fullDataWriter, mapLocalWavesWriter, mapLocalWaves,
					mapNLWavesWriter, mapNLAssignmentDispersionWriter, mapNLWaves, reduceWavesWriter, reduceWaves, mapNLAssignmentDispersion, 
					mapNLAssignmentECDFWriter,	mapDoneDispersion, mapDoneECDFWriter, mapAssignmentDispersion, mapAssignmentECDFWriter);
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
				
			case JobTimeBarComp:
				if(jobGroupName.contains("CountUpDriver"))
					plotJobTimeBarComp(resultDir, jobGroupName + key.name(), graphTypes.get(key));
				break;

			case JobProcessingLines:
				plotProcessingLines(resultDir, sourceFileName, jobGroupName + key.name(), graphTypes.get(key));
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

			case MapNLAssignmentECDF:
				plotMapNLAssignmentECDF(r_engine, resultDir, resultDir.toString() + "/" + dataDir, jobGroupName + key.name(),graphTypes.get(key));
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

			case ECDF:
				plotECDFs(r_engine, attemptTimes, jobGroupName, resultPath, dataDir);
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

	private static void plotECDFs(Rengine rengine, SortedMap<String, Histogram[]> attemptTimes, String jobGroupName, String resultPath, String dataPath)
			throws IOException {

		Set<String> names = attemptTimes.keySet();
		for (String fileName : names) {

			String resultPathStr = resultPath + "/" + jobGroupName + "/";
			String dataPathStr = resultPath + "/" + jobGroupName + "/" + dataPath;
			Histogram[] histogram = attemptTimes.get(fileName);

			// Plots Map ECDF
			plotECDF(rengine, histogram[0], resultPathStr, dataPathStr, fileName + "_ECDFMap");

			// Plots Reduce ECDF
			plotECDF(rengine, histogram[1], resultPathStr, dataPathStr, fileName + "_ECDFReduce");
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
			List<Long> mapNLAssignmentTimes, List<Long> mapDoneTimes, List<Long> mapAssignmentTimes) {

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
				mapAssignmentTimes.add(mapTask.getStartTime());
				if (!isLocal) {
					nlMapTimes.add(mapTime);
					mapNLAssignmentTimes.add(mapTask.getStartTime());
				}
				mapDoneTimes.add(mapTask.getFinishTime());
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
			BufferedWriter mapNLWavesWriter, BufferedWriter mapNLAssignmentDispersionWriter, SortedMap<Integer, DescriptiveStatistics[]> mapNLWaves,
			BufferedWriter reduceWavesWriter, SortedMap<Integer, DescriptiveStatistics[]> reduceWaves, List<Double> mapNLAssignmentDispersion, 
			BufferedWriter mapNLAssignmentECDFWriter, List<Double> mapDoneDispersion, BufferedWriter mapDoneECDFWriter, 
			List<Double> mapAssignmentDispersion, BufferedWriter mapAssignmentECDFWriter) {

		StringBuilder output = new StringBuilder();
		DecimalFormat df2 = new DecimalFormat("#,###,###,##0.00");

		//TODO rever como está sendo contabilizado o Time das fases. Tem um artigo que diz como ele está considerando, podemos pensar e usar o mesmo
		//TODO diferenciar o Time médio da tarefa, do Time médio da fase, verificar se esta distição está sendo feita
		jobRuntimeStats = removeOutliers(jobRuntimeStats);
		mapTimeStats = removeOutliers(mapTimeStats);
		mapShuffleTimeStats = removeOutliers(mapShuffleTimeStats);
		reduceTaskTimeStats = removeOutliers(reduceTaskTimeStats);
		reduceShuffleTimeStats = removeOutliers(reduceShuffleTimeStats);
		reduceSortTimeStats = removeOutliers(reduceSortTimeStats);
		reduceTimeStats = removeOutliers(reduceTimeStats);

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

		if (mapAssignmentECDFWriter != null) {
			try {
				for (Double mapAssignmentTime : mapAssignmentDispersion) {									
					mapAssignmentECDFWriter.write(mapAssignmentTime.toString());
					mapAssignmentECDFWriter.newLine();
					mapAssignmentECDFWriter.flush();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (mapNLAssignmentDispersionWriter != null && mapNLAssignmentECDFWriter != null) {
			try {
				for (Double startTime : mapNLAssignmentDispersion) {
					mapNLAssignmentDispersionWriter.write(startTime.toString());
					mapNLAssignmentDispersionWriter.newLine();
					mapNLAssignmentDispersionWriter.flush();					
					mapNLAssignmentECDFWriter.write(startTime.toString());
					mapNLAssignmentECDFWriter.newLine();
					mapNLAssignmentECDFWriter.flush();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (mapDoneECDFWriter != null) {
			try {
				for (Double mapFinishTime : mapDoneDispersion) {									
					mapDoneECDFWriter.write(mapFinishTime.toString());
					mapDoneECDFWriter.newLine();
					mapDoneECDFWriter.flush();
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
	
	public static void plotProcessingLines(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		
		Map<Consts,Integer> consts = params.getConstants();
		Integer datasetSize = consts.containsKey(Consts.dataset_90Gb) ? consts.get(Consts.dataset_90Gb) : consts.get(Consts.dataset_30Gb);		
				
		String strCommands = "reset;" +				 
				"f(x) = " + datasetSize + " / x;" + 
				"g(x,y) = (" + datasetSize + " / x) / y;" + 				
				"plot " +
				"'data/" + sourceFileName + ".dat' u 1:( f($2) ) w lp lt 0  pt 5 t \"Throughput - 32MB\"," + 
				"'data/" + sourceFileName + ".dat' u 1:( f($46) ) w lp lt 5 pt 7 t \"Throughput - 64MB\"," +
				"'data/" + sourceFileName + ".dat' u 1:( f($90) ) w lp lt 0  pt 9 t \"Throughput - 128MB\"," +
				"'data/" + sourceFileName + ".dat' u 1:( g($2,$1) ) w lp lt 0  pt 4 axis x1y2 t \"Throughput/Nodes - 32MB\"," + 
				"'data/" + sourceFileName + ".dat' u 1:( g($46,$1) ) w lp lt 5 pt 6 axis x1y2 t \"Throughput/Nodes - 64MB\"," + 
				"'data/" + sourceFileName + ".dat' u 1:( g($90,$1) ) w lp lt 7 pt 8 axis x1y2 t \"Throughput/Nodes - 128MB\";" +				
				"YMAX=GPVAL_Y_MAX;" +
				"YMIN=GPVAL_Y_MIN;" +
				"Y2MAX=GPVAL_Y2_MAX;" +
				"Y2MIN=GPVAL_Y2_MIN;" +
				"XMAX=GPVAL_X_MAX;" +
				"XMIN=GPVAL_X_MIN;" +				
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";"	+
				"set yrange [YMIN-(YMAX-YMIN)*0.05:YMAX+(YMAX-YMIN)*0.3];" + 
				"set ylabel \"Throughput (Mbps)\";" +
				"set ytics nomirror;" + 
				"set y2range [Y2MIN-(Y2MAX-Y2MIN)*0.05:Y2MAX+(Y2MAX-Y2MIN)*0.05];" +				
				"set y2label \"Throughput/Nodes (Mbps)\";" +
				"set y2tics;" +
				"set xtics 2;" + 
				"set xlabel \"Nodes\";" +
				"set xrange [XMIN-(XMAX-XMIN)*0.01:XMAX+(XMAX-XMIN)*0.01];" +
				"set key top right;" +
				"plot " +
				"'data/" + sourceFileName + ".dat' u 1:( f($2) ) w lp lt 0  pt 5 t \"Throughput - 32MB\"," + 
				"'data/" + sourceFileName + ".dat' u 1:( f($46) ) w lp lt 5 pt 7 t \"Throughput - 64MB\"," +
				"'data/" + sourceFileName + ".dat' u 1:( f($90) ) w lp lt 0  pt 9 t \"Throughput - 128MB\"," +
				"'data/" + sourceFileName + ".dat' u 1:( g($2,$1) ) w lp lt 0  pt 4 axis x1y2 t \"Throughput/Nodes - 32MB\"," + 
				"'data/" + sourceFileName + ".dat' u 1:( g($46,$1) ) w lp lt 5 pt 6 axis x1y2 t \"Throughput/Nodes - 64MB\"," + 
				"'data/" + sourceFileName + ".dat' u 1:( g($90,$1) ) w lp lt 7 pt 8 axis x1y2 t \"Throughput/Nodes - 128MB\";";		
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
	
	public static void plotJobTimeBarComp(File destPath, String destFileName, GraphParameters parameters) {
		
		Map<Integer,String> sources = parameters.getSources();
		Map<Integer,String> labels = parameters.getLabels();
		
		String strCommands = "reset;" +				
				"set style data histogram;" + 
				"set style histogram errorbars gap 2 lw 3;" + 
				"set style fill pattern border;" +
				"plot " + 
				"'data/" + sources.get(0) + ".dat' u 2:3:xtic(1) lt 1 t \"" + labels.get(0) + " - 32MB\", " + 
				"'../" + sources.get(1) + "/data/" + sources.get(1) + ".dat' u 2:3 lt 1 t \"" + labels.get(1) + " - 32MB\", " + 
				"'data/" + sources.get(0) + ".dat' u 46:47:xtic(1) lt 1 t \"" + labels.get(0) + " - 64MB\"," + 
				"'../" + sources.get(1) + "/data/" + sources.get(1) + ".dat' u 46:47 lt 1 t \"" + labels.get(1) + " - 64MB\", " + 
				"'data/" + sources.get(0) + ".dat' u 90:91:xtic(1) lt 1 t \"" + labels.get(0) + " - 128MB\"," + 
				"'../" + sources.get(1) + "/data/" + sources.get(1) + ".dat' u 90:91 lt 1 t \"" + labels.get(1) + " - 128MB\";" +
				"YMAX=GPVAL_Y_MAX;" +
				
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";"	+ 
				"set yrange [0:YMAX];" +				
				"set ylabel \"Time(s)\";" +
				"set xlabel \"Nodes\";" +
				"set boxwidth 0.9;" + 
				"plot " + 
				"'data/" + sources.get(0) + ".dat' u 2:3:xtic(1) lt 1 t \"" + labels.get(0) + " - 32MB\", " + 
				"'../" + sources.get(1) + "/data/" + sources.get(1) + ".dat' u 2:3 lt 1 t \"" + labels.get(1) + " - 32MB\", " + 
				"'data/" + sources.get(0) + ".dat' u 46:47:xtic(1) lt 1 t \"" + labels.get(0) + " - 64MB\"," + 
				"'../" + sources.get(1) + "/data/" + sources.get(1) + ".dat' u 46:47 lt 1 t \"" + labels.get(1) + " - 64MB\", " + 
				"'data/" + sources.get(0) + ".dat' u 90:91:xtic(1) lt 1 t \"" + labels.get(0) + " - 128MB\"," + 
				"'../" + sources.get(1) + "/data/" + sources.get(1) + ".dat' u 90:91 lt 1 t \"" + labels.get(1) + " - 128MB\"";
		try {
			ProcessBuilder process = new ProcessBuilder("gnuplot", "-e", strCommands);
			process.directory(destPath);
			process.start();
			System.out.println(strCommands);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void plotJobSpeedupLines(File destPath, String sourceFileName, String destFileName, GraphParameters params) {
		
		Map<Consts,Integer> consts = params.getConstants();
		
		String strCommands = "reset;" + 
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";"	+ 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" +
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" +
				"set y2range [" + params.getyMin2() + ":" + params.getyMax2() + "];" + "set y2tics;" +
				"set xlabel \"Nodes\";" + 
				"set ytics nomirror;" + 
				"set ylabel \"Runtime(s)\";" +				 
				"set y2label \"Speedup\";" +				
				"set key top right;" + 
				"f(x) = " + consts.get(Consts.block_32MB) + " / x;" + 
				"g(x) = " + consts.get(Consts.block_64MB) + " / x;" + 
				"h(x) = " + consts.get(Consts.block_128MB) + " / x;" + 
				"plot 'data/" + sourceFileName + 
				".dat' using 1:2:3 axis x1y1 title \"Completion Time - 32MB\" w yerrorlines, " + "'data/" + sourceFileName + 
				".dat' using 1:46:47 axis x1y1 title \"Completion Time - 64MB\" w yerrorlines, " + "'data/" + sourceFileName + 
				".dat' using 1:90:91 axis x1y1 title \"Completion Time - 128MB\" w yerrorlines," + "'data/" + sourceFileName + 
				".dat' using 1:( f($2) ) w lp lt 5 pt 5 axis x1y2 title \"Speedup - 32MB\", " + "'data/" + sourceFileName + 
				".dat' using 1:( g($46) ) w lp lt 7 pt 7 axis x1y2 title \"Speedup - 64MB\", " + "'data/" + sourceFileName + 
				".dat' using 1:( h($90) ) w lp lt 12 pt 9 axis x1y2 title \"Speedup - 128MB\";";
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
		
		Map<Consts,Integer> consts = params.getConstants();
		
		String strCommands = "reset;" + 						
				"f(x) = " + consts.get(Consts.block_32MB) + " / x;" + 
				"g(x) = " + consts.get(Consts.block_64MB) + " / x;" + 
				"h(x) = " + consts.get(Consts.block_128MB) + " / x;" +
				"set style data histogram;" + 
				"set style histogram errorbars gap 1.5 lw 3;" + 
				"set style fill pattern border;" +
				"plot " + 
				"'data/" + sourceFileName + ".dat' u 2:3:xtic(1) lt 1 axis x1y1 t \"Completion Time - 32MB\"," + 
				"'data/" + sourceFileName + ".dat' u 46:47:xtic(1) lt 1 axis x1y1 t \"Completion Time - 64MB\"," + 
				"'data/" + sourceFileName + ".dat' u 90:91:xtic(1) lt 1 axis x1y1 t \"Completion Time - 128MB\"," + 
				"'data/" + sourceFileName + ".dat' u ( f($2) ):xtic(1) w lp lt 0  pt 5 axis x1y2 t \"Speed-up - 32MB\"," + 
				"'data/" + sourceFileName + ".dat' u ( g($46) ):xtic(1) w lp lt 5 pt 7 axis x1y2 t \"Speed-up - 64MB\"," + 
				"'data/" + sourceFileName + ".dat' u ( h($90) ):xtic(1) w lp lt 7 pt 9 axis x1y2 t \"Speed-up - 128MB\";" +
				
				"YMAX=GPVAL_Y_MAX;" +
				"Y2MAX=GPVAL_Y2_MAX;" +
				"Y2MIN=GPVAL_Y2_MIN;" +
				 
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";"	+ 
				"set key top right;" + 
				"set ytics nomirror;" + 
				"set ylabel \"Time(s)\";" + 
				"set yrange [0:YMAX];" + 
				"set y2tics;" + 
				"set y2label \"Speed-up\";" + 
				"set y2range [Y2MIN-(Y2MAX-Y2MIN)*0.05:Y2MAX+(Y2MAX-Y2MIN)*0.35];" +				
				"set xlabel \"Nodes\";" + 
				"plot " + 
				"'data/" + sourceFileName + ".dat' u 2:3:xtic(1) lt 1 axis x1y1 t \"Completion Time - 32MB\"," + 
				"'data/" + sourceFileName + ".dat' u 46:47:xtic(1) lt 1 axis x1y1 t \"Completion Time - 64MB\"," + 
				"'data/" + sourceFileName + ".dat' u 90:91:xtic(1) lt 1 axis x1y1 t \"Completion Time - 128MB\"," + 
				"'data/" + sourceFileName + ".dat' u ( f($2) ):xtic(1) w lp lt 0  pt 5 axis x1y2 t \"Speed-up - 32MB\"," + 
				"'data/" + sourceFileName + ".dat' u ( g($46) ):xtic(1) w lp lt 5 pt 7 axis x1y2 t \"Speed-up - 64MB\"," + 
				"'data/" + sourceFileName + ".dat' u ( h($90) ):xtic(1) w lp lt 7 pt 9 axis x1y2 t \"Speed-up - 128MB\";";
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
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";" + 
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
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";" + 
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
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std	+ "\";"	+ 
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
//				"set yrange [0:500] noreverse nowriteback;" +
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
		String strCommands = "reset;" + 
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";" + 
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
				"set ylabel \"% of Completion Time\";" + 
				"set yrange [0:100] noreverse nowriteback;" + 
				"perc(x,y) = (x/y)*100;" + 
				"percRemain(a,b,c,d,e,f,g,h) = ((a - b - c - d - e - f - g - h)/a)*100;" + 
				"plot " + 
				"newhistogram \"32MB\" fs pattern 1, 'data/" + sourceFileName +	".dat' " + 
				"u ( perc($10 - $28, $2) ):xtic(1) t \"Map\" lt 1, '' u ( perc($28, $2) ) t \"Map e Shuffle\" lt 1, '' u ( perc($30, $2) ) t \"Shuffle\" lt 1, " + 
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
		str.append(getTerminalCode());
		str.append("set output \"" + destFileName + gnuplotOutType_std + "\";");
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
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";"	+ 
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
				" 'data/" + sourceFileName + ".dat' using 1:14:15 axis x1y1 title \"Map Meantime - 32MB\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:52:53 axis x1y1 title \"Map Meantime - 64MB\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:90:91 axis x1y1 title \"Map Meantime - 128MB\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:16:17 axis x1y2 title \"Map Non-locality - 32MB\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:54:55 axis x1y2 title \"Map Non-locality - 64MB\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:92:93 axis x1y2 title \"Map Non-locality - 64MB\" w yerrorlines;";
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
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";" + 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" +
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"plot 'data/" + sourceFileName + ".dat' using 1:14:15 title \"Map Meantime - 32MB\" w yerrorlines," + 
				"'data/" + sourceFileName + ".dat' using 1:52:53 title \"Map Meantime - 64MB\" w yerrorlines," + 
				"'data/" + sourceFileName + ".dat' using 1:90:91 title \"Map Meantime - 128MB\" w yerrorlines;";
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
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";" + 
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
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";" + 
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
				getTerminalCode() + 
				"set output \"" + destFileName + gnuplotOutType_std + "\";"	+ 
				"set xrange [" + params.getxMin() + ":" + params.getxMax() + "];" +
				"set yrange [" + params.getyMin() + ":" + params.getyMax() + "];" + 
				"set ylabel \"Runtime(s)\";" + 
				"set xlabel \"Nodes\";" + 
				"plot " + 
				" 'data/" + sourceFileName + ".dat' using 1:14:15 title \"Map Meantime - 32MB\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:52:53 title \"Map Meantime - 64MB\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:90:91 title \"Map Meantime - 128MB\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:28:29 title \"Reduce Meantime - 32MB\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:66:67 title \"Reduce Meantime - 64MB\" w yerrorlines," + 
				" 'data/" + sourceFileName + ".dat' using 1:104:105 title \"Reduce Meantime - 128MB\" w yerrorlines;";
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
				"set terminal " + gnuplotTerminal_waves + ";"  + 
				"set output \"waves/" + destFileName + gnuplotOutType_waves + "\";" + 
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

	public static void plotECDF(Rengine r_engine, Histogram times, String resultPath, String dataPath, String fileName) throws IOException {
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

	public static void plotMapNLAssignmentECDF(Rengine r_engine, File resultPath, String dataPath, String destFileName, GraphParameters params) throws IOException {
		
		Map<Integer,String> sourcesMap = params.getSources();
		Set<Integer> sourcesIds = sourcesMap.keySet();
		
//		data1 <- read.table("JxtaSocketPerfDriverMaxMapAssignmentECDF.dat")
//		data2 <- read.table("JxtaSocketPerfDriverMaxMapDoneECDF.dat")
//		data3 <- read.table("JxtaSocketPerfDriverMaxMapNLAssignmentECDF.dat")
//		cdf1 <- ecdf(data1$V1)
//		cdf2 <- ecdf(data2$V1)
//		cdf3 <- ecdf(data3$V1)
//		plot(cdf1, verticals=TRUE, do.p=FALSE,	main="ECDFs", xlab="Scores", ylab="Cumulative Percent",lty="dashed")
//		lines(cdf2, verticals=TRUE, do.p=FALSE, col.h="blue", col.v="blue",lty="dotted")
//		lines(cdf3, verticals=TRUE, do.p=FALSE, col.h="red", col.v="red",lty="dotted")
		
//		r_engine.eval
		
		int i = 1;
		System.out.println("setwd(\"" + dataPath + "\")");		
		for (Integer id : sourcesIds) {
			System.out.println("data <- read.table(\"" + sourcesMap.get(id) + ".dat\")");
			System.out.println("cdf" + i + " <- ecdf(data$V1)");	
			i++;
		}
		System.out.println("setwd(\"" + resultPath + "\")");				
		System.out.println("png(\"" + destFileName + ".png\")");
		System.out.println("plot(cdf1, main=\"Map Tasks\", xlab=\"Total Map Phase Duration\", ylab=\"ECDF\", xlim=c(0, 1), ylim=c(0, 1), ,lty=\"dashed\")");
		for (int j = 2; j < i; j++) {
			System.out.println("lines(ecdf(cdf" + j + "), col.h=\"red\", col.v=\"red\",lty=\"dotted\")");
		}
		System.out.println("dev.off()");		
	}
	
	public static String getTerminalCode(){
		String strCommands = "set terminal " + gnuplotTerminal_std + ";" + "set encoding utf8;";
		return strCommands;			
	}
	
	public static void evaluateEc2(){
		
		DecimalFormat df2 = new DecimalFormat("#,###,###,##0.00");
		DescriptiveStatistics jobRuntimeStats = new DescriptiveStatistics();
		jobRuntimeStats.addValue(333);
		jobRuntimeStats.addValue(319);
		jobRuntimeStats.addValue(318);
		jobRuntimeStats.addValue(319);
		jobRuntimeStats.addValue(327);
		jobRuntimeStats.addValue(321);
		jobRuntimeStats.addValue(331);
		jobRuntimeStats.addValue(322);
		jobRuntimeStats.addValue(313);
		jobRuntimeStats.addValue(312);
		jobRuntimeStats.addValue(331);
		jobRuntimeStats.addValue(321);
		jobRuntimeStats.addValue(321);
		jobRuntimeStats.addValue(322);
		jobRuntimeStats.addValue(322);
		jobRuntimeStats.addValue(322);
		jobRuntimeStats.addValue(330);
		jobRuntimeStats.addValue(324);
		jobRuntimeStats.addValue(327);
		jobRuntimeStats.addValue(323);
		jobRuntimeStats.addValue(321);
		jobRuntimeStats.addValue(325);
		jobRuntimeStats.addValue(323);
		jobRuntimeStats.addValue(321);
		jobRuntimeStats.addValue(318);
		jobRuntimeStats.addValue(320);
		jobRuntimeStats.addValue(322);
		jobRuntimeStats.addValue(317);
		jobRuntimeStats.addValue(324);
		jobRuntimeStats.addValue(327);
		System.out.println(df2.format(jobRuntimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(jobRuntimeStats)));

		df2 = new DecimalFormat("#,###,###,##0.00");
		jobRuntimeStats = new DescriptiveStatistics();
		jobRuntimeStats.addValue(262);
		jobRuntimeStats.addValue(244);
		jobRuntimeStats.addValue(237);
		jobRuntimeStats.addValue(244);
		jobRuntimeStats.addValue(245);
		jobRuntimeStats.addValue(245);
		jobRuntimeStats.addValue(246);
		jobRuntimeStats.addValue(250);
		jobRuntimeStats.addValue(246);
		jobRuntimeStats.addValue(247);
		jobRuntimeStats.addValue(243);
		jobRuntimeStats.addValue(241);
		jobRuntimeStats.addValue(243);
		jobRuntimeStats.addValue(246);
		jobRuntimeStats.addValue(250);
		jobRuntimeStats.addValue(247);
		jobRuntimeStats.addValue(246);
		jobRuntimeStats.addValue(241);
		jobRuntimeStats.addValue(243);
		jobRuntimeStats.addValue(247);
		jobRuntimeStats.addValue(249);
		jobRuntimeStats.addValue(247);
		jobRuntimeStats.addValue(246);
		jobRuntimeStats.addValue(245);
		jobRuntimeStats.addValue(255);
		jobRuntimeStats.addValue(243);
		jobRuntimeStats.addValue(243);
		jobRuntimeStats.addValue(250);
		jobRuntimeStats.addValue(241);
		jobRuntimeStats.addValue(249);
		System.out.println(df2.format(jobRuntimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(jobRuntimeStats)));

		df2 = new DecimalFormat("#,###,###,##0.00");
		jobRuntimeStats = new DescriptiveStatistics();
		jobRuntimeStats.addValue(178);
		jobRuntimeStats.addValue(172);
		jobRuntimeStats.addValue(171);
		jobRuntimeStats.addValue(172);
		jobRuntimeStats.addValue(177);
		jobRuntimeStats.addValue(172);
		jobRuntimeStats.addValue(171);
		jobRuntimeStats.addValue(175);
		jobRuntimeStats.addValue(172);
		jobRuntimeStats.addValue(172);
		jobRuntimeStats.addValue(171);
		jobRuntimeStats.addValue(175);
		jobRuntimeStats.addValue(171);
		jobRuntimeStats.addValue(178);
		jobRuntimeStats.addValue(171);
		jobRuntimeStats.addValue(172);
		jobRuntimeStats.addValue(170);
		jobRuntimeStats.addValue(170);
		jobRuntimeStats.addValue(176);
		jobRuntimeStats.addValue(171);
		jobRuntimeStats.addValue(171);
		jobRuntimeStats.addValue(176);
		jobRuntimeStats.addValue(171);
		jobRuntimeStats.addValue(175);
		jobRuntimeStats.addValue(174);
		jobRuntimeStats.addValue(175);
		jobRuntimeStats.addValue(175);
		jobRuntimeStats.addValue(175);
		jobRuntimeStats.addValue(168);
		jobRuntimeStats.addValue(178);
		System.out.println(df2.format(jobRuntimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(jobRuntimeStats)));

		df2 = new DecimalFormat("#,###,###,##0.00");
		jobRuntimeStats = new DescriptiveStatistics();
		jobRuntimeStats.addValue(169);
		jobRuntimeStats.addValue(147);
		jobRuntimeStats.addValue(147);
		jobRuntimeStats.addValue(154);
		jobRuntimeStats.addValue(163);
		jobRuntimeStats.addValue(147);
		jobRuntimeStats.addValue(145);
		jobRuntimeStats.addValue(159);
		jobRuntimeStats.addValue(144);
		jobRuntimeStats.addValue(145);
		jobRuntimeStats.addValue(150);
		jobRuntimeStats.addValue(150);
		jobRuntimeStats.addValue(151);
		jobRuntimeStats.addValue(151);
		jobRuntimeStats.addValue(144);
		jobRuntimeStats.addValue(151);
		jobRuntimeStats.addValue(150);
		jobRuntimeStats.addValue(151);
		jobRuntimeStats.addValue(144);
		jobRuntimeStats.addValue(151);
		jobRuntimeStats.addValue(150);
		jobRuntimeStats.addValue(151);
		jobRuntimeStats.addValue(151);
		jobRuntimeStats.addValue(150);
		jobRuntimeStats.addValue(148);
		jobRuntimeStats.addValue(145);
		jobRuntimeStats.addValue(159);
		jobRuntimeStats.addValue(163);
		jobRuntimeStats.addValue(162);
		jobRuntimeStats.addValue(160);
		System.out.println(df2.format(jobRuntimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(jobRuntimeStats)));

		df2 = new DecimalFormat("#,###,###,##0.00");
		jobRuntimeStats = new DescriptiveStatistics();
		jobRuntimeStats.addValue(128);
		jobRuntimeStats.addValue(130);
		jobRuntimeStats.addValue(129);
		jobRuntimeStats.addValue(126);
		jobRuntimeStats.addValue(130);
		jobRuntimeStats.addValue(126);
		jobRuntimeStats.addValue(127);
		jobRuntimeStats.addValue(126);
		jobRuntimeStats.addValue(132);
		jobRuntimeStats.addValue(125);
		jobRuntimeStats.addValue(126);
		jobRuntimeStats.addValue(128);
		jobRuntimeStats.addValue(128);
		jobRuntimeStats.addValue(125);
		jobRuntimeStats.addValue(125);
		jobRuntimeStats.addValue(144);
		jobRuntimeStats.addValue(130);
		jobRuntimeStats.addValue(123);
		jobRuntimeStats.addValue(127);
		jobRuntimeStats.addValue(127);
		jobRuntimeStats.addValue(126);
		jobRuntimeStats.addValue(126);
		jobRuntimeStats.addValue(121);
		jobRuntimeStats.addValue(126);
		jobRuntimeStats.addValue(125);
		jobRuntimeStats.addValue(125);
		jobRuntimeStats.addValue(125);
		jobRuntimeStats.addValue(123);
		jobRuntimeStats.addValue(129);
		jobRuntimeStats.addValue(127);	
		System.out.println(df2.format(jobRuntimeStats.getMean()) + "\t" + df2.format(getConfidenceInterval(jobRuntimeStats)));
		
	}

}