package org.apache.hadoop.analyzer;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedLocation;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.apache.hadoop.tools.rumen.Pre21JobHistoryConstants.Values;
import org.codehaus.jackson.map.ObjectMapper;

//TODO Quais jobs tiveram falhas das máquinas?
//TODO Jobs com execução em nó que não tem o dado ocorreram devido a falha nos nós que tinham o dado?
//TODO Qual o impacto no tempo de execução causado por 2% das execuções serem com dados não locais?

//TODO Agrupar os jobs por JobName, DataSize e ClusterSize, fixando JobName e DataSize e variando ClusterSize
//TODO Tempo de reduce muito elevado e se tornando um limitador quando há aumento de escala

public class JobExecutionAnalyzer {

	private static final String CountUpDriver = "CountUpDriver";
	private static final String CountUp = "CountUp";
	private static final String JxtaSocketPerfDriver = "JxtaSocketPerfDriver";

	private static final List<LoggedJob> cup32Min = new ArrayList<LoggedJob>();
	private static final List<LoggedJob> cup64Min = new ArrayList<LoggedJob>();
	private static final List<LoggedJob> cup128Min = new ArrayList<LoggedJob>();
	private static final List<LoggedJob> cup32Max = new ArrayList<LoggedJob>();
	private static final List<LoggedJob> cup64Max = new ArrayList<LoggedJob>();
	private static final List<LoggedJob> cup128Max = new ArrayList<LoggedJob>();

	private static final List<LoggedJob> cupd32Min = new ArrayList<LoggedJob>();
	private static final List<LoggedJob> cupd64Min = new ArrayList<LoggedJob>();
	private static final List<LoggedJob> cupd128Min = new ArrayList<LoggedJob>();
	private static final List<LoggedJob> cupd32Max = new ArrayList<LoggedJob>();
	private static final List<LoggedJob> cupd64Max = new ArrayList<LoggedJob>();
	private static final List<LoggedJob> cupd128Max = new ArrayList<LoggedJob>();

	//JxtaSocketPerfDriver jobs
	private static List<LoggedJob> jxta32Min = new ArrayList<LoggedJob>();
	private static List<LoggedJob> jxta64Min = new ArrayList<LoggedJob>();
	private static List<LoggedJob> jxta32Max = new ArrayList<LoggedJob>();
	private static List<LoggedJob> jxta64Max = new ArrayList<LoggedJob>();

	private static String fileName = "/home/thiago/tmp/experiment/05-job-trace.json";

	private static int mapOut = 0;
	private static int mapTasksOk = 0;

	private static float cupOut = 0;
	private static float cupdOut = 0;
	private static float jxtaOut = 0;

	private static float cupMaps = 0;
	private static float cupdMaps = 0;
	private static float jxtaMaps = 0;

	public static void main(String... args) throws Exception {
		ObjectMapper mapper = new ObjectMapper();		
		for (int i = 3; i <= 30; i++) {
			try{

				if(i > 9){
					fileName = "/home/thiago/tmp/experiment/" + i + "-job-trace.json";
				}else{
					fileName = "/home/thiago/tmp/experiment/0" + i + "-job-trace.json";
				}					

				Iterator<LoggedJob> jobs = mapper.reader(LoggedJob.class).readValues(new File(fileName));
				while (jobs.hasNext()) {
					decode(jobs.next());
				}
				System.out.print(i);
				evaluate(jxta32Min);
				evaluate(jxta64Min);
				System.out.println();
				jxta32Min = new ArrayList<LoggedJob>();
				jxta64Min = new ArrayList<LoggedJob>();
				jxta32Max = new ArrayList<LoggedJob>();
				jxta64Max = new ArrayList<LoggedJob>();
			}catch(Exception e){
				continue;
			}
		}

		// JXTA
		//		System.out.print("Jxta32min");
		//		evaluate(jxta32Min);
		//		System.out.print("\t");
		//		evaluate(jxta64Min);
		//		System.out.println();
		//		System.out.print("Jxta32max");
		//		evaluate(jxta32Max);		
		//		System.out.println();
		//		System.out.print("Jxta64max");
		//		evaluate(jxta64Max);

		//		System.out.print("CountUpDriver32min");
		//		evaluate(cupd32Min);
		//		System.out.println();
		//		System.out.print("CountUp32min");
		//		evaluate(cup32Min);
		//
		//		System.out.println();
		//		System.out.print("CountUpDriver64min");
		//		evaluate(cupd64Min);
		//		System.out.println();
		//		System.out.print("CountUp64min");
		//		evaluate(cup64Min);
		//
		//		System.out.println();
		//		System.out.print("CountUpDriver128min");
		//		evaluate(cupd128Min);
		//		System.out.println();
		//		System.out.print("CountUp128min");
		//		evaluate(cup128Min);		
		//
		//		System.out.println();
		//		System.out.print("CountUpDriver32max");
		//		evaluate(cupd32Max);
		//		System.out.println();
		//		System.out.print("CountUp32max");
		//		evaluate(cup32Max);	
		//
		//		System.out.println();
		//		System.out.print("CountUpDriver64max");
		//		evaluate(cupd64Max);
		//		System.out.println();
		//		System.out.print("CountUp64max");
		//		evaluate(cup64Max);
		//
		//		System.out.println();
		//		System.out.print("CountUpDriver128max");
		//		evaluate(cupd128Max);		
		//		System.out.println();
		//		System.out.print("CountUp128max");
		//		evaluate(cup128Max);


		//
		//		System.out.println("\n\n");
		//		System.out.println("### DataLocality ###");
		//		System.out.println("### JxtaOut: " + jxtaOut + "/" + jxtaMaps + " == " + ((jxtaOut/jxtaMaps)*100) + "%");
		//		System.out.println("### CountUpOut: " + cupOut + "/" + cupMaps + " == " + ((cupOut/cupMaps)*100) + "%");
		//		System.out.println("### CountUpDriverOut: " + cupdOut + "/" + cupdMaps + " == " + ((cupdOut/cupdMaps)*100) + "%");
		//		System.out.println();
		//		System.out.println("### TotalOut:" + mapOut);
		//		System.out.println("### TotalMapTasks:" + mapTasksOk);
	}

	private static void evaluate(List<LoggedJob> jobs){
		DescriptiveStatistics jobRuntimeStats = new DescriptiveStatistics();
		DescriptiveStatistics jobNotLocalAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics jobTotalAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics jobNumMapStats = new DescriptiveStatistics();

		DescriptiveStatistics mapTaskTimeStats = new DescriptiveStatistics();		
		DescriptiveStatistics mapAttemptStats = new DescriptiveStatistics();
		DescriptiveStatistics mapNotLocalAttemptStats = new DescriptiveStatistics();

		DescriptiveStatistics reduceTaskTimeStats = new DescriptiveStatistics();		
		DescriptiveStatistics reduceNumTasksStats = new DescriptiveStatistics();

		DescriptiveStatistics inputStats = new DescriptiveStatistics();
		DescriptiveStatistics shuffleStats = new DescriptiveStatistics();
		DescriptiveStatistics outputStats = new DescriptiveStatistics();

		for (LoggedJob job : jobs) {			
			double jobRuntime = (job.getFinishTime() - job.getLaunchTime());
			double jobNotLocalAttempt = 0;
			double jobTotalAttempt = 0;
			jobRuntimeStats.addValue(jobRuntime);
			
			// Map
			List<LoggedTask> mapTasks = job.getMapTasks();
			jobNumMapStats.addValue(mapTasks.size());
			if(mapTasks != null){
				for (LoggedTask mapTask : mapTasks) {
					double mapNotLocalAttempt = 0;
					double mapTime = (mapTask.getFinishTime() - mapTask.getStartTime());
					mapTaskTimeStats.addValue(mapTime);
					inputStats.addValue(mapTask.getInputBytes());

					List<LoggedLocation> dataLocations = mapTask.getPreferredLocations();
					List<LoggedTaskAttempt> attemptList = mapTask.getAttempts();
					if(attemptList != null){
						mapAttemptStats.addValue(attemptList.size());
						jobTotalAttempt += attemptList.size();

						for (LoggedTaskAttempt mapAttempt : attemptList) {
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
								// ??
							}
						}						
					}					
					mapNotLocalAttemptStats.addValue(mapNotLocalAttempt);
				}
			}
			
			// Reduce 
			List<LoggedTask> reduceTasks = job.getReduceTasks();
			reduceNumTasksStats.addValue(reduceTasks.size());
			if(reduceTasks != null){
				for (LoggedTask reduceTask : reduceTasks) {
					double reduceTime = (reduceTask.getFinishTime() - reduceTask.getStartTime());			
					reduceTaskTimeStats.addValue(reduceTime);
					outputStats.addValue(reduceTask.getOutputBytes());
					List<LoggedTaskAttempt> attemptList = reduceTask.getAttempts();
					if(attemptList != null){
						double shuffleBytes = 0;
						for (LoggedTaskAttempt reduceAttempt : attemptList) {
							shuffleBytes += reduceAttempt.getReduceShuffleBytes();
						}
						shuffleStats.addValue(shuffleBytes);
					}
				}
			}
			
			jobNotLocalAttemptStats.addValue(jobNotLocalAttempt);
			jobTotalAttemptStats.addValue(jobTotalAttempt);			
		}

		//		printStats(jobRuntimeStats, jobNotLocalAttemptStats, jobTotalAttemptStats,
		//				mapTaskTimeStats, mapNumTasksStats, mapAttemptStats,
		//				mapNotLocalAttemptStats,
		//				reduceTaskTimeStats, reduceNumTasksStats, inputStats,
		//				shuffleStats, outputStats);

		printInlineStats(jobRuntimeStats, jobNotLocalAttemptStats, jobTotalAttemptStats,
				mapTaskTimeStats, jobNumMapStats, mapAttemptStats,
				mapNotLocalAttemptStats,
				reduceTaskTimeStats, reduceNumTasksStats, inputStats,
				shuffleStats, outputStats);
	}

	@SuppressWarnings("unused")
	private static void printStats(DescriptiveStatistics jobRuntimeStats,
			DescriptiveStatistics jobNotLocalAttemptStats,
			DescriptiveStatistics jobTotalAttemptStats,
			DescriptiveStatistics mapTaskTimeStats,
			DescriptiveStatistics mapNumTasksStats,
			DescriptiveStatistics mapAttemptStats,
			DescriptiveStatistics mapNotLocalAttemptStats,
			DescriptiveStatistics reduceTaskTimeStats,
			DescriptiveStatistics reduceNumTasksStats,
			DescriptiveStatistics inputStats,
			DescriptiveStatistics shuffleStats,
			DescriptiveStatistics outputStats) {

		DecimalFormat df2 = new DecimalFormat( "#,###,###,##0.00" );

		System.out.println("### Job:\t" + df2.format(jobRuntimeStats.getMean()) + " \t " + df2.format(jobRuntimeStats.getPercentile(50)) + " \t " + df2.format(jobRuntimeStats.getStandardDeviation()));
		System.out.println("### JobNotLocalAttempts:\t" + df2.format(jobNotLocalAttemptStats.getMean()) + " \t " + df2.format(jobNotLocalAttemptStats.getPercentile(50)) + " \t " + df2.format(jobNotLocalAttemptStats.getStandardDeviation()));
		System.out.println("### JobAttempts:\t" + df2.format(jobTotalAttemptStats.getMean()) + " \t " + df2.format(jobTotalAttemptStats.getPercentile(50)) + " \t " + df2.format(jobTotalAttemptStats.getStandardDeviation()));
		System.out.println();
		System.out.println("### Input:\t" + df2.format(inputStats.getMean()) + " \t " + df2.format(inputStats.getPercentile(50)) + " \t " + df2.format(inputStats.getStandardDeviation()));
		System.out.println("### Shuffle:\t" + df2.format(shuffleStats.getMean()) + " \t " + df2.format(shuffleStats.getPercentile(50)) + " \t " + df2.format(shuffleStats.getStandardDeviation()));
		System.out.println("### Output:\t" + df2.format(outputStats.getMean()) + " \t " + df2.format(outputStats.getPercentile(50)) + " \t " + df2.format(outputStats.getStandardDeviation()));
		System.out.println();		
		System.out.println("### Map:\t" + df2.format(mapNumTasksStats.getMean()) + " \t " + df2.format(mapTaskTimeStats.getMean()) + " \t " + df2.format(mapTaskTimeStats.getPercentile(50)) + " \t " + df2.format(mapTaskTimeStats.getStandardDeviation()));
		System.out.println("### NotLocalAttemp:\t" + df2.format(mapNotLocalAttemptStats.getMean()) + " \t " + df2.format(mapNotLocalAttemptStats.getPercentile(50)) + " \t " + df2.format(mapNotLocalAttemptStats.getStandardDeviation()));
		System.out.println("### TotalAttemp:\t" + df2.format(mapAttemptStats.getMean()) + " \t " + df2.format(mapAttemptStats.getPercentile(50)) + " \t " + df2.format(mapAttemptStats.getStandardDeviation()));
		System.out.println();
		System.out.println("### Reduce:\t" + df2.format(reduceNumTasksStats.getMean()) + " \t " + df2.format(reduceTaskTimeStats.getMean()) + " \t " + df2.format(reduceTaskTimeStats.getPercentile(50)) + " \t " + df2.format(reduceTaskTimeStats.getStandardDeviation()));
		System.out.println();
		df2 = new DecimalFormat( "#,###,###,##0.00000000" );
		System.out.println("### Output/Input:\t" + df2.format(outputStats.getMean()/inputStats.getMean()) + " \t " + df2.format(outputStats.getPercentile(50)/inputStats.getPercentile(50)));
		System.out.println("### Shuffle/Input:\t" + df2.format(shuffleStats.getMean()/inputStats.getMean()) + " \t " + df2.format(shuffleStats.getPercentile(50)/inputStats.getPercentile(50)));
		System.out.println("### Output/Shuffle:\t" + df2.format(outputStats.getMean()/shuffleStats.getMean()) + " \t " + df2.format(outputStats.getPercentile(50)/shuffleStats.getPercentile(50)));
		df2 = new DecimalFormat( "#,###,###,##0.00" );
	}

	private static void printInlineStats(DescriptiveStatistics jobRuntimeStats,
			DescriptiveStatistics jobNotLocalAttemptStats,
			DescriptiveStatistics jobTotalAttemptStats,
			DescriptiveStatistics mapTaskTimeStats,
			DescriptiveStatistics mapNumTasksStats,
			DescriptiveStatistics mapAttemptStats,
			DescriptiveStatistics mapNotLocalAttemptStats,
			DescriptiveStatistics reduceTaskTimeStats,
			DescriptiveStatistics reduceNumTasksStats,
			DescriptiveStatistics inputStats,
			DescriptiveStatistics shuffleStats,
			DescriptiveStatistics outputStats) {

		DecimalFormat df2 = new DecimalFormat( "#,###,###,##0.00" );
		// JobRuntimeMean |JobRuntimeStdv | JobNotLocalAttemptsMean | JobNotLocalAttemptsStdv | JobAttemptsMean | JobAttemptsStdv | MapNumTasksMean | MapNumTasksStdv | MapTaskTimeMean | MapTaskTimeStdv | MapNotLocalAttempMean | MapNotLocalAttempStdv | MapTotalAttempMean | MapTotalAttempStdv | ReduceNumTasksMean | ReduceNumTasksStdv | ReduceTaskTimeMean | ReduceTaskTimeStdv | InputBytesMean | InputBytesStdv | ShuffleBytesMean | ShuffleBytesStdv | OutputBytesMean | OutputBytesStdv 
		System.out.print("\t" + df2.format(jobRuntimeStats.getMean()));
		System.out.print("\t" + df2.format(jobNotLocalAttemptStats.getMean()));
		System.out.print("\t" + df2.format(jobTotalAttemptStats.getMean()));
		System.out.print("\t" + df2.format(mapNumTasksStats.getMean()));
		System.out.print("\t" + df2.format(mapTaskTimeStats.getMean()));
		System.out.print("\t" + df2.format(mapNotLocalAttemptStats.getMean()));		
		System.out.print("\t" + df2.format(mapAttemptStats.getMean()));		
		System.out.print("\t" + df2.format(reduceNumTasksStats.getMean()));
		System.out.print("\t" + df2.format(reduceTaskTimeStats.getMean()));		
		System.out.print("\t" + df2.format(inputStats.getMean()));
		System.out.print("\t" + df2.format(shuffleStats.getMean()));
		System.out.print("\t" + df2.format(outputStats.getMean()));

		//		System.out.print("\t" + df2.format(jobRuntimeStats.getMean()) + "\t" + df2.format(jobRuntimeStats.getStandardDeviation()));
		//		System.out.print("\t" + df2.format(jobNotLocalAttemptStats.getMean()) + "\t" + df2.format(jobNotLocalAttemptStats.getStandardDeviation()));
		//		System.out.print("\t" + df2.format(jobTotalAttemptStats.getMean()) + "\t" + df2.format(jobTotalAttemptStats.getStandardDeviation()));
		//		System.out.print("\t" + df2.format(mapNumTasksStats.getMean()) + "\t" + df2.format(mapNumTasksStats.getStandardDeviation()));
		//		System.out.print("\t" + df2.format(mapTaskTimeStats.getMean()) + "\t" + df2.format(mapTaskTimeStats.getStandardDeviation()));
		//		System.out.print("\t" + df2.format(mapNotLocalAttemptStats.getMean()) + "\t" + df2.format(mapNotLocalAttemptStats.getStandardDeviation()));		
		//		System.out.print("\t" + df2.format(mapAttemptStats.getMean()) + "\t" + df2.format(mapAttemptStats.getStandardDeviation()));		
		//		System.out.print("\t" + df2.format(reduceNumTasksStats.getMean()) + "\t" + df2.format(reduceNumTasksStats.getStandardDeviation()));
		//		System.out.print("\t" + df2.format(reduceTaskTimeStats.getMean()) + "\t" + df2.format(reduceTaskTimeStats.getStandardDeviation()));		
		//		System.out.print("\t" + df2.format(inputStats.getMean()) + "\t" + df2.format(inputStats.getStandardDeviation()));
		//		System.out.print("\t" + df2.format(shuffleStats.getMean()) + "\t" + df2.format(shuffleStats.getStandardDeviation()));
		//		System.out.print("\t" + df2.format(outputStats.getMean()) + "\t" + df2.format(outputStats.getStandardDeviation()));
		//		df2 = new DecimalFormat( "#,###,###,##0.00000000" );
		//		System.out.print("Output/Input\t" + df2.format(outputStats.getMean()/inputStats.getMean()) + "\t" + df2.format(outputStats.getPercentile(50)/inputStats.getPercentile(50)));
		//		System.out.print("Shuffle/Input\t" + df2.format(shuffleStats.getMean()/inputStats.getMean()) + "\t" + df2.format(shuffleStats.getPercentile(50)/inputStats.getPercentile(50)));
		//		System.out.print("Output/Shuffle\t" + df2.format(outputStats.getMean()/shuffleStats.getMean()) + "\t" + df2.format(outputStats.getPercentile(50)/shuffleStats.getPercentile(50)));
	}

	private static void decode(LoggedJob job){

		if(job.getJobID().equals("job_201208161208_0001")){
			//			JobExecutionViewer.jobPrettyPrint(job);
		}

		if(job.getJobName().equals(JxtaSocketPerfDriver)){
			List<LoggedTask> mapTasks = job.getMapTasks();
			if(mapTasks != null){
				if(mapTasks.size() == 120){
					jxta32Min.add(job);
				}else if(mapTasks.size() == 60){
					jxta64Min.add(job);
				}else if(mapTasks.size() == 360){
					jxta32Max.add(job);
				}else if(mapTasks.size() == 180){
					jxta64Max.add(job);
				}					
			}
		}else
			if(job.getJobName().equals(CountUpDriver)){
				List<LoggedTask> mapTasks = job.getMapTasks();
				if(mapTasks != null){
					if(mapTasks.size() == 120){
						cupd32Min.add(job);
					}else if(mapTasks.size() == 60){
						cupd64Min.add(job);
					}else if(mapTasks.size() == 30){
						cupd128Min.add(job);
					}else if(mapTasks.size() == 360){
						cupd32Max.add(job);
					}else if(mapTasks.size() == 180){
						cupd64Max.add(job);
					}else if(mapTasks.size() == 90){
						cupd128Max.add(job);
					}					
				}
			}else
				if(job.getJobName().equals(CountUp)){
					List<LoggedTask> mapTasks = job.getMapTasks();
					if(mapTasks != null){
						if(mapTasks.size() == 120){
							cup32Min.add(job);
						}else if(mapTasks.size() == 60){
							cup64Min.add(job);
						}else if(mapTasks.size() == 30){
							cup128Min.add(job);
						}else if(mapTasks.size() == 360){
							cup32Max.add(job);
						}else if(mapTasks.size() == 180){
							cup64Max.add(job);
						}else if(mapTasks.size() == 90){
							cup128Max.add(job);
						}					
					}
				}

		// Data Locality
		List<LoggedTask> mapTasks = job.getMapTasks();
		if(mapTasks != null){
			for (LoggedTask mapTask : mapTasks) {
				List<LoggedLocation> dataLocations = mapTask.getPreferredLocations();
				List<LoggedTaskAttempt> attemptList = mapTask.getAttempts();
				if(attemptList != null){
					for (LoggedTaskAttempt mapAttempt : attemptList) {
						if(mapAttempt.getLocation() != null){
							boolean isLocal = false;
							for (LoggedLocation location : dataLocations) {
								isLocal = isLocal || location.getLayers().equals(mapAttempt.getLocation().getLayers());								
							} 
							if(!isLocal){
								//								System.out.println("### IsLocal: " + isLocal);
								//								System.out.println("### AttemptID: " + mapAttempt.getAttemptID());
								//								JobExecutionViewer.jobPrettyPrint(job);
								mapOut++;
								if(job.getJobName().equals(JxtaSocketPerfDriver)){
									jxtaOut++;
								}else
									if(job.getJobName().equals(CountUpDriver)){
										cupdOut++;
									}else
										if(job.getJobName().equals(CountUp)){
											cupOut++;
										}
							}
						}else{
							//							System.out.println("### Null AttemptDataLocation: " + mapAttempt.getAttemptID());
							//							JobExecutionViewer.jobPrettyPrint(job);
						}

						if(mapAttempt.getResult().equals(Values.SUCCESS)){
							mapTasksOk++;
							if(job.getJobName().equals(JxtaSocketPerfDriver)){
								jxtaMaps++;
							}else
								if(job.getJobName().equals(CountUpDriver)){
									cupdMaps++;
								}else
									if(job.getJobName().equals(CountUp)){
										cupMaps++;
									}
						}
					}
				}else{
					//					System.out.println("### Null MaptAttempts: " + mapTask.getTaskID());
					//					JobExecutionViewer.jobPrettyPrint(job);
				}
			}
		}else{
			//			System.out.println("### Null MaptTasks: " + job.getJobID());
			//			JobExecutionViewer.jobPrettyPrint(job);
		}
	}
}