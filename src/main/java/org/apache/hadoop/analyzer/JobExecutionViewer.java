package org.apache.hadoop.analyzer;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.tools.rumen.LoggedDiscreteCDF;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedLocation;
import org.apache.hadoop.tools.rumen.LoggedSingleRelativeRanking;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.codehaus.jackson.map.ObjectMapper;

public class JobExecutionViewer {

	private static final String fileName = "/home/thiago/tmp/experiment/2nd_experiment/20x/traces/28-job-trace.json";

	public static void main(String... args) throws Exception {
		ObjectMapper mapper = new ObjectMapper();		
		Iterator<LoggedJob> jobs = mapper.reader(LoggedJob.class).readValues(new File(fileName));
		while (jobs.hasNext()) {
			jobPrettyPrint(jobs.next());
			System.out.println();
		}
	}

	public static void jobPrettyPrint(LoggedJob job){
		System.out.println("### Name: " + job.getJobName());
		System.out.println("### Id: " + job.getJobID());
		System.out.println("### Type: " + job.getJobtype());
		System.out.println("### Priority: " + job.getPriority());
		System.out.println("### Queue: " + job.getQueue());
		System.out.println("### Outcome: " + job.getOutcome());

		System.out.println("\n### ComputonsPerMapInputByte: " + job.getComputonsPerMapInputByte());
		System.out.println("### ComputonsPerMapOutputByte: " + job.getComputonsPerMapOutputByte());
		System.out.println("### ComputonsPerReduceInputByte: " + job.getComputonsPerReduceInputByte());
		System.out.println("### ComputonsPerReduceOutputByte: " + job.getComputonsPerReduceOutputByte());

		System.out.println("\n### TotalMaps: " + job.getTotalMaps());
		System.out.println("### TotalReduces: " + job.getTotalReduces());		
		System.out.println("### FailedMapperFraction: " + job.getFailedMapperFraction());

		System.out.println("\n### ClusterMapMB: " + job.getClusterMapMB());
		System.out.println("### ClusterReduceMB: " + job.getClusterReduceMB());
		System.out.println("### HeapMB: " + job.getHeapMegabytes());
		System.out.println("### JobMapMB: " + job.getJobMapMB());
		System.out.println("### JobReduceMB: " + job.getJobReduceMB());

		System.out.println("\n### StartupTime: " + (job.getLaunchTime() - job.getSubmitTime()));
		System.out.println("### RunTime: " + (job.getFinishTime() - job.getLaunchTime()));
		System.out.println("### SubmitTime: " + job.getSubmitTime());
		System.out.println("### StartTime: " + job.getLaunchTime());
		System.out.println("### FinishTime: " + job.getFinishTime());
		System.out.println("### RelativeTime: " + job.getRelativeTime());

		System.out.println("\n### DependantJobs:");
		List<String> directDependantJobs = job.getDirectDependantJobs();
		if(directDependantJobs != null){
			for (String string : directDependantJobs) {
				System.out.println(string);
			}
		}

		System.out.println("\n### TriesToSucceed:");
		double[] mapperTries = job.getMapperTriesToSucceed();
		if(mapperTries != null){
			for (double attempt : mapperTries) {
				System.out.println(attempt);
			}
		}

		System.out.println("\n### MapTasks:");
		List<LoggedTask> mapTasks = job.getMapTasks();
		if(mapTasks != null){
			for (LoggedTask mapTask : mapTasks) {				
				printTaskInfo(mapTask);
				System.out.println(" ## Attempts:");			
				List<LoggedTaskAttempt> attemptList = mapTask.getAttempts();
				if(attemptList != null){
					for (LoggedTaskAttempt mapAttempt : attemptList) {
						printAttempt(mapAttempt);
					}
				}
				System.out.println("---");
			}			
		}

		System.out.println("\n### ReduceTasks:");
		List<LoggedTask> reduceTasks = job.getReduceTasks();
		if(reduceTasks != null){
			for (LoggedTask reduceTask : reduceTasks) {
				printTaskInfo(reduceTask);
				System.out.println(" ## Attempts:");			
				List<LoggedTaskAttempt> attemptList = reduceTask.getAttempts();
				if(attemptList != null){
					for (LoggedTaskAttempt reduceAttempt : attemptList) {
						printAttempt(reduceAttempt);
					}
				}
				System.out.println("---");
			}
		}

		System.out.println("\n### OtherTasks:");
		List<LoggedTask> otherTasks = job.getOtherTasks();
		if(otherTasks != null){
			for (LoggedTask otherTask : otherTasks) {
				printTaskInfo(otherTask);
				System.out.println(" ## Attempts:");			
				List<LoggedTaskAttempt> attemptList = otherTask.getAttempts();
				if(attemptList != null){
					for (LoggedTaskAttempt otherAttempt : attemptList) {
						printAttempt(otherAttempt);
					}
				}
				System.out.println("---");
			}
		}

		System.out.println("\n### SuccessfulMapAttempt:");
		ArrayList<LoggedDiscreteCDF> successfulMapAttempt = job.getSuccessfulMapAttemptCDFs();
		printAttemptCDF(successfulMapAttempt);

		System.out.println("\n### SuccessfulReduceAttempt:");
		LoggedDiscreteCDF successfulReduceAttempt = job.getSuccessfulReduceAttemptCDF();
		ArrayList<LoggedDiscreteCDF> successfulReduceAttemptList = new ArrayList<LoggedDiscreteCDF>();
		successfulReduceAttemptList.add(successfulReduceAttempt);
		printAttemptCDF(successfulReduceAttemptList);

		System.out.println("\n### FailedMapAttempt:");
		ArrayList<LoggedDiscreteCDF> failedMapAttempt = job.getFailedMapAttemptCDFs();
		printAttemptCDF(failedMapAttempt);

		System.out.println("\n### FailedReduceAttempt:");
		LoggedDiscreteCDF failedReduceAttempt = job.getFailedReduceAttemptCDF();		
		ArrayList<LoggedDiscreteCDF> failedReduceAttemptList = new ArrayList<LoggedDiscreteCDF>();
		failedReduceAttemptList.add(failedReduceAttempt);
		printAttemptCDF(failedReduceAttemptList);	
	}

	private static void printTaskInfo(LoggedTask otherTask) {
		System.out.println("ID: " + otherTask.getTaskID());			
		System.out.println("Type: " + otherTask.getTaskType());
		System.out.println("Status: " + otherTask.getTaskStatus());
		System.out.println("RunTime: " + (otherTask.getFinishTime() - otherTask.getStartTime()));
		System.out.println("StartTime: " + otherTask.getStartTime());
		System.out.println("FinishTime: " + otherTask.getFinishTime());
		System.out.println("InputBytes: " + otherTask.getInputBytes());
		System.out.println("InputRecords: " + otherTask.getInputRecords());
		System.out.println("OutputBytes: " + otherTask.getOutputBytes());
		System.out.println("OutputRecords: " + otherTask.getOutputRecords());
		List<LoggedLocation> llList = otherTask.getPreferredLocations();
		if(llList != null){
			System.out.print("DataLocations:");
			for (LoggedLocation loggedLocation : llList) {
				System.out.print(" " + loggedLocation.getLayers());
			}
			System.out.println("");
		}
	}

	private static void printAttemptCDF(
			ArrayList<LoggedDiscreteCDF> successfulMapAttempt) {
		List<LoggedSingleRelativeRanking> lsrrList;
		if(successfulMapAttempt != null){
			for (LoggedDiscreteCDF ldCDF : successfulMapAttempt) {				
				lsrrList = ldCDF.getRankings();
				if(lsrrList != null && lsrrList.size() > 0){
					System.out.println("Max: " + ldCDF.getMaximum());
					System.out.println("Min: " + ldCDF.getMinimum());
					System.out.println("[Datum,RelRanking]");
					for (LoggedSingleRelativeRanking lsrr : lsrrList) {
						System.out.println(lsrr.getDatum() + " " + lsrr.getRelativeRanking());
					}
				}
			}
		}
	}

	private static void printAttempt(LoggedTaskAttempt attempt) {
		System.out.println(" ID: " + attempt.getAttemptID());
		System.out.println(" HostName: " + attempt.getHostName());
		System.out.println(" Result: " + attempt.getResult());
		System.out.println(" RunTime: " + (attempt.getFinishTime() - attempt.getStartTime()));
		System.out.println(" StartTime: " + attempt.getStartTime());
		System.out.println(" FinishTime: " + attempt.getFinishTime());
		System.out.print(" ExecutionLocation: ");
		if(attempt.getLocation() != null)
			System.out.println(attempt.getLocation().getLayers());
		else
			System.out.println();
		System.out.println(" HdfsBytesRead: " + attempt.getHdfsBytesRead());
		System.out.println(" FileBytesRead: " + attempt.getFileBytesRead());
		System.out.println(" HdfsBytesWritten: " + attempt.getHdfsBytesWritten());						
		System.out.println(" FileBytesWritten: " + attempt.getFileBytesWritten());						
		System.out.println(" MapInputRecords: " + attempt.getMapInputRecords());
		System.out.println(" MapInputBytes: " + attempt.getMapInputBytes());
		System.out.println(" MapOutputRecords: " + attempt.getMapOutputRecords());
		System.out.println(" MapOutputBytes: " + attempt.getMapOutputBytes());						
		System.out.println(" SpilledRecords: " + attempt.getSpilledRecords());						
		System.out.println(" CombineInputRecords: " + attempt.getCombineInputRecords());						
		System.out.println(" ReduceInputGroups: " + attempt.getReduceInputGroups());
		System.out.println(" ReduceOutputRecords: " + attempt.getReduceOutputRecords());
		System.out.println(" ReduceShuffleBytes: " + attempt.getReduceShuffleBytes());						
		System.out.println(" ShuffleFinished: " + attempt.getShuffleFinished());
		System.out.println(" SortFinished: " + attempt.getSortFinished());
		System.out.println(" -");
	}
}