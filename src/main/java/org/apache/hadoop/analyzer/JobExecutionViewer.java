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

//TODO verificar casos com valor null, se representam inconsistência ou não

public class JobExecutionViewer {
	
	private static final String fileName = "/home/thiago/tmp/experiment/1st_experiment/traces/30-job-trace.json";

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

		System.out.println();
		System.out.println("### ComputonsPerMapInputByte: " + job.getComputonsPerMapInputByte());
		System.out.println("### ComputonsPerMapOutputByte: " + job.getComputonsPerMapOutputByte());
		System.out.println("### ComputonsPerReduceInputByte: " + job.getComputonsPerReduceInputByte());
		System.out.println("### ComputonsPerReduceOutputByte: " + job.getComputonsPerReduceOutputByte());

		System.out.println();
		System.out.println("### TotalMaps: " + job.getTotalMaps());
		System.out.println("### TotalReduces: " + job.getTotalReduces());		
		System.out.println("### FailedMapperFraction: " + job.getFailedMapperFraction());

		System.out.println();
		System.out.println("### ClusterMapMB: " + job.getClusterMapMB());
		System.out.println("### ClusterReduceMB: " + job.getClusterReduceMB());
		System.out.println("### HeapMB: " + job.getHeapMegabytes());
		System.out.println("### JobMapMB: " + job.getJobMapMB());
		System.out.println("### JobReduceMB: " + job.getJobReduceMB());

		System.out.println();
		System.out.println("### StartupTime: " + (job.getLaunchTime() - job.getSubmitTime()));
		System.out.println("### RunTime: " + (job.getFinishTime() - job.getLaunchTime()));
		System.out.println("### RelativeTime: " + job.getRelativeTime());

		System.out.println();
		System.out.println("### DependantJobs:");
		List<String> directDependantJobs = job.getDirectDependantJobs();
		if(directDependantJobs != null){
			for (String string : directDependantJobs) {
				System.out.println(string);
			}
		}

		System.out.println();
		System.out.println("### TriesToSucceed:");
		double[] mapperTries = job.getMapperTriesToSucceed();
		if(mapperTries != null){
			for (double attempt : mapperTries) {
				System.out.println(attempt);
			}
		}

		System.out.println();
		System.out.println("### MapTasks:");
		List<LoggedTask> mapTasks = job.getMapTasks();
		if(mapTasks != null){
			for (LoggedTask mapTask : mapTasks) {				
				System.out.println("ID: " + mapTask.getTaskID());			
				System.out.println("Type: " + mapTask.getTaskType());
				System.out.println("Status: " + mapTask.getTaskStatus());
				System.out.println("RunTime: " + (mapTask.getFinishTime() - mapTask.getStartTime()));
				System.out.println("InputBytes: " + mapTask.getInputBytes());
				System.out.println("InputRecords: " + mapTask.getInputRecords());
				System.out.println("OutputBytes: " + mapTask.getOutputBytes());
				System.out.println("OutputRecords: " + mapTask.getOutputRecords());
				List<LoggedLocation> llList = mapTask.getPreferredLocations();
				if(llList != null){
					System.out.print("DataLocations:");
					for (LoggedLocation loggedLocation : llList) {
						System.out.print(" " + loggedLocation.getLayers());
					}
					System.out.println("");
				}
				System.out.println(" ## Attempts:");			
				List<LoggedTaskAttempt> attemptList = mapTask.getAttempts();
				if(attemptList != null){
					for (LoggedTaskAttempt mapAttempt : attemptList) {
						System.out.println(" ID: " + mapAttempt.getAttemptID());
						System.out.println(" HostName: " + mapAttempt.getHostName());
						System.out.println(" Result: " + mapAttempt.getResult());
						System.out.println(" RunTime: " + (mapAttempt.getFinishTime() - mapAttempt.getStartTime()));
						if(mapAttempt.getLocation() !=null)
							System.out.println(" ExecutionLocation: " + mapAttempt.getLocation().getLayers());						
						System.out.println(" MapInputRecords: " + mapAttempt.getMapInputRecords());
						System.out.println(" MapInputBytes: " + mapAttempt.getMapInputBytes());						
						System.out.println(" HdfsBytesRead: " + mapAttempt.getHdfsBytesRead());
						System.out.println(" FileBytesRead: " + mapAttempt.getFileBytesRead());						
						System.out.println(" MapOutputRecords: " + mapAttempt.getMapOutputRecords());
						System.out.println(" MapOutputBytes: " + mapAttempt.getMapOutputBytes());						
						System.out.println(" HdfsBytesWritten: " + mapAttempt.getHdfsBytesWritten());						
						System.out.println(" FileBytesWritten: " + mapAttempt.getFileBytesWritten());
						System.out.println(" CombineInputRecords: " + mapAttempt.getCombineInputRecords());						
						System.out.println(" ReduceInputGroups: " + mapAttempt.getReduceInputGroups());
						System.out.println(" ReduceOutputRecords: " + mapAttempt.getReduceOutputRecords());
						System.out.println(" ReduceShuffleBytes: " + mapAttempt.getReduceShuffleBytes());						
						System.out.println(" ShuffleFinished: " + mapAttempt.getShuffleFinished());
						System.out.println(" SortFinished: " + mapAttempt.getSortFinished());
						System.out.println(" SpilledRecords: " + mapAttempt.getSpilledRecords());				
						System.out.println(" -");
					}
				}
				System.out.println("---");
			}			
		}

		System.out.println();
		System.out.println("### ReduceTasks:");
		List<LoggedTask> reduceTasks = job.getReduceTasks();
		if(reduceTasks != null){
			for (LoggedTask reduceTask : reduceTasks) {
				System.out.println("ID: " + reduceTask.getTaskID());			
				System.out.println("Type: " + reduceTask.getTaskType());
				System.out.println("Status: " + reduceTask.getTaskStatus());
				System.out.println("RunTime: " + (reduceTask.getFinishTime() - reduceTask.getStartTime()));
				System.out.println("InputBytes: " + reduceTask.getInputBytes());
				System.out.println("InputRecords: " + reduceTask.getInputRecords());
				System.out.println("OutputBytes: " + reduceTask.getOutputBytes());
				System.out.println("OutputRecords: " + reduceTask.getOutputRecords());
				List<LoggedLocation> llList = reduceTask.getPreferredLocations();
				if(llList != null){
					System.out.print("DataLocations:");
					for (LoggedLocation loggedLocation : llList) {
						System.out.print(" " + loggedLocation.getLayers());
					}
					System.out.println("");
				}
				System.out.println(" ## Attempts:");			
				List<LoggedTaskAttempt> attemptList = reduceTask.getAttempts();
				if(attemptList != null){
					for (LoggedTaskAttempt reduceAttempt : attemptList) {
						System.out.println(" ID: " + reduceAttempt.getAttemptID());
						System.out.println(" HostName: " + reduceAttempt.getHostName());
						System.out.println(" Result: " + reduceAttempt.getResult());
						System.out.println(" RunTime: " + (reduceAttempt.getFinishTime() - reduceAttempt.getStartTime()));
						if(reduceAttempt.getLocation() !=null)
							System.out.println(" ExecutionLocation: " + reduceAttempt.getLocation().getLayers());						
						System.out.println(" MapInputRecords: " + reduceAttempt.getMapInputRecords());
						System.out.println(" MapInputBytes: " + reduceAttempt.getMapInputBytes());						
						System.out.println(" HdfsBytesRead: " + reduceAttempt.getHdfsBytesRead());
						System.out.println(" FileBytesRead: " + reduceAttempt.getFileBytesRead());						
						System.out.println(" MapOutputRecords: " + reduceAttempt.getMapOutputRecords());
						System.out.println(" MapOutputBytes: " + reduceAttempt.getMapOutputBytes());						
						System.out.println(" HdfsBytesWritten: " + reduceAttempt.getHdfsBytesWritten());						
						System.out.println(" FileBytesWritten: " + reduceAttempt.getFileBytesWritten());
						System.out.println(" CombineInputRecords: " + reduceAttempt.getCombineInputRecords());						
						System.out.println(" ReduceInputGroups: " + reduceAttempt.getReduceInputGroups());
						System.out.println(" ReduceOutputRecords: " + reduceAttempt.getReduceOutputRecords());
						System.out.println(" ReduceShuffleBytes: " + reduceAttempt.getReduceShuffleBytes());						
						System.out.println(" ShuffleFinished: " + reduceAttempt.getShuffleFinished());
						System.out.println(" SortFinished: " + reduceAttempt.getSortFinished());
						System.out.println(" SpilledRecords: " + reduceAttempt.getSpilledRecords());				
						System.out.println(" -");
					}
				}
				System.out.println("---");
			}
		}

		System.out.println();
		System.out.println("### OtherTasks:");
		List<LoggedTask> otherTasks = job.getOtherTasks();
		if(otherTasks != null){
			for (LoggedTask otherTask : otherTasks) {
				System.out.println("ID: " + otherTask.getTaskID());			
				System.out.println("Type: " + otherTask.getTaskType());
				System.out.println("Status: " + otherTask.getTaskStatus());
				System.out.println("RunTime: " + (otherTask.getFinishTime() - otherTask.getStartTime()));
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
				System.out.println(" ## Attempts:");			
				List<LoggedTaskAttempt> attemptList = otherTask.getAttempts();
				if(attemptList != null){
					for (LoggedTaskAttempt otherAttempt : attemptList) {
						System.out.println(" ID: " + otherAttempt.getAttemptID());
						System.out.println(" HostName: " + otherAttempt.getHostName());
						System.out.println(" Result: " + otherAttempt.getResult());
						System.out.println(" RunTime: " + (otherAttempt.getFinishTime() - otherAttempt.getStartTime()));
						if(otherAttempt.getLocation() !=null)
							System.out.println(" ExecutionLocations: " + otherAttempt.getLocation().getLayers());						
						System.out.println(" MapInputRecords: " + otherAttempt.getMapInputRecords());
						System.out.println(" MapInputBytes: " + otherAttempt.getMapInputBytes());						
						System.out.println(" HdfsBytesRead: " + otherAttempt.getHdfsBytesRead());
						System.out.println(" FileBytesRead: " + otherAttempt.getFileBytesRead());						
						System.out.println(" MapOutputRecords: " + otherAttempt.getMapOutputRecords());
						System.out.println(" MapOutputBytes: " + otherAttempt.getMapOutputBytes());						
						System.out.println(" HdfsBytesWritten: " + otherAttempt.getHdfsBytesWritten());						
						System.out.println(" FileBytesWritten: " + otherAttempt.getFileBytesWritten());
						System.out.println(" CombineInputRecords: " + otherAttempt.getCombineInputRecords());						
						System.out.println(" ReduceInputGroups: " + otherAttempt.getReduceInputGroups());
						System.out.println(" ReduceOutputRecords: " + otherAttempt.getReduceOutputRecords());
						System.out.println(" ReduceShuffleBytes: " + otherAttempt.getReduceShuffleBytes());						
						System.out.println(" ShuffleFinished: " + otherAttempt.getShuffleFinished());
						System.out.println(" SortFinished: " + otherAttempt.getSortFinished());
						System.out.println(" SpilledRecords: " + otherAttempt.getSpilledRecords());				
						System.out.println(" -");
					}
				}
				System.out.println("---");
			}
		}

		System.out.println();
		System.out.println("### FailedMapAttempt:");
		ArrayList<LoggedDiscreteCDF> failedMapAttempt = job.getFailedMapAttemptCDFs();
		if(failedMapAttempt != null){
			for (LoggedDiscreteCDF ldCDF : failedMapAttempt) {				
				List<LoggedSingleRelativeRanking> lsrrList = ldCDF.getRankings();
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

		System.out.println();
		System.out.println("### FailedReduceAttempt:");
		LoggedDiscreteCDF failedReduceAttempt = job.getFailedReduceAttemptCDF();		
		List<LoggedSingleRelativeRanking> lsrrList = failedReduceAttempt.getRankings();
		if(lsrrList != null && lsrrList.size() > 0){
			System.out.println("Max: " + failedReduceAttempt.getMaximum());
			System.out.println("Min: " + failedReduceAttempt.getMinimum());
			System.out.println("[Datum,RelRanking]");
			for (LoggedSingleRelativeRanking lsrr : lsrrList) {
				System.out.println(lsrr.getDatum() + " " + lsrr.getRelativeRanking());
			}
		}

		System.out.println();
		System.out.println("### SuccessfulMapAttempt:");
		ArrayList<LoggedDiscreteCDF> successfulMapAttempt = job.getSuccessfulMapAttemptCDFs();
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

		System.out.println();		
		System.out.println("### SuccessfulReduceAttempt:");
		LoggedDiscreteCDF successfulReduceAttempt = job.getSuccessfulReduceAttemptCDF();
		System.out.println("Max: " + successfulReduceAttempt.getMaximum());
		System.out.println("Min: " + successfulReduceAttempt.getMinimum());
		lsrrList = successfulReduceAttempt.getRankings();
		if(lsrrList != null && lsrrList.size() > 0){
			System.out.println("[Datum,RelRanking]");
			for (LoggedSingleRelativeRanking lsrr : lsrrList) {
				System.out.println(lsrr.getDatum() + " " + lsrr.getRelativeRanking());
			}
		}
	}
}