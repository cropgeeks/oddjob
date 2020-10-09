// Copyright 2020 Information & Computational Sciences, JHI. All rights
// reserved. Use is subject to the accompanying licence terms.

package jhi.oddjob;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class ProcessScheduler implements IScheduler
{
	private Logger LOG;

	private AtomicInteger jobCount = new AtomicInteger(0);
	private ConcurrentHashMap<String,Job> jobs = new ConcurrentHashMap<>();
	private ArrayList<Job> waitingJobs = new ArrayList<>();

	private int cores = Runtime.getRuntime().availableProcessors();
	private ExecutorService executor;

	@Override
	public void initialize()
	{
		LOG = Logger.getLogger(ProcessScheduler.class.getName());
	}

	@Override
	public void destroy()
		throws Exception
	{
	}

	public ProcessScheduler()
	{
		executor = Executors.newFixedThreadPool(cores);
	}

	@Override
	public JobInfo submit(String jobName, String command, List<String> args, String wrkDir)
		throws Exception
	{
		return submit(jobName, command, args, wrkDir, null);
	}

	@Override
	public JobInfo submit(String jobName, String command, List<String> args, String wrkDir, List<String> dependencies)
		throws Exception
	{
		LOG.info("Submitting a ProcessBuilder job...");
		LOG.info("Working directory: " + wrkDir);

		final String id = "" + jobCount.addAndGet(1);

		final Job job = new Job();
		job.info = new JobInfo(id, jobName);
		job.info.setTimeSubmitted(System.currentTimeMillis());
		job.dependencies = dependencies;
		jobs.put(id, job);

		job.r = new Runnable() {
			public void run()
			{
				job.info.setStatus(JobInfo.RUNNING);
				job.info.setTimeStarted(System.currentTimeMillis());

				try
				{
					args.add(0, command);

					ProcessBuilder pb = new ProcessBuilder(args);
					pb.directory(new File(wrkDir));

					// Redirect the output/error streams
					pb.redirectOutput(new File(wrkDir, command + ".o" + id));
					pb.redirectError(new File(wrkDir, command + ".e" + id));

					LOG.info("Starting process");
					Process proc = pb.start();

					LOG.info("Waiting for process");
					proc.waitFor();

					LOG.info("Process finished");
					job.info.setStatus(JobInfo.ENDED);
				}
				catch (Exception e)
				{
					LOG.log(Level.SEVERE, e.getMessage(), e);
					job.info.setStatus(JobInfo.FAILED);
				}

				job.info.setTimeEnded(System.currentTimeMillis());
				job.info.calcTimeTaken();

				processWaitingJobs();
			}
		};

		waitingJobs.add(job);
		processWaitingJobs();

		return job.info;
	}

	// Run whenever a job finishes (or is cancelled); processes the list of
	// waiting jobs (that have dependencies on other jobs) to see if any of them
	// can now start
	private synchronized void processWaitingJobs()
	{
		// For all waiting jobs...
//		for (Job job: waitingJobs)
		for (int j = 0; j < waitingJobs.size(); j++)
		{
			Job job = waitingJobs.get(j);

			boolean okToStart = true;

			// Check to see if its dependencies are still running
			if (job.dependencies != null)
			{
				for (String id: job.dependencies)
				{
					Job depJob = jobs.get(id);
					// TODO: What happens if we can't find the dependent job?
					if (depJob == null)
						continue;

					int depStatus = depJob.info.status;
					if (depStatus == JobInfo.WAITING || depStatus == JobInfo.RUNNING)
					{
						okToStart = false;
						break;
					}
				}
			}

			if (okToStart)
			{
				// Remove the job from the waiting list
				waitingJobs.remove(job);
				j--;

				job.future = executor.submit(job.r);
			}
		}
	}

	@Override
	public boolean isJobFinished(String id)
		throws Exception
	{
//		Process proc = jobs.get(id);

//		if (proc != null && proc.isAlive())
//			return false;

		if (jobs.get(id) != null)
		{
			Future<?> future = jobs.get(id).future;

			return future.isDone();
		}

		// TODO: Need a better way to handle unknown job situations
		return true;
	}

	@Override
	public void cancelJob(String id)
		throws Exception
	{
//		Process proc = jobs.get(id);

//		if (proc != null && proc.isAlive())
//			proc.destroy();

		if (jobs.get(id) != null)
		{
			Future<?> future = jobs.get(id).future;
			future.cancel(true);

//			jobs.remove(id);

			LOG.info("Cancelled job with ID " + id);
		}
	}

	@Override
	public JobInfo getJobInfo(String id)
		throws Exception
	{
		Job job = jobs.get(id);

		if (job != null)
		{
			job.info.calcTimeTaken();
			return job.info;
		}
		else
			return null;
	}

	private static class Job
	{
		private JobInfo info;

		private Future future;
		private Runnable r;

		// An optional list of other job IDs that must be finished before this
		// job will start execution
		private List<String> dependencies;
	}
}