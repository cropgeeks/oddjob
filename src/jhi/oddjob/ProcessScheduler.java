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

	// Tracks the index of the last job that was run
	private File jobIdFile;
	private AtomicInteger jobCount;


	private ConcurrentHashMap<String,Job> jobs = new ConcurrentHashMap<>();
	private ArrayList<Job> waitingJobs = new ArrayList<>();

	private int cores = Runtime.getRuntime().availableProcessors();
	private ThreadPoolExecutor executor;

	@Override
	public void initialize()
	{
		LOG = Logger.getLogger(ProcessScheduler.class.getName());

		if (jobCount == null)
		{
			try
			{
				BufferedReader in = new BufferedReader(new FileReader(jobIdFile));
				int index = Integer.parseInt(in.readLine());
				in.close();

				jobCount = new AtomicInteger(index);
			}
			catch (Exception e)	{
				e.printStackTrace();

				jobCount = new AtomicInteger(0);
			}
		}

		new Thread(new Runnable() {
			public void run() {
				while (true)
				{
					processWaitingJobs();

					LOG.info("used threads: " + executor.getActiveCount());

					try { Thread.sleep(5000); }
					catch (Exception e) {}
				}
			}
		}).start();
	}

	@Override
	public void destroy()
		throws Exception
	{
	}

	public ProcessScheduler()
	{
		executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(cores);
	}

	public ProcessScheduler(File jobIdFile)
	{
		this();

		this.jobIdFile = jobIdFile;
	}


	@Override
	public JobInfo submit(String jobName, String command, int requestedCores, List<String> args, String wrkDir)
		throws Exception
	{
		return submit(jobName, command, requestedCores, args, wrkDir, null);
	}

	@Override
	public JobInfo submit(String jobName, String command, int requestedCores, List<String> args, String wrkDir, List<String> dependencies)
		throws Exception
	{
		LOG.info("Submitting a ProcessBuilder job...");
		LOG.info("Working directory: " + wrkDir);

		final String id = getNextJobId();

		final Job job = new Job();
		job.info = new JobInfo(id, jobName);
		job.requestedCores = requestedCores;
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
					pb.redirectErrorStream(true);
					pb.redirectOutput(new File(wrkDir, command + ".o" + id));

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
					job.info.setTimeEnded(System.currentTimeMillis());
					job.info.calcTimeTaken();
				}

				job.info.setTimeEnded(System.currentTimeMillis());
				job.info.calcTimeTaken();

//				processWaitingJobs();
			}
		};

		waitingJobs.add(job);

		for (int i = 0; i < job.requestedCores-1; i++)
		{
			final int dummyID = i;

			Runnable dummyThread = new Runnable() {
				public void run()
				{
					while (job.future.isDone() == false)
					{
						LOG.info(job.info.getId() + " is still running; checked by dummy " + dummyID);

						try { Thread.sleep(5000); }
						catch (Exception e) {}
					}
				}
			};

			job.dummyThreads.add(dummyThread);
		}

//		processWaitingJobs();

		return job.info;
	}

	private synchronized String getNextJobId()
	{
		int jobId = jobCount.addAndGet(1);

		if (jobIdFile != null)
		{
			try
			{
				BufferedWriter out = new BufferedWriter(new FileWriter(jobIdFile));
				out.write("" + jobId);
				out.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}

		return "" + jobId;
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

			if (okToStart && executor.getCorePoolSize() - executor.getActiveCount() >= job.requestedCores)
			{
				// Remove the job from the waiting list
				waitingJobs.remove(job);
				j--;

				job.future = executor.submit(job.r);

				for (Runnable r: job.dummyThreads)
					executor.submit(r);
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
		private int requestedCores = 1;

		// An optional list of other job IDs that must be finished before this
		// job will start execution
		private List<String> dependencies;

		private List<Runnable> dummyThreads = new ArrayList<Runnable>();
	}
}