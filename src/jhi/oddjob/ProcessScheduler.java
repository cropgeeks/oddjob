// Copyright 2018 Information & Computational Sciences, JHI. All rights
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
		LOG.info("Submitting a ProcessBuilder job...");

		final String id = "" + jobCount.addAndGet(1);

		final Job job = new Job();
		job.info = new JobInfo(id, jobName);
		job.info.setTimeSubmitted(System.currentTimeMillis());
		jobs.put(id, job);

		Runnable r = new Runnable() {
			public void run()
			{
				job.info.setStatus(JobInfo.RUNNING);
				job.info.setTimeStarted(System.currentTimeMillis());

				try
				{
					args.add(0, command);

					ProcessBuilder pb = new ProcessBuilder(args);
					pb.directory(new File(wrkDir));

					LOG.info("Starting process");
					Process proc = pb.start();

//					jobs.put(id, proc);

					LOG.info("Waiting for process");
					File oFile = new File(wrkDir, command + ".o" + id);
					SOutputCatcher oStream = new SOutputCatcher(proc.getInputStream(), oFile);
					File eFile = new File(wrkDir, command + ".e" + id);
					SOutputCatcher eStream = new SOutputCatcher(proc.getErrorStream(), eFile);

					proc.waitFor();

					oStream.close();
					eStream.close();

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
			}
		};

		job.future = executor.submit(r);


//		new Thread(r).start();

		return job.info;
	}



	private class SOutputCatcher extends StreamCatcher
	{
		PrintWriter out;

		SOutputCatcher(InputStream in, File oFile)
			throws IOException
		{
			super(in);
			out = new PrintWriter(new BufferedWriter(new FileWriter(oFile)));
		}

		@Override
		protected void processLine(String line)
			throws Exception
		{
			out.println(line);

			// If we don't flush on every line, the file can look 'empty' to
			// anything monitoring it while the job is still running
			out.flush();
		}

		void close()
			throws Exception
		{
			out.close();
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
	}
}