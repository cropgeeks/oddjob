// Copyright 2018 Information & Computational Sciences, JHI. All rights
// reserved. Use is subject to the accompanying licence terms.

package jhi.oddjob;

import java.util.*;

public interface IScheduler
{
	public void initialize()
		throws Exception;

	public void destroy()
		throws Exception;

	public JobInfo submit(String jobName, String command, int requestedCores, List<String> args, String wrkDir)
		throws Exception;

	public JobInfo submit(String jobName, String command, int requestedCores, List<String> args, String wrkDir, List<String> depIDs)
		throws Exception;

	public boolean isJobFinished(String id)
		throws Exception;

	public void cancelJob(String id)
		throws Exception;

	public JobInfo getJobInfo(String id)
		throws Exception;
}