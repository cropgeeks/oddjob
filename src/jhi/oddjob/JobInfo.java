package jhi.oddjob;

public class JobInfo
{
	// Job has been created but hasn't yet started
	public static final int WAITING = 1;
	// Job is active (ie thread running etc)
	public static final int RUNNING = 2;
	// Thread has terminated (try/caught) without reaching its expected end
	public static final int FAILED = 3;
	// Thread *did* reach the end of its code...but may still has failed in other
	// ways so ENDED does not necassarily mean SUCCESSFUL
	public static final int ENDED = 4;

	protected String id;
	protected String name;

	protected int status = WAITING;
	protected long timeSubmitted, timeStarted, timeEnded, timeTaken;

	public JobInfo()
	{
	}

	public JobInfo(String id, String name)
	{
		this.id = id;
		this.name = name;
	}

	public String getId()
		{ return id; }

	public String getName()
		{ return name; }

	public int getStatus()
		{ return status; }

	public void setStatus(int status)
		{ this.status = status; }

	public String getStatusString()
	{
		switch (status)
		{
			case WAITING: return "WAITING";
			case RUNNING: return "RUNNING";
			case FAILED: return "FAILED";
			case ENDED: return "ENDED";
		}

		return "UNKNOWN";
	}

	public long getTimeSubmitted()
		{ return timeSubmitted; }

	public void setTimeSubmitted(long timeSubmitted)
		{ this.timeSubmitted = timeSubmitted; }

	public long getTimeStarted()
		{ return timeStarted; }

	public void setTimeStarted(long timeStarted)
		{ this.timeStarted = timeStarted; }

	public long getTimeEnded()
		{ return timeEnded; }

	public void setTimeEnded(long timeEnded)
		{ this.timeEnded = timeEnded; }

	public long getTimeTaken()
		{ return timeTaken; }

	public void setTimeTaken(long timeTaken)
		{ this.timeTaken = timeTaken; }

	public void calcTimeTaken()
	{
		if (status == WAITING || status == RUNNING)
			timeTaken = System.currentTimeMillis() - timeSubmitted;
		else if (status == ENDED)
			timeTaken = timeEnded - timeSubmitted;

		// Tricky case with failed (when did it fail?)
		else if (status == FAILED && timeTaken == 0)
		{
			timeTaken = System.currentTimeMillis() - timeSubmitted;
		}
	}
}