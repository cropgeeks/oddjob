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

	private String id;
	private int status = WAITING;

	public JobInfo(String id)
	{
		this.id = id;
	}

	public String getId()
		{ return id; }

	public int getStatus()
		{ return status; }

	public void setStatus(int status)
		{ this.status = status; }
}