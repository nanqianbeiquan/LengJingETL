package tools;

import java.io.PrintWriter;
import java.io.StringWriter;

public class SysFunc {

	public static String getError(Exception e)
	{
		StringWriter sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw, true));
		return sw.toString();
	}
}
