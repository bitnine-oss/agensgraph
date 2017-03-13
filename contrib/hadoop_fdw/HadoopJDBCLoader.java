/*-------------------------------------------------------------------------
 *
 *		  foreign-data wrapper for HADOOP 
 *
 * IDENTIFICATION
 *		  hadoop_fdw/HadoopJDBCLoader.java
 *
 *-------------------------------------------------------------------------
 */

import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.MalformedURLException;
 
public class HadoopJDBCLoader extends URLClassLoader
{

/*
 * HadoopJDBCLoader
 *		Constructor of HadoopJDBCLoader class.
 */
	public 
	HadoopJDBCLoader(URL[] path)
	{
		super(path);
	}

/*
 * addPath
 *		Adds a path to the path of the loader.
 */
	public void 
	addPath(String path) throws MalformedURLException
	{
		addURL(new URL (path));
	}

/*
 * CheckIfClassIsLoaded
 *		Checks if a class of given classname has been loaded by the loader or not.
 */
	public Class
	CheckIfClassIsLoaded(String ClassName)
	{
		return findLoadedClass(ClassName);
	}
}
