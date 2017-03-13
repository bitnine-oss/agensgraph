/*-------------------------------------------------------------------------
 *
 *		  foreign-data wrapper for HADOOP
 *
 * IDENTIFICATION
 *		  hadoop_fdw/HadoopJDBCUtils.java
 *
 *-------------------------------------------------------------------------
 */

import java.sql.*;
import java.text.*;
import java.io.*;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.MalformedURLException;
import java.util.*;


public class HadoopJDBCUtils
{
	private ResultSet result_set;
	private ResultSet result_set1;
	private Connection conn;
	private int NumberOfColumns;
	private int NumberOfRows;
	private Statement sql;
	private		String[] Iterate;
	private static HadoopJDBCLoader Hadoop_Driver_Loader;
	private StringWriter exception_stack_trace_string_writer;
	private PrintWriter exception_stack_trace_print_writer;
	private		ArrayList < String > mylist;


/*
 * ConnInitialize
 *		Initiates the connection to the foreign database after setting
 *		up initial configuration and executes the query.
*/
	public String
	ConnInitialize(String[] options_array) throws IOException
	{
		DatabaseMetaData	db_metadata;
		ResultSetMetaData	result_set_metadata;
		Properties		HadoopProperties;
		Class			HadoopDriverClass = null;
		Driver				HadoopDriver = null;
		String				DriverClassName = options_array[0];
		String				url = options_array[1];
		String				userName = options_array[2];
		String				password = options_array[3];

		exception_stack_trace_string_writer = new StringWriter();
		exception_stack_trace_print_writer = new PrintWriter(exception_stack_trace_string_writer);

		NumberOfColumns = 0;
		conn = null;

		try
		{
			File	JarFile = new File(options_array[4]);
			String		jarfile_path = JarFile.toURI().toURL().toString();

			if (Hadoop_Driver_Loader == null)
			{
				/* If Hadoop_Driver_Loader is being created. */
				Hadoop_Driver_Loader = new HadoopJDBCLoader(new URL[]{JarFile.toURI().toURL()});
			}
			else if (Hadoop_Driver_Loader.CheckIfClassIsLoaded(DriverClassName) == null)
			{
				Hadoop_Driver_Loader.addPath(jarfile_path);
			}

			HadoopDriverClass = Hadoop_Driver_Loader.loadClass(DriverClassName);

			HadoopDriver = (Driver)HadoopDriverClass.newInstance();
			HadoopProperties = new Properties();

			HadoopProperties.put("user", userName);
			HadoopProperties.put("password", password);

			conn = HadoopDriver.connect(url, HadoopProperties);

		}
		catch (Exception initialize_exception)
		{
			/* If an exception occurs,it is returned back to the
			 * calling C code by returning a Java String object
			 * that has the exception's stack trace.
			 * If all goes well,a null String is returned. */
			initialize_exception.printStackTrace(exception_stack_trace_print_writer);
			return (new String(exception_stack_trace_string_writer.toString()));
		}
		return null;
	}


/*
 * Initialize
 *		Initiates the connection to the foreign database after setting
 *		up initial configuration and executes the query.
*/
	public String
	Execute_Query(String query) throws IOException
	{
		DatabaseMetaData	db_metadata;
		ResultSetMetaData	result_set_metadata;
		Properties		HadoopProperties;
		Class			HadoopDriverClass = null;
		Driver				HadoopDriver = null;

		exception_stack_trace_string_writer = new StringWriter();
		exception_stack_trace_print_writer = new PrintWriter(exception_stack_trace_string_writer);
		NumberOfColumns = 0;

		try
		{
			db_metadata = conn.getMetaData();

			sql = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			result_set = sql.executeQuery(query);

			result_set_metadata = result_set.getMetaData();
			NumberOfColumns = result_set_metadata.getColumnCount();
			Iterate = new String[NumberOfColumns];
		}
		catch (Exception initialize_exception)
		{
			/* If an exception occurs,it is returned back to the
			 * calling C code by returning a Java String object
			 * that has the exception's stack trace.
			 * If all goes well,a null String is returned. */

			initialize_exception.printStackTrace(exception_stack_trace_print_writer);
			return (new String(exception_stack_trace_string_writer.toString()));
		}
		return null;
	}

/*
 * ReturnResultSet
 *		Returns the result set that is returned from the foreign database
 *		after execution of the query to C code.
 */
	public String[]
	ReturnResultSet()
	{
		int	i = 0;

		try
		{
			/* Row-by-row processing is done in hive_fdw.One row
			 * at a time is returned to the C code. */
			if (result_set.next())
			{
				for (i = 0; i < NumberOfColumns; i++)
				{
					Iterate[i] = result_set.getString(i+1);
				}

				++NumberOfRows;

				/* The current row in result_set is returned
				 * to the C code in a Java String array that
				 * has the value of the fields of the current
				 * row as it values. */

				return (Iterate);
			}

		}
		catch (Exception returnresultset_exception)
		{
			returnresultset_exception.printStackTrace();
		}

		/* All of result_set's rows have been returned to the C code. */
		return null;
	}

/*
 * Close
 *		Releases the resources used.
 */
	public String
	Close()
	{

		try
		{
			result_set.close();
			conn.close();
			result_set = null;
			conn = null;
			Iterate = null;
		}
		catch (Exception close_exception)
		{
			/* If an exception occurs,it is returned back to the
			 * calling C code by returning a Java String object
			 * that has the exception's stack trace.
			 * If all goes well,a null String is returned. */

			close_exception.printStackTrace(exception_stack_trace_print_writer);
			return (new String(exception_stack_trace_string_writer.toString()));
		}

		return null;
	}

/*
 * Cancel
 *		Cancels the query and releases the resources in case query
 *		cancellation is requested by the user.
 */
	public String
	Cancel()
	{

		try
		{
			result_set.close();
			conn.close();
		}
		catch(Exception cancel_exception)
		{
			/* If an exception occurs,it is returned back to the
			 * calling C code by returning a Java String object
			 * that has the exception's stack trace.
			 * If all goes well,a null String is returned. */

			cancel_exception.printStackTrace(exception_stack_trace_print_writer);
			return (new String(exception_stack_trace_string_writer.toString()));
		}

		return null;
	}
/*
 **   Generates CREATE FOREIGN TABLE statements for each of the tables
 **   in the source schema and returns the list of these statements
 **   to the caller.
**/
	public String
	    PrepareDDLStmtList(String schema, String servername) throws IOException
	{
		DatabaseMetaData	db_metadata;
		ResultSetMetaData	result_set_metadata;
		ResultSetMetaData	result_set_metadata1;
		Properties		HadoopProperties;
		Class			HadoopDriverClass = null;
		Driver			HadoopDriver = null;
		String[]		column_name;
		String[]		column_type;
		int[]			column_size;
		int[]			column_length;
		int			total_col = 0;
		int			col = 0;
		int			i = 0;

		exception_stack_trace_string_writer = new StringWriter();
		exception_stack_trace_print_writer = new PrintWriter(exception_stack_trace_string_writer);

		NumberOfColumns = 0;

		try
		{
			db_metadata = conn.getMetaData();

			result_set = db_metadata.getTables(null, schema , "%", null);

			result_set_metadata = result_set.getMetaData();
			NumberOfColumns = result_set_metadata.getColumnCount();

			Iterate = new String[100];
			column_name = new String[200];
			column_type = new String[200];
			column_size = new int[200];
			column_length = new int[200];

			mylist = new ArrayList<String>();
		 try
		{
			while (result_set.next())
			{
					 String table_name = result_set.getString("TABLE_NAME");

				      result_set1 = db_metadata.getColumns(null,
						  schema,
						  table_name ,
						  null);

					result_set_metadata1 = result_set1.getMetaData();
					NumberOfColumns = result_set_metadata1.getColumnCount();

					while (result_set1.next())
					{
						column_name[total_col] =  result_set1.getString("COLUMN_NAME");
						column_type[total_col] = result_set1.getString("TYPE_NAME");
						column_size[total_col] = result_set1.getInt("COLUMN_SIZE");
						column_length[total_col] = result_set1.getInt("CHAR_OCTET_LENGTH");

					String field = column_type[total_col];

					if (field.compareTo( "TINYINT") == 0)
						column_type[total_col]	= "smallint";
					else if (field.compareTo("INT") == 0)
						column_type[total_col] = "int";
					else if (field.compareTo("BIGINT") == 0)
						column_type[total_col] = "bigint";
					else if (field.compareTo("BOOLEAN") == 0)
						column_type[total_col] = "boolean";
					else if (field.compareTo("FLOAT") == 0)
						column_type[total_col] = "float";
					else if (field.compareTo("DOUBLE") == 0)
						column_type[total_col] = "double precision";
					else if (field.compareTo("STRING") == 0)
						column_type[total_col] = "varchar";
					else if (field.compareTo("BINARY") == 0)
						column_type[total_col] = "bytea";
					else if (field.compareTo("TIMESTAMP") == 0)
						column_type[total_col] = "timestamp";
					else if (field.compareTo("DECIMAL") == 0)
						column_type[total_col] = "decimal";
					else if (field.compareTo("DATE") == 0)
						column_type[total_col] = "date";
					else if (field.compareTo("CHAR") == 0)
						column_type[total_col] = "char";
					else if (field.compareTo("VARCHAR") == 0)
                                                column_type[total_col] = "varchar";
					else	/* We need to error out for unsupported data types */
					{
						System.out.println("Unsupported Hive Data Type "+ field);
					}

					total_col++;
					}
					String stmt_str = "CREATE FOREIGN TABLE "+table_name+"(";
					for(col = 0;col<total_col;col++)
					{
						if(column_type[col].compareTo("CHAR") == 0)
						{
						  stmt_str = stmt_str+column_name[col]+" "+column_type[col]+"("+column_size[col]+")";
						}
						else if(column_type[col].compareTo("VARCHAR") == 0)
						{
						  stmt_str = stmt_str+column_name[col]+" "+column_type[col]+"("+column_size[col]+")";
						}
						else
						{
						  stmt_str = stmt_str+column_name[col]+" "+column_type[col];
						}

						 if( col == total_col-1 )
						{
						   stmt_str = stmt_str+")";
						}
						else
						{
						  stmt_str = stmt_str+",";
						}

					}
					stmt_str = stmt_str+" SERVER " + servername +  " OPTIONS (table '"+table_name+"',schema '"+schema+"');";
					mylist.add(stmt_str);
					String s = mylist.get(i);
					System.out.println("STATEMENT "+s);
					i++;
					total_col = 0;
			 }
			int sz = mylist.size();
			Iterate = new String[sz];
			for (int j = 0; j < sz; j++)
			{
				Iterate[j] = mylist.get(j);
			}
			NumberOfRows = sz;
		}
		catch (Exception returnresultset_exception)
		{
			returnresultset_exception.printStackTrace();
		}

	}
	catch (Exception initialize_exception)
		{
			/* If an exception occurs,it is returned back to the
			 * calling C code by returning a Java String object
			 * that has the exception's stack trace.
			 * If all goes well,a null String is returned. */

			initialize_exception.printStackTrace(exception_stack_trace_print_writer);
			return (new String(exception_stack_trace_string_writer.toString()));
		}

	return null;
}

/*  Retruns the foreign table DDL statements string array
 *
 */


public String[]
	ReturnDDLStmtList()
	{
		int i=0;
		return Iterate;
	}
}
