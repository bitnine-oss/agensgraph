/*-------------------------------------------------------------------------
 **
 **                Hive JDBC test client program  
 **                to connect the Hive server
 **
 ** IDENTIFICATION
 **                hadoop_fdw/Test/Cli/HiveJdbcClient.java
 **
 **-------------------------------------------------------------------------
**/

import java.util.*;
import java.sql.*;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;

public class HiveJdbcClient {

    private static String url       	 	= "jdbc:hive2://";     
    private static final String query    	= "SHOW DATABASES";
    private static String schema   	 	= "default";
    private static String user     	 	= "";	
    private static String password       	= "";
    private static final String driverName 	= "org.apache.hive.jdbc.HiveDriver";
    private static String host			= "localhost";
    private static String port			= "10000"; 
    private static int arg_count 		= 0;
   
public static void main(String[] args) throws SQLException {

	int count = 0;
        String value = null;

	if (args.length > 8)
	{
		System.out.println("Nummber of arguments are exceeding the limit");
		System.exit(0);
	}
	
	try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
	
	for(int i = 0; i < args.length; i++)
	{	
		switch(args[i])
		{
	   		case "-h" : 
			{
				if((i+1) == args.length)
				{
					System.out.println("\n Please enter the hostname");
					System.exit(0);
				}	 
				host = args[i+1];
				break;
			}
	   		case "-p" : 
	        	{
		 		if((i+1) == args.length)
                                {
                                        System.out.println("\n Please enter the port number");
                                        System.exit(0);
                                }
				port = args[i+1];
		 		break;
			}
	   		case "-s" : 
			{
				if((i+1) == args.length)
                                {
                                        System.out.println("\n Please enter the schema name");
                                        System.exit(0);
                                }
				schema = args[i+1];
           			break;
			}
	   		case "-help":
			{
				System.out.println("\n HiveJdbClient is a Hive JDBC clinet connection test with Hive2 server");
				System.out.println("\n General options:");
				System.out.println("\t -h =HOSTNAME   hive server host name or IP address(default: localhost)");
 				System.out.println("\t -p =PORT       database name to connect to (default: 10000)");
  				System.out.println("\t -s =SCHEMA     database schema name (default: default)");  
				System.exit(0);
		   	}
			default:
			{	
				System.out.println("HivejdbcClient: invalid option");
				System.out.println("Try HivejdbcClient -help  for more information.");
				System.exit(0);
			}
		}
		i++; 
 	}
	
	url = url+host+":"+port+"/"+schema;

        Connection con = DriverManager.getConnection(url, user, password);
        Statement stmt = con.createStatement();

        System.out.println("\nRunning query on Hive Server: \n" + query);
        System.out.println("\nRunning query result on Hive Server: ");
	
	 ResultSet res = stmt.executeQuery(query);

        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
}
