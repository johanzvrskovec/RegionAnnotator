package org.ki.meb.geneconnector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

public class GwasBioinfDataCache 
{
	public static short DATATYPE_BOOLEAN = 0;
	public static short DATATYPE_DOUBLE = 1;
	public static short DATATYPE_STRING = 2;
	
	private String cacheDBURL;
	private Connection con;
	
	private PreparedStatement insertStatement;
	
	
	//entry variables
	private String path;
	private JSONArray data;
	private String variableNameListSQL;
	
	public GwasBioinfDataCache() 
	{
		cacheDBURL = "jdbc:derby:GwasBioinf;create=true;user=GwasBioinfInternal;password=GwasBioinfInternalPass";
	}
	
	public GwasBioinfDataCache createCacheConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
    {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        //Get a connection
        con = DriverManager.getConnection(cacheDBURL);
        return this;
    }
	
	public GwasBioinfDataCache shutdownCacheConnection() throws SQLException
    {
        if (con != null)
        {
            DriverManager.getConnection(cacheDBURL + ";shutdown=true");
            con.close();
        }
        return this;
    }
	
	private String constructVariableNameListSQL()
	{
		StringBuilder q = new StringBuilder();
		if(0<data.length())
			q.append(data.getJSONObject(0).getString("name"));
		
		for(int iVal=1; iVal<data.length(); iVal++)
		{
			q.append(","+data.getJSONObject(iVal).getString("name"));
		}
		
		return q.toString();
	}
	
	private String constructCreateStatement()
	{
		return "INSERT INTO "+path+"("+variableNameListSQL+") VALUES ("+StringUtils.repeat("?,", data.length()-1)+StringUtils.repeat("?",Math.min(1,data.length()))+");";
	}
	
	private String constructInsertStatement()
	{
		return "INSERT INTO "+path+"("+variableNameListSQL+") VALUES ("+StringUtils.repeat("?,", data.length()-1)+StringUtils.repeat("?",Math.min(1,data.length()))+");";
	}
	
	public GwasBioinfDataCache enterRow(JSONObject row) throws SQLException, ApplicationException
	{
		path = row.getString("path");
		if(!path.matches("^[^\\s;\"\']+$"))
			throw new ApplicationException("(Table) Path syntax error");
		data = row.getJSONArray("data");
		
		variableNameListSQL = constructVariableNameListSQL();
		
		insertStatement = con.prepareStatement(constructInsertStatement());
		return this;
	}

}
