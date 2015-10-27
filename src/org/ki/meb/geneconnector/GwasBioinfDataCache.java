package org.ki.meb.geneconnector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class GwasBioinfDataCache 
{
	public static short DATATYPE_BOOLEAN = 0;
	public static short DATATYPE_DOUBLE = 1;
	public static short DATATYPE_STRING = 2;
	
	private String cacheDBURL;
	private Connection con;
	
	private PreparedStatement createStatement;
	private PreparedStatement insertStatement;
	
	
	//entry variables
	private String path;
	private JSONArray data;
	private String variableNameListSQL;
	private String variableDeclarationListSQL;
	
	public GwasBioinfDataCache() 
	{
		//cacheDBURL = "jdbc:derby:GwasBioinf;create=true;user=GwasBioinfInternal;password=GwasBioinfInternalPass";
		cacheDBURL = "jdbc:derby:GwasBioinf;create=true;";
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
	
	private void assertDBString(String dbString) throws ApplicationException
	{
		if(!dbString.matches("^[^\\s;\"\']+$"))
			throw new ApplicationException("Database string error.");
	}
	
	private void assertVarnames() throws ApplicationException
	{
		String varname;
		for(int iVal=0; iVal<data.length(); iVal++)
		{
			varname = data.getJSONObject(iVal).getString("name");
			assertDBString(varname);
		}
	}
	
	private String constructVariableNameListSQL() throws ApplicationException
	{
		StringBuilder q = new StringBuilder();
		String varname;
		if(0<data.length())
		{
			varname = data.getJSONObject(0).getString("name");
			q.append("\""+varname+"\"");
		}
		for(int iVal=1; iVal<data.length(); iVal++)
		{
			varname = data.getJSONObject(iVal).getString("name");
			q.append(",\""+varname+"\"");
		}
		
		return q.toString();
	}
	
	private String constructVariableTypeDeclaration(int datatype) throws ApplicationException
	{
		if(datatype==DATATYPE_BOOLEAN)
			return "SMALLINT";
		else if(datatype==DATATYPE_DOUBLE)
			return "DOUBLE";
		else if(datatype==DATATYPE_STRING)
			return "VARCHAR(32672)";
		else throw new ApplicationException("Wrong datatype");
	}
	
	private String constructVariableDeclarationListSQL() throws JSONException, ApplicationException
	{
		StringBuilder q = new StringBuilder();
		String varname;
		int vartype;
		if(0<data.length())
		{
			varname=data.getJSONObject(0).getString("name");
			vartype=data.getJSONObject(0).getInt("type");
			q.append("\""+varname+"\"");
			q.append(" "+constructVariableTypeDeclaration(vartype));
		}
		
		for(int iVal=1; iVal<data.length(); iVal++)
		{
			varname=data.getJSONObject(iVal).getString("name");
			vartype=data.getJSONObject(iVal).getInt("type");
			q.append(",\""+varname+"\"");
			q.append(" "+constructVariableTypeDeclaration(vartype));
		}
		
		return q.toString();
	}
	
	private String constructCreateStatement() throws ApplicationException
	{
		return "CREATE TABLE \""+path+"\"("+variableDeclarationListSQL+")";
	}
	
	private String constructInsertStatement() throws ApplicationException
	{
		return "INSERT INTO \""+path+"\"("+variableNameListSQL+") VALUES ("+StringUtils.repeat("?,", data.length()-1)+StringUtils.repeat("?",Math.min(1,data.length()))+")";
	}
	
	public GwasBioinfDataCache enterRow(JSONObject row) throws SQLException, ApplicationException
	{
		return enterRows(new JSONArray().put(row));
	}
	
	public GwasBioinfDataCache enterRows(JSONArray rows) throws SQLException, ApplicationException
	{
		JSONObject row;
		if(rows.length()>0)
		{
			row=rows.getJSONObject(0);
			path = row.getString("path");
			assertDBString(path);
			data = row.getJSONArray("data");
			assertVarnames();
		}
		variableNameListSQL = constructVariableNameListSQL();
		variableDeclarationListSQL=constructVariableDeclarationListSQL();
		
		createStatement = con.prepareStatement(constructCreateStatement());
		try
		{
			createStatement.execute();
		}
		catch (SQLException e)
		{
			//the table already exists
		}
		
		for(int iRow=0; iRow<rows.length(); iRow++)
		{
			row=rows.getJSONObject(iRow);
			data = row.getJSONArray("data");
			assertVarnames();
			insertStatement = con.prepareStatement(constructInsertStatement());
			for(int iVar=0; iVar<data.length(); iVar++)
			{
				if(data.getJSONObject(iVar).getInt("type")==DATATYPE_BOOLEAN)
					insertStatement.setInt(iVar+1, data.getJSONObject(iVar).getInt("value"));
				else if(data.getJSONObject(iVar).getInt("type")==DATATYPE_DOUBLE)
					insertStatement.setDouble(iVar+1, data.getJSONObject(iVar).getDouble("value"));
				else if(data.getJSONObject(iVar).getInt("type")==DATATYPE_STRING)
					insertStatement.setString(iVar+1, data.getJSONObject(iVar).getString("value"));
				else throw new ApplicationException("Datatype conversion error.");
			}
			insertStatement.execute();
		}
		return this;
	}

}
