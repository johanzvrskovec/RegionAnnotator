package org.ki.meb.geneconnector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
	private boolean settingRefreshExistingTables;
	
	private PreparedStatement createStatement;
	private PreparedStatement insertStatement;
	private PreparedStatement dropStatement;
	
	
	//entry variables
	private String tableName;
	private JSONObject elementMeta;
	private JSONObject elementNameMap;
	private JSONObject elementIndexNameMap;
	private JSONArray elementNameArray;
	private JSONObject rowDataNameMap;
	private JSONArray rowDataNameArray;
	private String variableNameListSQL;
	private String variableDeclarationListSQL;
	
	public GwasBioinfDataCache() 
	{
		//cacheDBURL = "jdbc:derby:GwasBioinf;create=true;user=GwasBioinfInternal;password=GwasBioinfInternalPass";
		cacheDBURL = "jdbc:derby:GwasBioinf;create=true;";
		settingRefreshExistingTables=false;
	}
	
	public GwasBioinfDataCache setRefreshExistingTables(boolean nSettingRefreshExistingTables){settingRefreshExistingTables=nSettingRefreshExistingTables; return this;}
	public boolean getRefreshExistingTables(){return settingRefreshExistingTables;}

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
	
	private static void assertDBString(String dbString) throws ApplicationException
	{
		if(!dbString.matches("^[^\\s;\"\']+$"))
			throw new ApplicationException("Database string error.");
	}
	
	private void assertMeta() throws ApplicationException
	{
		assertDBString(tableName);
		
		String varname;
		for(int iVal=0; iVal<elementNameArray.length(); iVal++)
		{
			varname = elementNameArray.getJSONObject(iVal).getString("name");
			assertDBString(varname);
		}
	}
	
	private void assertRowMeta() throws ApplicationException
	{
		if(elementNameArray.length()!=rowDataNameArray.length())
			throw new ApplicationException("Incoherent element count");
		
		String varname;
		for(int iVal=0; iVal<rowDataNameArray.length(); iVal++)
		{
			varname = rowDataNameArray.getJSONObject(iVal).getString("name");
			assertDBString(varname);
		}
	}
	
	private void constructVariableNameListSQL() throws ApplicationException
	{
		StringBuilder q = new StringBuilder();
		String varname;
		if(0<elementNameArray.length())
		{
			varname = elementNameArray.getJSONObject(0).getString("name");
			q.append("\""+varname+"\"");
		}
		for(int iVal=1; iVal<elementNameArray.length(); iVal++)
		{
			varname = elementNameArray.getJSONObject(iVal).getString("name");
			q.append(",\""+varname+"\"");
		}
		variableNameListSQL=q.toString();
	}
	
	private static String getVariableTypeDeclaration(int datatype) throws ApplicationException
	{
		if(datatype==DATATYPE_BOOLEAN)
			return "SMALLINT";
		else if(datatype==DATATYPE_DOUBLE)
			return "DOUBLE";
		else if(datatype==DATATYPE_STRING)
			return "VARCHAR(32672)";
		else throw new ApplicationException("Wrong datatype");
	}
	
	private void constructVariableDeclarationListSQL() throws JSONException, ApplicationException
	{
		StringBuilder q = new StringBuilder();
		String varname;
		int vartype;
		if(0<elementNameArray.length())
		{
			varname=elementNameArray.getJSONObject(0).getString("name");
			vartype=elementNameArray.getJSONObject(0).getInt("type");
			q.append("\""+varname+"\"");
			q.append(" "+getVariableTypeDeclaration(vartype));
		}
		
		for(int iVal=1; iVal<elementNameArray.length(); iVal++)
		{
			varname=elementNameArray.getJSONObject(iVal).getString("name");
			vartype=elementNameArray.getJSONObject(iVal).getInt("type");
			q.append(",\""+varname+"\"");
			q.append(" "+getVariableTypeDeclaration(vartype));
		}
		variableDeclarationListSQL = q.toString();
	}
	
	private String constructCreateStatement() throws ApplicationException
	{
		return "CREATE TABLE \""+tableName+"\"("+variableDeclarationListSQL+")";
	}
	
	private String constructInsertStatement() throws ApplicationException
	{
		
		return "INSERT INTO \""+tableName+"\"("+variableNameListSQL+") VALUES ("+StringUtils.repeat("?,", elementNameArray.length()-1)+StringUtils.repeat("?",Math.max(Math.min(1,elementNameArray.length()),1))+")";
	}
	
	public boolean getHasTable(String name) throws ApplicationException, SQLException
	{
		assertDBString(name);
		ResultSet rs = con.getMetaData().getTables(null, null, name, new String[]{"TABLE"});
		boolean result =  rs.next();
		rs.close();
		return result;
	}
	
	public GwasBioinfDataCache enter(JSONObject entry) throws SQLException, ApplicationException
	{
		JSONObject row;
		JSONArray rows = entry.getJSONArray("rows");
		if(rows.length()<=0)
			return this;
		
		//construct element meta
		if(entry.has("elementmeta"))
			elementMeta = entry.getJSONObject("elementmeta");
		else
			elementMeta=new JSONObject();
		
		//construct required meta from rows
		if(!elementMeta.has("namemap")||!elementMeta.has("indexmap")||!elementMeta.has("elementarray")||!elementMeta.has("path"))
		{
			for(int iRow=0; iRow<rows.length(); iRow++ )
			{
				row=rows.getJSONObject(iRow);
				JSONObject rowData = row.getJSONObject("data");
				JSONArray elements = rowData.toJSONArray(rowData.names());
				elementMeta.putOnce("path",row.getString("path"));
				elementNameMap = new JSONObject();
				elementMeta.putOnce("namemap",elementNameMap);
				elementIndexNameMap = new JSONObject();
				elementMeta.putOnce("indenamexmap",elementIndexNameMap);
				elementNameArray=new JSONArray();
				elementMeta.putOnce("elementnamearray",elementNameArray);
				
				for(int ie=0; ie<elements.length(); ie++)
				{
					JSONObject re = rowData.getJSONObject(elements.getString(ie));
					JSONObject e = new JSONObject();
					e.put("name", re.getString("name"));
					e.put("type", re.getString("type"));
					e.put("index", re.getString("index"));
					elementNameMap.putOnce(e.getString("name"),e);
					elementIndexNameMap.putOnce(e.getString("index"),e.getString("name"));
					elementNameArray.put(e.getString("name"));
				}
				
			}
			
		}
		
		assertMeta();
		
		constructVariableNameListSQL();
		constructVariableDeclarationListSQL();
		
		createStatement = con.prepareStatement(constructCreateStatement());
		
		
		boolean tablePreExists = getHasTable(tableName);
		
		if(tablePreExists)
		{
			if(settingRefreshExistingTables)
			{
				dropStatement=con.prepareStatement("DROP TABLE \""+tableName+"\"");
				dropStatement.execute();
			}
			else
				return this;
		}
		
		createStatement.execute();
		
		
		for(int iRow=0; iRow<rows.length(); iRow++)
		{
			row=rows.getJSONObject(iRow);
			rowDataNameMap = row.getJSONObject("data");
			rowDataNameArray = rowDataNameMap.toJSONArray(rowDataNameMap.names());
			assertRowMeta();
			insertStatement = con.prepareStatement(constructInsertStatement());
			for(int iVar=0; iVar<elementNameArray.length(); iVar++)
			{
				JSONObject e = elementNameMap.getJSONObject(elementNameArray.getString(iVar));
				if(rowDataNameMap.has(e.getString("name")))
				{
					JSONObject re = rowDataNameMap.getJSONObject(rowDataNameArray.getString(iVar));
					if(e.getInt("type")==DATATYPE_BOOLEAN&&re.getInt("type")==DATATYPE_BOOLEAN)
						insertStatement.setInt(iVar+1, re.getInt("value"));
					else if(e.getInt("type")==DATATYPE_DOUBLE && re.getInt("type")==DATATYPE_DOUBLE)
						insertStatement.setDouble(iVar+1, re.getDouble("value"));
					else if(e.getInt("type")==DATATYPE_STRING && re.getInt("type")==DATATYPE_STRING)
						insertStatement.setString(iVar+1, re.getString("value"));
					else throw new ApplicationException("Datatype coherencey error.");
				}
				else
				{
					if(e.getInt("type")==DATATYPE_BOOLEAN)
						insertStatement.setNull(iVar+1, java.sql.Types.INTEGER);
					else if(e.getInt("type")==DATATYPE_DOUBLE)
						insertStatement.setNull(iVar+1, java.sql.Types.DOUBLE);
					else if(e.getInt("type")==DATATYPE_STRING)
						insertStatement.setNull(iVar+1, java.sql.Types.LONGVARCHAR);
					else throw new ApplicationException("Datatype coherencey error.");
				}
			}
			insertStatement.execute();
		}
		return this;
	}

}
