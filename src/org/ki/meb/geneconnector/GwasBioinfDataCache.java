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
	private JSONObject elementNameIndexMap;
	//private JSONArray elementNameArray;
	private JSONObject rowDataNameMap;
	private String variableNameListSQL;
	private String variableDeclarationListSQL;
	
	public GwasBioinfDataCache() 
	{
		//cacheDBURL = "jdbc:derby:GwasBioinf;create=true;user=GwasBioinfInternal;password=GwasBioinfInternalPass";
		cacheDBURL = "jdbc:derby:GwasBioinf";
		settingRefreshExistingTables=false;
	}
	
	public GwasBioinfDataCache setRefreshExistingTables(boolean nSettingRefreshExistingTables){settingRefreshExistingTables=nSettingRefreshExistingTables; return this;}
	public boolean getRefreshExistingTables(){return settingRefreshExistingTables;}

	public GwasBioinfDataCache createCacheConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException
    {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        //Get a connection
        con = DriverManager.getConnection(cacheDBURL+";create=true;");
        con.setAutoCommit(false);
        rebuildCommonArchitecture();
        con.commit();
        return this;
    }
	
	public GwasBioinfDataCache shutdownCacheConnection() throws SQLException
    {
        if (con != null&&!con.isClosed())
        {
        	con.setAutoCommit(false);
            DriverManager.getConnection(cacheDBURL + ";shutdown=true");
            con.commit();
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
		JSONArray names = elementNameMap.names();
		for(int iVal=0; iVal<names.length(); iVal++)
		{
			assertDBString(names.getString(iVal));
		}
	}
	
	private void assertRowMeta() throws ApplicationException
	{
		//if(elementNameMap.length()!=rowDataNameMap.length())
			//throw new ApplicationException("Incoherent element count"); //this can be handled now
		
		JSONArray names = rowDataNameMap.names();
		for(int iVal=0; iVal<names.length(); iVal++)
		{
			assertDBString(names.getString(iVal));
		}
	}
	
	private void constructVariableNameListSQL() throws ApplicationException
	{
		StringBuilder q = new StringBuilder();
		JSONArray names = elementNameMap.names();
		if(0<names.length())
		{
			q.append("\""+names.getString(0)+"\"");
		}
		for(int iVal=1; iVal<names.length(); iVal++)
		{
			q.append(",\""+names.getString(iVal)+"\"");
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
		int vartype;
		JSONArray names = elementNameMap.names();
		JSONArray elements = elementNameMap.toJSONArray(names);
		if(0<elements.length())
		{
			JSONObject e = elements.getJSONObject(0);
			vartype=e.getInt("type");
			q.append("\""+names.getString(0)+"\"");
			q.append(" "+getVariableTypeDeclaration(vartype));
		}
		
		for(int iVal=1; iVal<elements.length(); iVal++)
		{
			JSONObject e = elements.getJSONObject(iVal);
			vartype=e.getInt("type");
			q.append(",\""+names.getString(iVal)+"\"");
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
		JSONArray elements = elementNameMap.toJSONArray(elementNameMap.names());
		return "INSERT INTO \""+tableName+"\"("+variableNameListSQL+") VALUES ("+StringUtils.repeat("?,", elements.length()-1)+StringUtils.repeat("?",Math.max(Math.min(1,elements.length()),1))+")";
	}
	
	public boolean getHasTable(String name) throws ApplicationException, SQLException
	{
		assertDBString(name);
		ResultSet rs = con.getMetaData().getTables(null, null, name, new String[]{"TABLE"});
		boolean result =  rs.next();
		rs.close();
		return result;
	}
	
	public boolean getHasFunction(String name) throws ApplicationException, SQLException
	{
		assertDBString(name);
		ResultSet rs = con.getMetaData().getFunctions(null, null, name);
		boolean result =  rs.next();
		rs.close();
		return result;
	}
	
	//private void dropFunction(String functionName)
	
	private void rebuildCommonArchitecture() throws ApplicationException, SQLException
	{
		if(!getHasFunction("stringSeparateFixedSpacing".toUpperCase()))
		{
			createStatement=con.prepareStatement("CREATE FUNCTION stringSeparateFixedSpacing(target VARCHAR(32672),separator VARCHAR(50),spacing INT) RETURNS VARCHAR(32672) PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'org.ki.meb.geneconnector.Utils.stringSeparateFixedSpacing'");
			createStatement.execute();
		}
		
	}
	
	public GwasBioinfDataCache dropTable(String tableName) throws SQLException
	{
		PreparedStatement ds=con.prepareStatement("DROP TABLE \""+tableName+"\"");
		ds.execute();
		return this;
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
		for(int iRow=0; iRow<rows.length(); iRow++ )
		{
			row=rows.getJSONObject(iRow);
			JSONObject rowData = row.getJSONObject("data");
			JSONArray names = rowData.names();
			JSONArray elements = rowData.toJSONArray(names);
			if(!elementMeta.has("path"))
				elementMeta.putOnce("path",row.getString("path"));
			
			if(!elementMeta.has("namemap"))
			{
				elementNameMap = new JSONObject();
				elementMeta.putOnce("namemap",elementNameMap);
			}
			
			if(!elementMeta.has("nameindexmap"))
			{
				elementNameIndexMap = new JSONObject();
				elementMeta.putOnce("nameindexmap",elementNameIndexMap);
			}
			
			/*
			if(!elementMeta.has("elementnamearray"))
			{
				elementNameArray=new JSONArray();
				elementMeta.putOnce("elementnamearray",elementNameArray);
			}
			*/
			
			//shared variable init
			tableName=elementMeta.getString("path");
			elementNameMap=elementMeta.getJSONObject("namemap");
			elementNameIndexMap=elementMeta.getJSONObject("nameindexmap");
			
			for(int ie=0; ie<elements.length()&&(!elementMeta.has("namemap")||!elementMeta.has("nameindexmap")); ie++)
			{
				JSONObject re = elements.getJSONObject(ie);
				JSONObject e = new JSONObject();
				
				if(!elementMeta.has("namemap")&&!elementNameMap.has(names.getString(ie)))
					elementNameMap.put(names.getString(ie),e);
				
				if(re.has("type"))
				{
					e.put("type", re.getInt("type"));
				}
				
				if(re.has("index"))
				{
					e.put("index", re.getString("index"));
					if(!elementMeta.has("nameindexmap")&&!elementNameIndexMap.has(names.getString(ie)))
						elementNameIndexMap.put(names.getString(ie),e.getString("index"));
				}
			}
			
			if(elementNameMap.length()>=elements.length()&&elementNameIndexMap.length()>=elements.length())
				break;
			
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
			assertRowMeta();
			insertStatement = con.prepareStatement(constructInsertStatement());
			JSONArray names = elementNameMap.names();
			JSONArray elements = elementNameMap.toJSONArray(names);
			for(int iVar=0; iVar<elements.length(); iVar++)
			{
				JSONObject e = elements.getJSONObject(iVar);
				if(rowDataNameMap.has(names.getString(iVar)))
				{
					JSONObject re = rowDataNameMap.getJSONObject(names.getString(iVar));
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
	
	public GwasBioinfDataCache dataset(String name, String query) throws ApplicationException, SQLException
	{
		if(getHasTable(name))
		{
			dropTable(name);
		}
		
		PreparedStatement ps = con.prepareStatement("CREATE TABLE \""+name+"\" AS "+query+" WITH NO DATA");
		ps.execute();
		ps = con.prepareStatement("INSERT INTO \""+name+"\" "+query);
		ps.execute();
		
		return this;
	}
	
	public String scriptDoubleToVarchar(String columnName)
	{
		return "TRIM(CAST(CAST(CAST(\""+columnName+"\" AS DECIMAL(25,0)) AS CHAR(38)) AS VARCHAR(32672)))";
	}
	
	public String scriptSeparateFixedSpacingRight(String expression, String separator, int spacing)
	{
		return "STRINGSEPARATEFIXEDSPACINGRIGHT("+expression+",'"+separator+"',"+spacing+")";
	}
	
	public GwasBioinfDataCache linkGenes() throws SQLException, ApplicationException
	{
		StringBuilder q = new StringBuilder();
		
		//create candidate genes
		//q = "SELECT \"mdd2clumpraw\".*,STRINGSEPARATEFIXEDSPACINGRIGHT(CAST(CHAR(\"six1\") AS VARCHAR(32672) ) ,',',3) AS \"cc\" FROM app.\"mdd2clumpraw\" WHERE \"p\">0 AND \"p\"<1e-5";
		//q = "SELECT TRIM(CAST(CAST(CAST(\"six1\" AS DECIMAL(25,0)) AS CHAR(38)) AS VARCHAR(32672))) AS \"six1varchar\",STRINGSEPARATEFIXEDSPACINGRIGHT(TRIM(CAST(CAST(CAST(\"six1\" AS DECIMAL(25,0)) AS CHAR(38)) AS VARCHAR(32672))),',',3) AS \"cc\" FROM app.\"mdd2clumpraw\" WHERE \"p\">0 AND \"p\"<1e-5";
		q.append("SELECT ");
		q.append("ROW_NUMBER() OVER() AS \"nrank\", \"sub\".* FROM (SELECT ");
		q.append(scriptSeparateFixedSpacingRight(scriptDoubleToVarchar("six1"),",", 3)+"||'-'||"+scriptSeparateFixedSpacingRight(scriptDoubleToVarchar("six2"),",", 3)+" AS \"ll\",");
		q.append("\"hg19chrc\" AS \"nr0\", \"six1\" AS \"nr1\", \"six2\" AS \"nr2\",");
		q.append("\"hg19chrc\"||':'||"+scriptSeparateFixedSpacingRight(scriptDoubleToVarchar("six1"),",", 3)+"||'-'||"+scriptSeparateFixedSpacingRight(scriptDoubleToVarchar("six2"),",", 3)+" AS \"cc\",");
		q.append("'=HYPERLINK(\"http://genome.ucsc.edu/cgi-bin/hgTracks?&org=Human&db=hg19&position='||\"hg19chrc\"||'%3A'||"+scriptDoubleToVarchar("six1")+"||'-'||"+scriptDoubleToVarchar("six2")+"||'\",\"ucsc\")' AS \"nucsc\",");
		q.append("\"mdd2clumpraw\".* FROM app.\"mdd2clumpraw\" WHERE \"p\">0 AND \"p\"<1e-5 ORDER BY \"p\") AS \"sub\"");
		dataset("candidate", q.toString());
		con.commit();
		
		q = new StringBuilder();
		q.append("SELECT \"rank\", \"p\", \"hg19chrc\", \"six1\", \"six2\", \"nr0\", \"nr1\", \"nr2\", \"b\".* FROM \"candidate\" AS \"a\" INNER JOIN \"nhgri_gwas\" AS \"b\" ON (\"a\".\"nr0\"=\"b\".\"hg19chrom\" AND \"a\".\"nr1\"<=\"b\".\"bp\" AND \"b\".\"bp\"<=\"nr2\")");
		dataset("nhgri", q.toString());
		con.commit();
		
		return this;
	}
}
