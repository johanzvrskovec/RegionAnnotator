package org.ki.meb.geneconnector;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.ki.meb.common.ApplicationException;
import org.ki.meb.common.IndexedMap;


/**
 * NOT THREAD SAFE! SHOULD BE USED PER THREAD
 * @author johkal
 *
 */
public class GwasBioinfDataCache 
{
	public static short DATATYPE_BOOLEAN = 0;
	public static short DATATYPE_DOUBLE = 1;
	public static short DATATYPE_STRING = 2;
	
	private String cacheDBURL;
	private Connection con;
	private boolean settingRefreshExistingTables;
	private File settingDBJarFile;
	
	private PreparedStatement createStatement;
	private PreparedStatement insertStatement;
	private PreparedStatement dropStatement;
	
	
	//entry variables
	private String tableName;
	private JSONObject elementMeta;
	private IndexedMap<String,JSONObject> elementNameMap;

	//private JSONArray elementNameArray;
	private JSONObject rowDataNameMap;
	private String variableNameListSQL;
	private String variableDeclarationListSQL;
	
	public GwasBioinfDataCache() 
	{
		//cacheDBURL = "jdbc:derby:GwasBioinf;create=true;user=GwasBioinfInternal;password=GwasBioinfInternalPass";
		cacheDBURL = "jdbc:derby:GwasBioinf";
		settingRefreshExistingTables=false;
		settingDBJarFile=new File("GwasBioinf.jar");
	}
	
	public GwasBioinfDataCache setRefreshExistingTables(boolean nSettingRefreshExistingTables){settingRefreshExistingTables=nSettingRefreshExistingTables; return this;}
	public GwasBioinfDataCache setDBJarFile(File nDBJarFile){settingDBJarFile=nDBJarFile; return this;}
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
            con.close();
        	try
            {
        		DriverManager.getConnection(cacheDBURL + ";shutdown=true");
            }
        	catch (java.sql.SQLNonTransientConnectionException e)
        	{
        		//Shutdown was OK - ERROR 08006: Database 'GwasBioinf' shutdown. (according to Derby) 
        	}
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
		ArrayList<String> names = elementNameMap.keys();

		for(int iVal=0; iVal<names.size(); iVal++)
		{
			assertDBString(names.get(iVal));
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
		ArrayList<String> names = elementNameMap.keys();
		if(0<names.size())
		{
			q.append("\""+names.get(0)+"\"");
		}
		for(int iVal=1; iVal<names.size(); iVal++)
		{
			q.append(",\""+names.get(iVal)+"\"");
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
		ArrayList<String> names = elementNameMap.keys();
		ArrayList<JSONObject> elements = elementNameMap.values();
		if(0<elements.size())
		{
			JSONObject e = elements.get(0);
			vartype=e.getInt("type");
			q.append("\""+names.get(0)+"\"");
			q.append(" "+getVariableTypeDeclaration(vartype));
		}
		
		for(int iVal=1; iVal<elements.size(); iVal++)
		{
			JSONObject e = elements.get(iVal);
			vartype=e.getInt("type");
			q.append(",\""+names.get(iVal)+"\"");
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
		ArrayList<JSONObject> elements = elementNameMap.values();
		return "INSERT INTO \""+tableName+"\"("+variableNameListSQL+") VALUES ("+StringUtils.repeat("?,", elements.size()-1)+StringUtils.repeat("?",Math.max(Math.min(1,elements.size()),1))+")";
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
		ResultSet rs = con.getMetaData().getFunctions(null, null, name.toUpperCase());
		boolean result =  rs.next();
		rs.close();
		return result;
	}
	
	//private void dropFunction(String functionName)
	
	private void rebuildCommonArchitecture() throws ApplicationException, SQLException
	{
		PreparedStatement s;
		try
		{
			s=con.prepareStatement("CALL SQLJ.REPLACE_JAR('"+settingDBJarFile.getAbsolutePath()+"', 'GwasBioinf')");
			s.execute();
		}
		catch (SQLException e)
		{ 
			s=con.prepareStatement("CALL SQLJ.INSTALL_JAR('"+settingDBJarFile.getAbsolutePath()+"', 'GwasBioinf',0)");
			s.execute();
		}
		
		if(!getHasFunction("stringSeparateFixedSpacingRight"))
		{
			s=con.prepareStatement("CREATE FUNCTION stringSeparateFixedSpacingRight(target VARCHAR(32672),separator VARCHAR(50),spacing INT) RETURNS VARCHAR(32672) PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'DbUtils.stringSeparateFixedSpacingRight'");
			s.execute();
		}
		
		if(!getHasFunction("stringSeparateFixedSpacingLeft"))
		{
			s=con.prepareStatement("CREATE FUNCTION stringSeparateFixedSpacingLeft(target VARCHAR(32672),separator VARCHAR(50),spacing INT) RETURNS VARCHAR(32672) PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'DbUtils.stringSeparateFixedSpacingLeft'");
			s.execute();
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
		boolean tmpAutocommit = con.getAutoCommit();
		
		con.setAutoCommit(false);
		
		JSONObject row;
		JSONArray rows = entry.getJSONArray("rows");
		if(rows.length()<=0)
			return this;
		
		elementNameMap = new IndexedMap<String,JSONObject>();
		
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
			
			if(elementMeta.has("namemap"))
				elementNameMap=elementNameMap.fromJSON(elementMeta.getJSONObject("namemap"));
			
			
			//shared variable init
			tableName=elementMeta.getString("path");
			
			
			for(int ie=0; ie<elements.length(); ie++)
			{
				JSONObject re = elements.getJSONObject(ie);
				JSONObject e = new JSONObject();
				
				if(!elementNameMap.containsKey(names.getString(ie)))
				{
					
					if(re.has("type"))
					{
						e.put("type", re.getInt("type"));
					}
					
					elementNameMap.put(names.getString(ie),e);
					
					/*
					if(re.has("index"))
					{
						if(re.getInt("index")<=elementNameMap.size())
						{
							e.put("index", re.getString("index"));
							elementNameMap.put(names.getString(ie),e,re.getInt("index"));
						}
					}
					else
					{
						elementNameMap.put(names.getString(ie),e);
					}
					*/
				}
			}
			
			if(elementNameMap.size()>=elements.length())
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
		
		insertStatement = con.prepareStatement(constructInsertStatement());
		
		for(int iRow=0; iRow<rows.length(); iRow++)
		{
			insertStatement.clearParameters();
			row=rows.getJSONObject(iRow);
			rowDataNameMap = row.getJSONObject("data");
			assertRowMeta();
			
			ArrayList<String> names = elementNameMap.keys();
			ArrayList<JSONObject> elementMeta = elementNameMap.values();
			for(int iVar=0; iVar<elementMeta.size(); iVar++)
			{
				JSONObject e = elementMeta.get(iVar);
				if(rowDataNameMap.has(names.get(iVar)))
				{
					JSONObject re = rowDataNameMap.getJSONObject(names.get(iVar));
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
			insertStatement.addBatch();
		}
		
		int[] batchRes = insertStatement.executeBatch();
		con.commit();
		con.setAutoCommit(tmpAutocommit);
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
