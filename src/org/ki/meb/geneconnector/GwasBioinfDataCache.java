package org.ki.meb.geneconnector;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.h2.jdbcx.JdbcConnectionPool;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.ki.meb.common.ApplicationException;
import org.ki.meb.common.IndexedMap;
import org.omg.CosNaming.IstringHelper;


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
	public enum ReservedKeyword {CROSS, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, DISTINCT, EXCEPT, EXISTS, FALSE, FETCH, FOR, FROM, FULL, GROUP, HAVING, INNER, INTERSECT, IS, JOIN, LIKE, LIMIT, MINUS, NATURAL, NOT, NULL, OFFSET, ON, ORDER, PRIMARY, ROWNUM, SELECT, SYSDATE, SYSTIME, SYSTIMESTAMP, TODAY, TRUE, UNION, UNIQUE, WHERE };
	private HashSet<String> reservedKeywordsSet;
	
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
		reservedKeywordsSet=new HashSet<String>();
		fillReservedKeywordSet();
		cacheDBURL = "jdbc:h2:./GwasBioinf";
		settingRefreshExistingTables=false;
		settingDBJarFile=new File("GwasBioinf.jar");
	}
	
	private void fillReservedKeywordSet()
	{
		ReservedKeyword[] values =ReservedKeyword.values();
		for(int i=0; i<values.length; i++)
		{
			reservedKeywordsSet.add(values[i].toString());
		}
		
	}
	
	public boolean isReservedKeyword(String toTest)
	{
		return reservedKeywordsSet.contains(toTest.toUpperCase());
	}
	
	
	public GwasBioinfDataCache setRefreshExistingTables(boolean nSettingRefreshExistingTables){settingRefreshExistingTables=nSettingRefreshExistingTables; return this;}
	public GwasBioinfDataCache setDBJarFile(File nDBJarFile){settingDBJarFile=nDBJarFile; return this;}
	public boolean getRefreshExistingTables(){return settingRefreshExistingTables;}

	public GwasBioinfDataCache createCacheConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException
    {
		DataSource dSource;
		//Class.forName("org.h2.Driver").newInstance();
        //Get a connection
        //con = DriverManager.getConnection(cacheDBURL);
		dSource = JdbcConnectionPool.create(cacheDBURL, "user", "password");
		con=dSource.getConnection();
	
        //Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
        //Get a connection
        //con = DriverManager.getConnection(cacheDBURL+";create=true;");
		
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
            /*
            if(dbFlavor==DBFlavor.DERBY)
            {
	        	try
	            {
	        		DriverManager.getConnection(cacheDBURL + ";shutdown=true");
	            }
	        	catch (java.sql.SQLNonTransientConnectionException e)
	        	{
	        		//Shutdown was OK - ERROR 08006: Database 'GwasBioinf' shutdown. (according to Derby) 
	        	}
            }
            */
        }
        return this;
    }
	
	public GwasBioinfDataCache commit() throws SQLException
	{
		con.commit();
		return this;
	}
	
	private static void assertDBString(String dbString) throws ApplicationException
	{
		if(!dbString.matches("^[^\\s;\"\']+$"))
			throw new ApplicationException("Database string error.");
	}
	
	private void assertDBStringReserved(String dbString) throws ApplicationException
	{
		if(isReservedKeyword(dbString))
			throw new ApplicationException("Database string \""+dbString+"\" matches reserved keyword");
	}
	
	private void assertMeta() throws ApplicationException
	{
		assertDBString(tableName);
		assertDBStringReserved(tableName);
		ArrayList<String> names = elementNameMap.keys();

		for(int iVal=0; iVal<names.size(); iVal++)
		{
			assertDBString(names.get(iVal));
			assertDBStringReserved(names.get(iVal));
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
			q.append(names.get(0));
		}
		for(int iVal=1; iVal<names.size(); iVal++)
		{
			q.append(","+names.get(iVal));
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
			q.append(names.get(0));
			q.append(" "+getVariableTypeDeclaration(vartype));
		}
		
		for(int iVal=1; iVal<elements.size(); iVal++)
		{
			JSONObject e = elements.get(iVal);
			vartype=e.getInt("type");
			q.append(","+names.get(iVal));
			q.append(" "+getVariableTypeDeclaration(vartype));
		}
		variableDeclarationListSQL = q.toString();
	}
	
	private String constructCreateStatement() throws ApplicationException
	{
		return "CREATE TABLE "+tableName+"("+variableDeclarationListSQL+")";
	}
	
	private String constructDropStatement() throws ApplicationException
	{
		return "DROP TABLE "+tableName.toUpperCase();
	}
	
	private String constructInsertStatement() throws ApplicationException
	{
		ArrayList<JSONObject> elements = elementNameMap.values();
		return "INSERT INTO "+tableName+"("+variableNameListSQL+") VALUES ("+StringUtils.repeat("?,", elements.size()-1)+StringUtils.repeat("?",Math.max(Math.min(1,elements.size()),1))+")";
	}
	
	public boolean getHasTable(String name) throws ApplicationException, SQLException
	{
		assertDBString(name);
		ResultSet rs = con.getMetaData().getTables(null, null, name.toUpperCase(), new String[]{"TABLE"});
		boolean result =  rs.next();
		rs.close();
		return result;
	}
	
	/**
	 * Deprecated
	 * 
	 * @param name
	 * @return
	 * @throws ApplicationException
	 * @throws SQLException
	 */
	public boolean getHasFunction(String name) throws ApplicationException, SQLException
	{
		assertDBString(name);
		//ResultSet rs = con.getMetaData().getFunctions(null, null, name.toUpperCase());
		//boolean result =  rs.next();
		//rs.close();
		
		//return result;
		return false;
	}
	
	//private void dropFunction(String functionName)
	
	private void rebuildCommonArchitecture() throws ApplicationException, SQLException
	{
		PreparedStatement s;
		s=con.prepareStatement("CREATE ALIAS IF NOT EXISTS stringSeparateFixedSpacingRight FOR \"org.ki.meb.common.Utils.stringSeparateFixedSpacingRight\"");
		s.execute();
		s=con.prepareStatement("CREATE ALIAS IF NOT EXISTS stringSeparateFixedSpacingLeft FOR \"org.ki.meb.common.Utils.stringSeparateFixedSpacingLeft\"");
		s.execute();
		s=con.prepareStatement("CREATE ALIAS IF NOT EXISTS NUM_MAX_DOUBLE FOR \"org.ki.meb.common.Utils.numMaxDouble\"");
		s.execute();
		s=con.prepareStatement("CREATE ALIAS IF NOT EXISTS NUM_MIN_DOUBLE FOR \"org.ki.meb.common.Utils.numMinDouble\"");
		s.execute();
		s=con.prepareStatement("CREATE ALIAS IF NOT EXISTS NUM_MAX_INTEGER FOR \"org.ki.meb.common.Utils.numMaxInteger\"");
		s.execute();
		s=con.prepareStatement("CREATE ALIAS IF NOT EXISTS NUM_MIN_INTEGER FOR \"org.ki.meb.common.Utils.numMinInteger\"");
		s.execute();
	}
	
	public GwasBioinfDataCache dropTable(String tableName) throws SQLException
	{
		PreparedStatement ds=con.prepareStatement("DROP TABLE "+tableName.toUpperCase()+" CASCADE");
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
				dropStatement=con.prepareStatement(constructDropStatement());
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
	
	public GwasBioinfDataCache table(String name, String query) throws ApplicationException, SQLException
	{
		assertDBString(name);
		assertDBStringReserved(name);
		
		if(getHasTable(name))
		{
			dropTable(name);
		}
		
		PreparedStatement ps = con.prepareStatement("CREATE TABLE "+name.toUpperCase()+" AS "+query);
		ps.execute();
		return this;
	}
	
	public GwasBioinfDataCache view(String name, String query) throws ApplicationException, SQLException
	{
		assertDBString(name);
		assertDBStringReserved(name);
		PreparedStatement ps = con.prepareStatement("CREATE OR REPLACE VIEW "+name.toUpperCase()+" AS "+query);
		ps.execute();
		return this;
	}
	
	public GwasBioinfDataCache index(String tablename, String columnname) throws SQLException, ApplicationException
	{
		return index(tablename,columnname, tablename+"_"+columnname);
	}
	
	public GwasBioinfDataCache index(String tablename, String columnname, String indexname) throws SQLException, ApplicationException
	{
		assertDBString(indexname);
		assertDBStringReserved(indexname);
		PreparedStatement ps = con.prepareStatement("CREATE INDEX IF NOT EXISTS "+indexname.toUpperCase()+" ON "+tablename.toUpperCase()+"("+columnname.toUpperCase()+")");
		ps.execute();
		return this;
	}
	
	public String scriptDoubleToVarchar(String columnName)
	{
		//return "TRIM(CAST(CAST(CAST(\""+columnName+"\" AS DECIMAL(25,0)) AS CHAR(38)) AS VARCHAR(32672)))";
		return " TRIM(CAST(CAST("+columnName.toUpperCase()+" AS DECIMAL(25,0)) AS VARCHAR(32672)))";
	}
	
	public String scriptSeparateFixedSpacingRight(String expression, String separator, int spacing)
	{
		return "STRINGSEPARATEFIXEDSPACINGRIGHT("+expression+",'"+separator+"',"+spacing+")";
	}
	
	public String scriptTwoSegmentOverlapCondition(String a0, String a1, String b0, String b1)
	{
		return "(("+a0+"<="+b0+" AND "+b0+"<="+a1+") OR ("+a0+"<="+b1+" AND "+b1+"<="+a1+") OR ("+b0+"<="+a0+" AND "+a0+"<="+b1+") OR ("+b0+"<="+a1+" AND "+a1+"<="+b1+"))";
	}
	
}
