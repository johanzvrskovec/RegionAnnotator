package org.ki.meb.geneconnector;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.ibatis.jdbc.SQL;
import org.ki.meb.common.ApplicationException;
import org.ki.meb.common.ParallelWorker;
import org.ki.meb.geneconnector.GwasBioinfCustomFormatter.InputOutputType;

import com.healthmarketscience.sqlbuilder.SelectQuery;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;

//import getl.proc.Flow;

public class GwasBioinf //extends ParallelWorker
{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private CommandLine commandLine;
	
	private static Options clOptions = new Options();
	
	private File settingInputFolder, settingOutputFolder, settingConfigFile;
	private GwasBioinfDataCache dataCache;
	private FilenameFilter filterExcelXlsx, filterExcelCSV;
	
	static
	{
		//clOptions.addOption(OptionBuilder.create(TextMap.regtest)); //inactivated as default
		clOptions.addOption(TextMap.help,false,"Print usage help.");
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Config file").hasArg().create(TextMap.config));
		clOptions.addOption(TextMap.init,false,"Initiate the database content from input files");
		//clOptions.addOption(TextMap.refresh,false,"Refresh the database content from input files");
		clOptions.addOption(TextMap.operate,false,"Perform operation specifics");
		clOptions.addOption(TextMap.get,false,"Get the database content as exported output");
		
		
		
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Path to the folder of the input files used for initiation").hasArg().create("ipath"));
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Database function jar (special case)").hasArg().create("dbjar"));
		
		
	}

	public GwasBioinf()
	{
		filterExcelXlsx= new FilenameFilter() 
		{
			@Override
			public boolean accept(File dir, String name) 
			{
				return name.toLowerCase().matches("^.+\\.xlsx$");
			}
		};
		
		filterExcelCSV= new FilenameFilter() 
		{
			@Override
			public boolean accept(File dir, String name) 
			{
				return name.toLowerCase().matches("^.+\\.csv$");
			}
		};
	}

	private void init() throws ConfigurationException
	{
		XMLConfiguration config = new XMLConfiguration(settingConfigFile);
		ConfigurationNode rootNode = config.getRootNode();
		//setDBConnectionString((String)((ConfigurationNode)rootNode.getChildren(TextMap.database_connectionstring).get(0)).getValue());
		//setResourceFolder((String)((ConfigurationNode)rootNode.getChildren(TextMap.resourcefolderpath).get(0)).getValue());
		settingInputFolder = new File((String)((ConfigurationNode)rootNode.getChildren(TextMap.inputfolderpath).get(0)).getValue());
		settingOutputFolder = new File((String)((ConfigurationNode)rootNode.getChildren(TextMap.outputfolderpath).get(0)).getValue());
		dataCache=new GwasBioinfDataCache().setRefreshExistingTables(commandLine.hasOption(TextMap.refresh));
		if(commandLine.hasOption("dbjar"))
			dataCache.setDBJarFile(new File(commandLine.getOptionValue("dbjar")));
		
	}
	
	public static CommandLine constructCommandLine(String[] args) throws ParseException
	{
		CommandLineParser parser = new org.apache.commons.cli.GnuParser();
		CommandLine commandLine = parser.parse(clOptions, args);
		return commandLine;
	}
	
	
	public static void main(String[] args) throws Exception
	{
		//System.out.println("TEST>"+Utils.stringSeparateFixedSpacingRight("abcdefg", ",", 3));
		new GwasBioinf().setCommandLine(constructCommandLine(args)).runCommands();
	}
	
	//always to standard output
	private void printHelp()
	{
		System.out.println("Report Generator Command Line Application");
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("ReportGenerator", "", clOptions, "", true);
	}
	
	public GwasBioinf setCommandLine(CommandLine nCommandLine)
	{
		commandLine=nCommandLine;
		return this;
	}
	
	//Camel version -test
	/*
	private void initDataFromFiles_camel() throws Exception
	{
		CamelContext routingEngineContext = new DefaultCamelContext();
		ConnectionFactory conFactory = new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
		routingEngineContext.addComponent("jmscomponent", JmsComponent.jmsComponentAutoAcknowledge(conFactory));
		
		RouteBuilder routeBuilder = new RouteBuilder() 
		{
			
			@Override
			public void configure() throws Exception 
			{
				from("jmscomponent:queue:test.queue").to("file://test");
			}
		};
		
		routingEngineContext.addRoutes(routeBuilder);
		
		
		ProducerTemplate template = routingEngineContext.createProducerTemplate();
		
		routingEngineContext.start();
		
		for (int i = 0; i < 10; i++) 
		{
            template.sendBody("jmscomponent:queue:test.queue", "Hello Camel! Test Message: " + i);
        }
		
		Thread.sleep(1000);
		if(!routingEngineContext.getStatus().isStoppable())
		{
			throw new Exception("Can't stop the routing context!");
		}
		routingEngineContext.stop();
	}
	*/
	
	//POI version
	private void initDataFromFiles() throws ApplicationException, Exception
	{
		GwasBioinfCustomFormatter inputReader = new GwasBioinfCustomFormatter().setDataCache(dataCache).setOutputType(InputOutputType.DATACACHE);
		//import all files in input
		File[] inputFilesCsv = settingInputFolder.listFiles(filterExcelCSV);
		for(int iFile=0; iFile<inputFilesCsv.length; iFile++)
		{
			inputReader.setInputType(InputOutputType.CSV).setInputFile(inputFilesCsv[iFile]).read();
		}
		
		File[] inputFilesXlsx = settingInputFolder.listFiles(filterExcelXlsx);
		for(int iFile=0; iFile<inputFilesXlsx.length; iFile++)
		{
			inputReader.setInputType(InputOutputType.EXCEL).setInputFile(inputFilesXlsx[iFile]).read();
		}
		
		
	}
	
	private void getData() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException
	{

		//TODO
	}
	/*
	private void performTavernaWorkflow() throws ReaderException, IOException
	{
		//TODO
		WorkflowBundleIO io = new WorkflowBundleIO();
		WorkflowBundle ro = io.readBundle(new File("workflow.t2flow"), null);
		ro.getMainWorkflow();
	}
	*/
	private GwasBioinf runCommands() throws Exception
	{
		if(commandLine.hasOption(TextMap.help))
		{
			printHelp();
			return this;
		}
		
		if(commandLine.hasOption(TextMap.config))
		{
			settingConfigFile = new File(commandLine.getOptionValue(TextMap.config));
		}
		else
		{
			settingConfigFile = new File("config.xml");
		}
		
		init();
		
		dataCache.createCacheConnection();
		
		if(commandLine.hasOption(TextMap.init))
		{
			//initDataFromFiles_camel();
			initDataFromFiles();
		}
		
		if(commandLine.hasOption(TextMap.operate))
		{
			operate();
		}
		
		if(commandLine.hasOption(TextMap.get))
		{
			getData();
		}
		
		dataCache.shutdownCacheConnection();
		
		return this;
	}
	
	
	private void operate() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException
	{
		StringBuilder q = new StringBuilder();
		
		//create candidate genes
		//q = "SELECT \"mdd2clumpraw\".*,STRINGSEPARATEFIXEDSPACINGRIGHT(CAST(CHAR(\"six1\") AS VARCHAR(32672) ) ,',',3) AS \"cc\" FROM app.\"mdd2clumpraw\" WHERE \"p\">0 AND \"p\"<1e-5";
		//q = "SELECT TRIM(CAST(CAST(CAST(\"six1\" AS DECIMAL(25,0)) AS CHAR(38)) AS VARCHAR(32672))) AS \"six1varchar\",STRINGSEPARATEFIXEDSPACINGRIGHT(TRIM(CAST(CAST(CAST(\"six1\" AS DECIMAL(25,0)) AS CHAR(38)) AS VARCHAR(32672))),',',3) AS \"cc\" FROM app.\"mdd2clumpraw\" WHERE \"p\">0 AND \"p\"<1e-5";
		q.append("SELECT ");
		q.append("ROW_NUMBER() OVER() AS \"nrank\", \"sub\".* FROM (SELECT ");
		q.append(dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("six1"),",", 3)+"||'-'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("six2"),",", 3)+" AS \"ll\",");
		q.append("\"hg19chrc\" AS \"nr0\", \"six1\" AS \"nr1\", \"six2\" AS \"nr2\",");
		q.append("\"hg19chrc\"||':'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("six1"),",", 3)+"||'-'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("six2"),",", 3)+" AS \"cc\",");
		q.append("'=HYPERLINK(\"http://genome.ucsc.edu/cgi-bin/hgTracks?&org=Human&db=hg19&position='||\"hg19chrc\"||'%3A'||"+dataCache.scriptDoubleToVarchar("six1")+"||'-'||"+dataCache.scriptDoubleToVarchar("six2")+"||'\",\"ucsc\")' AS \"nucsc\",");
		q.append("\"mdd2clumpraw\".* FROM app.\"mdd2clumpraw\" WHERE \"p\">0 AND \"p\"<1e-5 ORDER BY \"p\") AS \"sub\"");
		dataCache.dataset("candidate", q.toString()).commit();
		
		
		//*====== genes for bioinformatics ======;
		//*=== Genes: GENCODE v17 genes in bin, expand by 20kb;
		
		q = new StringBuilder();
		q.append("SELECT \"gencode_master\".*, (\"f4\"-20000) AS \"e4\", (\"f5\"+20000) AS \"e5\" FROM app.\"gencode_master\"");
		dataCache.dataset("t2", q.toString()).commit();
		
		q = new StringBuilder();
		q.append("SELECT * FROM app.\"candidate\" INNER JOIN app.\"t2\" ON (\"nr0\"=\"f1\" AND ((\"nr1\"<=\"e4\" AND \"e4\"<=\"nr2\") OR (\"nr1\"<=\"e5\" AND \"e5\"<=\"nr2\") OR (\"e4\"<=\"nr1\" AND \"nr1\"<=\"e5\") OR (\"e4\"<=\"nr2\" AND \"nr2\"<=\"e5\")))");
		dataCache.dataset("t3", q.toString()).commit();
		
		q = new StringBuilder();
		q.append("SELECT \"rank\", \"p\", \"hg19chrc\", \"six1\", \"six2\", \"f1\" AS \"g0\", \"f4\" AS \"g1\", \"f5\" AS \"g2\", \"f7\" AS \"gstr\", \"gid\" AS \"ensgene\" FROM app.\"t3\"");
		dataCache.dataset("allgenes20kb", q.toString()).commit();
		
		
		
		
		//*====== PC genes & distance ======;
		//* expand by 10mb;
		
		q = new StringBuilder();
		q.append("SELECT \"gencode_master\".*, (\"f4\"-10e6) AS \"e4\", (\"f5\"-10e6) AS \"e5\" FROM app.\"gencode_master\" WHERE \"ttype\"='protein_coding'");
		dataCache.dataset("r1", q.toString()).commit();
		
		//* join;
		q = new StringBuilder();
		q.append("SELECT \"rank\", \"p\", \"hg19chrc\", \"six1\", \"six2\" FROM app.\"candidate\" LEFT JOIN app.\"r1\" ON (\"nr0\"=\"f1\" AND ((\"nr1\"<=\"e4\" AND \"e4\"<=\"nr2\") OR (\"nr1\"<=\"e5\" AND \"e5\"<=\"nr2\") OR (\"e4\"<=\"nr1\" AND \"nr1\"<=\"e5\") OR (\"e4\"<=\"nr2\" AND \"nr2\"<=\"e5\")))");
		dataCache.dataset("r2", q.toString()).commit();

		/*
		DbSpec bDB = new DbSpec();
		DbSchema schemaApp = bDB.createSchema("APP");
		DbTable r2Table = new DbTable(schemaApp, "r2");
		SelectQuery bq = new SelectQuery();
		String testq = bq.addFromTable(r2Table).toString();
		*/
		
		/*
		String testq = new SQL()
		{
			{
				SELECT("*");
				FROM("APP.r2");
			}
		}.toString();
		dataCache.dataset("r3", testq).commit();
		*/
		
		
		q = new StringBuilder();
		q.append("SELECT \"rank\", \"p\", \"hg19chrc\", \"six1\", \"six2\", \"nr0\", \"nr1\", \"nr2\", \"b\".* FROM \"candidate\" AS \"a\" INNER JOIN \"nhgri_gwas\" AS \"b\" ON (\"a\".\"nr0\"=\"b\".\"hg19chrom\" AND \"a\".\"nr1\"<=\"b\".\"bp\" AND \"b\".\"bp\"<=\"nr2\")");
		dataCache.dataset("nhgri", q.toString()).commit();
		
	}
	
}
