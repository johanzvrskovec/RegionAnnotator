package org.ki.meb.geneconnector;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;

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
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.json.JSONObject;
import org.ki.meb.common.ApplicationException;
import org.ki.meb.common.DataCache;
import org.ki.meb.common.IndexedMap;
import org.ki.meb.common.Utils;
import org.ki.meb.common.DataCache.DataEntry;
import org.ki.meb.common.formatter.CustomFormatter;
import org.ki.meb.common.formatter.CustomFormatter.IOType;

//import getl.proc.Flow;

public class GwasBioinf //extends ParallelWorker
{
	
	private CommandLine commandLine;
	private long startTimeNanos;
	
	private static Options clOptions = new Options();
	
	private File settingConfigFile, settingInputFile, settingOutputFile;
	private CustomFormatter.IOType settingInputFormat, settingOutputFormat;
	private boolean settingReference, settingGene, settingOverwriteExistingTables, settingFirstRowVariableNames;
	private int settingDBCacheSizeKB;
	private DataCache dataCache;
	private FilenameFilter filterExcelXlsx, filterCSV, filterTSV, filterJSON;
	//public enum EntryTemplateType {_input,input,gene,reference};
	private DataCache.DataEntry referenceEntryTemplate, linkEntryTemplate;
	private IndexedMap<String, DataCache.DataEntry> entryTemplate;
	
	static
	{
		//clOptions.addOption(OptionBuilder.create(TextMap.regtest)); //inactivated as default
		clOptions.addOption(TextMap.help,false,"Print usage help.");
		
		clOptions.addOption(OptionBuilder.withArgName("file/folder path").withDescription("Input from the specified file or folder.").hasArg().create(TextMap.input));
		clOptions.addOption("reference",false,"Enter as reference data");
		clOptions.addOption("gene",false,"Enter as gene data");
		clOptions.addOption("nonames",false,"The first row of data in the input files contains NO column names");
		//clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Output to the specified file - text").hasArg().create(TextMap.output+"_text"));
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Output to the specified file - excel").hasArg().create(TextMap.output+"_excel"));
		//clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Output to the specified file - datacache").hasArg().create(TextMap.output+"_datacache"));
		clOptions.addOption(OptionBuilder.withArgName("dataset name").withDescription("Get specific database content (table/view) as exported output.").hasArg().create(TextMap.get));
		clOptions.addOption(TextMap.output+"all",false,"Output all database content.");
		
		clOptions.addOption(OptionBuilder.withArgName("format - DATACACHE,EXCEL,CSV,TSV").withDescription("Force output format.").hasArg().create("of"));
		clOptions.addOption(OptionBuilder.withArgName("format - DATACACHE,EXCEL,CSV,TSV").withDescription("Force input format.").hasArg().create("if"));
		clOptions.addOption(OptionBuilder.withArgName("true/false").withDescription("Overwrite existing tables with the same names. Default - true.").hasArg().create("overwrite"));
		clOptions.addOption(OptionBuilder.withArgName("true/false").withDescription("Perform operation specifics or not. Default - true.").hasArg().create(TextMap.operate));
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Config file").hasArg().create(TextMap.config));
		
		//clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Path to the folder of the input files used for initiation").hasArg().create("ipath"));
		
		
	}

	public GwasBioinf()
	{
		startTimeNanos= System.nanoTime();
		
		filterExcelXlsx= new FilenameFilter() 
		{
			@Override
			public boolean accept(File dir, String name) 
			{
				return name.toLowerCase().matches("^.+\\.xlsx$");
			}
		};
		
		filterCSV= new FilenameFilter() 
		{
			@Override
			public boolean accept(File dir, String name) 
			{
				return name.toLowerCase().matches("^.+\\.csv$");
			}
		};
		
		filterTSV= new FilenameFilter() 
		{
			@Override
			public boolean accept(File dir, String name) 
			{
				return name.toLowerCase().matches("^.+\\.tsv$");
			}
		};
		
		filterJSON= new FilenameFilter() 
		{
			@Override
			public boolean accept(File dir, String name) 
			{
				return name.toLowerCase().matches("^.+\\.json$");
			}
		};
	}
	
	private void printTimeMeasure()
	{
		System.out.println("Running time: "+(System.nanoTime()-startTimeNanos)/1E9+" seconds");
	}

	private void init() throws ConfigurationException, ApplicationException
	{
		XMLConfiguration config = new XMLConfiguration(settingConfigFile);
		ConfigurationNode rootNode = config.getRootNode();
		settingInputFile = new File((String)((ConfigurationNode)rootNode.getChildren(TextMap.inputfolderpath).get(0)).getValue());
		settingOutputFile = new File((String)((ConfigurationNode)rootNode.getChildren(TextMap.outputfolderpath).get(0)).getValue());
		settingDBCacheSizeKB = Integer.parseInt((String)((ConfigurationNode)rootNode.getChildren("dbcachesizekb").get(0)).getValue());
		dataCache=new DataCache("./GwasBioinf");
		settingInputFormat=null;
		settingOutputFormat=IOType.CSV;
		settingReference=false;
		settingGene=false;
		settingFirstRowVariableNames=true;
		
		if(commandLine.hasOption(TextMap.input))
		{
			settingInputFile=new File(commandLine.getOptionValue(TextMap.input));
		}
		
		
		if(commandLine.hasOption("if"))
		{
			String ov = commandLine.getOptionValue("if").toUpperCase();
			try
			{
				settingInputFormat=IOType.valueOf(ov);
			}
			catch (Exception e)
			{
				throw new ApplicationException("Input format error. Provided:"+ov,e);
			}
		}
		
		if(commandLine.hasOption("of"))
		{
			String ov = commandLine.getOptionValue("of").toUpperCase();
			try
			{
				settingOutputFormat=IOType.valueOf(ov);
			}
			catch (Exception e)
			{
				throw new ApplicationException("Output format error. Provided:"+ov,e);
			}
		}
		
		if(commandLine.hasOption("reference"))
		{
			settingReference=true;
		}
		
		if(commandLine.hasOption("gene"))
		{
			settingGene=true;
		}
		
		if(commandLine.hasOption("nonames"))
		{
			settingFirstRowVariableNames=false;
		}
		
		settingOverwriteExistingTables=true;
		if(commandLine.hasOption("overwrite"))
		{
			settingOverwriteExistingTables=Boolean.parseBoolean(commandLine.getOptionValue("overwrite"));
		}
		//else if(settingGene)
			//settingOverwriteExistingTables=false;
		
		
		
		//formalized entry templates, names in UPPER CASE!!!!
		DataEntry ne; JSONObject element;
		entryTemplate=new IndexedMap<String, DataCache.DataEntry>();
				
		ne=dataCache.newEntry("_USER_INPUT"); //WORK
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("_IO",element);

		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("CHR",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("BP1",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("BP2",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("GENENAME",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("SNPID",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.DOUBLE);
		ne.namemap.put("PVALUE",element);
		
		entryTemplate.put("_USER_INPUT", ne);
		
		
		ne=dataCache.newEntry("USER_INPUT"); //WORK
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("_IO",element);

		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("CHR",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("BP1",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("BP2",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("GENENAME",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("SNPID",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.DOUBLE);
		ne.namemap.put("PVALUE",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		element.put("isExcelFormula", true);
		ne.namemap.put("UCSC_LINK",element);
		
		entryTemplate.put("USER_INPUT", ne);
		
				
		ne=dataCache.newEntry("GENE_MASTER");

		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("CHR",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("BP1",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("BP2",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("GENENAME",element);
		
		entryTemplate.put("GENE_MASTER", ne);
		
		
		ne=dataCache.newEntry((String)null);	//reference data
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("CHR",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("BP1",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("BP2",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("GENENAME",element);
		
		referenceEntryTemplate=ne;
		
		
		ne=dataCache.newEntry((String)null);	//link data
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("_IO",element);

		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("CHR",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("BP1",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("BP2",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("GENENAME",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		ne.namemap.put("SNPID",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.DOUBLE);
		ne.namemap.put("PVALUE",element);
		
		element=new JSONObject();
		element.put("type", java.sql.Types.VARCHAR);
		element.put("isExcelFormula", true);
		ne.namemap.put("UCSC_LINK",element);
		
		linkEntryTemplate=ne;
		
		
	}
	
	public static CommandLine constructCommandLine(String[] args) throws ParseException
	{
		CommandLine commandLine;
		CommandLineParser parser = new org.apache.commons.cli.GnuParser();
		try
		{
			commandLine = parser.parse(clOptions, args);
		}
		catch (Exception e)
		{
			commandLine = parser.parse(clOptions, new String[]{"-"+TextMap.help});
		}
		return commandLine;
	}
	
	
	public static void main(String[] args) throws Exception
	{
		new GwasBioinf().setCommandLine(constructCommandLine(args)).runCommands();
	}
	
	
	private GwasBioinf runCommands() throws Exception
	{
		if(commandLine.hasOption(TextMap.help) || commandLine.getOptions().length==0
				//||(!commandLine.hasOption(TextMap.config)&&!commandLine.hasOption("reference")&&!commandLine.hasOption(TextMap.operate)&&!commandLine.hasOption(TextMap.get)&&!commandLine.hasOption("getall"))
				)
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
		dataCache.setDBCacheSizeKB(settingDBCacheSizeKB);
		dataCache.commit();
		
		//if((!commandLine.hasOption(TextMap.operate)&&!settingReference&&!settingGene)||Boolean.parseBoolean(commandLine.getOptionValue(TextMap.operate))==true)
		if(commandLine.hasOption(TextMap.input))
		{
			inputDataFromFiles();
			if(!settingGene&&!settingReference)
				operate();
		}
		
		
		
		outputDataToFiles();
		
		dataCache.shutdownCacheConnection();
		
		System.out.println("THE END");
		return this;
	}
	
	
	//always to standard output
	private void printHelp()
	{
		System.out.println("Gene Connector Command Line Application");
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("java -jar \"GwasBioinf.jar\"", "", clOptions, "", true);
	}
	
	public GwasBioinf setCommandLine(CommandLine nCommandLine)
	{
		commandLine=nCommandLine;
		return this;
	}
	
	
	
	
	private void inputDataFromFiles() throws ApplicationException, Exception
	{
			
		CustomFormatter inputReader = new CustomFormatter().setDataCache(dataCache).setOverwriteExistingTables(settingOverwriteExistingTables).setFirstRowVariableNames(settingFirstRowVariableNames);
		
		DataEntry currentEntryTemplate;
		
		
		if(settingGene)
			currentEntryTemplate=entryTemplate.getValue("GENE_MASTER");
		else if(settingReference)
			currentEntryTemplate=referenceEntryTemplate;
		else
			currentEntryTemplate=entryTemplate.getValue("_USER_INPUT"); //for standard input
		
		inputReader.setPath(currentEntryTemplate.path);
		
		if(settingInputFile.isFile())
		{
			 inputDataFromFile(settingInputFile, settingInputFormat, inputReader, currentEntryTemplate);
		}
		else if(settingInputFile.isDirectory())
		{
			
			//import all files in input
			File[] inputFilesJSON = settingInputFile.listFiles(filterJSON);
			for(int iFile=0; iFile<inputFilesJSON.length; iFile++)
			{
				inputDataFromFile(inputFilesJSON[iFile], IOType.DATACACHE, inputReader, currentEntryTemplate);
			}
			
			File[] inputFilesCsv = settingInputFile.listFiles(filterCSV);
			for(int iFile=0; iFile<inputFilesCsv.length; iFile++)
			{
				inputDataFromFile(inputFilesCsv[iFile], IOType.CSV, inputReader, currentEntryTemplate);
			}
			
			File[] inputFilesTsv = settingInputFile.listFiles(filterTSV);
			for(int iFile=0; iFile<inputFilesTsv.length; iFile++)
			{
				inputDataFromFile(inputFilesTsv[iFile], IOType.TSV, inputReader, currentEntryTemplate);
			}
			
			File[] inputFilesXlsx = settingInputFile.listFiles(filterExcelXlsx);
			for(int iFile=0; iFile<inputFilesXlsx.length; iFile++)
			{
				inputDataFromFile(inputFilesXlsx[iFile], IOType.EXCEL, inputReader, currentEntryTemplate);
			}
			
		}
		else throw new ApplicationException("Wrong type of input; it is not a file nor a directory.");
		
		dataCache.commit();
	}
	
	private void inputDataFromFile(File inputFile, IOType usedInputFormat, CustomFormatter inputReader, DataEntry currentEntryTemplate) throws InvalidFormatException, IOException, ApplicationException, SQLException
	{
		
		if(usedInputFormat==null)
		{
			if(inputFile.getName().toLowerCase().matches("^.+\\.xlsx$"))
			{
				usedInputFormat=IOType.EXCEL;
			}
			else if(inputFile.getName().toLowerCase().matches("^.+\\.csv$"))
			{
				usedInputFormat=IOType.CSV;
			}
			else if(inputFile.getName().toLowerCase().matches("^.+\\.tsv$"))
			{
				usedInputFormat=IOType.TSV;
			}
			else if(inputFile.getName().toLowerCase().matches("^.+\\.json$"))
			{
				usedInputFormat=IOType.DATACACHE;
			}
		}
		
		DataEntry currentEntry = currentEntryTemplate.copy();
		if(settingGene)
		{
			inputReader.setInputType(usedInputFormat).setInputFile(inputFile).read(currentEntry);
			for(int i=0; i<currentEntryTemplate.namemap.size(); i++)
			{
				dataCache.index(currentEntry.path, currentEntryTemplate.namemap.getKeyAt(i));
			}

		}
		else if(settingReference)
		{
			inputReader.setInputType(usedInputFormat).setInputFile(inputFile).read(currentEntry);
			for(int i=0; i<currentEntryTemplate.namemap.size(); i++)
			{
				dataCache.index(currentEntry.path, currentEntryTemplate.namemap.getKeyAt(i));
			}
		}
		else
		{
			//USER_INPUT settings
			currentEntryTemplate.memory=true;
			currentEntryTemplate.temporary=false;
			currentEntryTemplate.local=false;
			inputReader.setInputType(usedInputFormat).setInputFile(inputFile).read(currentEntry);
			for(int i=0; i<currentEntryTemplate.namemap.size(); i++)
			{
				dataCache.index(currentEntry.path, currentEntryTemplate.namemap.getKeyAt(i));
			}
		}
	}
	
	
	private void outputDataToFiles() throws InstantiationException, IllegalAccessException, ClassNotFoundException, InvalidFormatException, SQLException, ApplicationException, IOException
	{
		System.out.println("Outputting to files...");
		if(commandLine.hasOption(TextMap.output+"all"))
		{
			outputAllData();
		}
		else if(commandLine.hasOption(TextMap.get))
		{
			outputDataToFile(commandLine.getOptionValue(TextMap.get),null,false,entryTemplate.getValue(commandLine.getOptionValue(TextMap.get)),null);
		}
		else if(commandLine.hasOption(TextMap.output+"_excel"))
		{
			settingOutputFormat=IOType.EXCEL;
			settingOutputFile = new File(commandLine.getOptionValue(TextMap.output+"_excel"));
			outputAllResultData();
		}
		
		printTimeMeasure();
		System.out.println("Outputted files done");
	}
	
	private void outputDataToFile(String datasetName, String filename, boolean appendToExcel, DataEntry currentEntryTemplate, File nof) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException, InvalidFormatException, IOException
	{

		CustomFormatter outputWriter = new CustomFormatter().setDataCache(dataCache).setInputType(IOType.DATACACHE);
		File of;
		if(filename==null)
		{
			if(datasetName==null)
				filename="output";
			else
				filename=datasetName;
		}
		
		if(nof!=null)
		{
			of=nof;
		}
		else if(!settingOutputFile.isDirectory())
		{
			of=settingOutputFile;
		}
		else
		{
			String outputFolderPath="";
			if(settingOutputFile.isDirectory())
				outputFolderPath= settingOutputFile.getAbsolutePath();
			
			if(settingOutputFormat==IOType.CSV)
			{
				of=new File(outputFolderPath+File.separator+filename+".csv");
			}
			else if(settingOutputFormat==IOType.TSV)
			{
				of=new File(outputFolderPath+File.separator+filename+".tsv");
			}
			else if(settingOutputFormat==IOType.EXCEL)
			{
				of=new File(outputFolderPath+File.separator+filename+".xlsx");
			}
			else
			{
				of=new File(outputFolderPath+File.separator+filename+".json");
			}
		}
		
		
		
		
		outputWriter.setPath(datasetName).setOutputType(settingOutputFormat).setOutputFile(of).setExcelAppend(appendToExcel).setOutputSkipEmptyColumns(true);
		DataEntry currentEntry = null;
		if(currentEntryTemplate!=null)
			currentEntry = currentEntryTemplate.copy();
		outputWriter.write(currentEntry);
	}
	
	private void outputAllResultData() throws InstantiationException, IllegalAccessException, ClassNotFoundException, InvalidFormatException, SQLException, ApplicationException, IOException
	{
		Utils.deleteFileIfExistsOldCompatSafe(settingOutputFile);
		
		outputDataToFile("README",null,true, null, settingOutputFile);
		outputDataToFile("USER_INPUT",null,true, entryTemplate.getValue("USER_INPUT"), settingOutputFile);
		outputDataToFile("link_gwas_catalog",null,true, linkEntryTemplate, settingOutputFile);
		outputDataToFile("link_omim",null,true, linkEntryTemplate, settingOutputFile);
		outputDataToFile("link_psychiatric_cnvs",null,true, linkEntryTemplate, settingOutputFile);
		outputDataToFile("link_asd_genes",null,true, linkEntryTemplate, settingOutputFile);
		outputDataToFile("link_id_devdelay_genes",null,true, linkEntryTemplate, settingOutputFile);
		outputDataToFile("link_mouse_knockout",null,true, linkEntryTemplate, settingOutputFile);
	}
	
	private void outputAllData() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, InvalidFormatException, ApplicationException, IOException
	{
		ArrayList<String> datasets =dataCache.listDatasets();
		for(int i=0; i<datasets.size(); i++)
		{
			outputDataToFile(datasets.get(i),null,false,entryTemplate.getValue(datasets.get(i)),null);
		}
	}
	
	/*
	private void performTavernaWorkflow() throws ReaderException, IOException
	{
		WorkflowBundleIO io = new WorkflowBundleIO();
		WorkflowBundle ro = io.readBundle(new File("workflow.t2flow"), null);
		ro.getMainWorkflow();
	}
	*/
	
	
	
	
	private void operate() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException, IOException
	{
		printTimeMeasure();
		System.out.println("Operating...");
		long operationStartTimeNanos = System.nanoTime();
		
		final String schemaName = "PUBLIC";
		String q;
		
		
		
		
		
		
		//operations
		
		//documentation
		File file = new File("documentation.txt");
		FileInputStream fis = new FileInputStream(file);
		byte[] data = new byte[(int) file.length()];
		fis.read(data);
		fis.close();
		//String[] documentation = new String(data, "UTF-8").split("(\n\n|\r\n\r\n)");
		String[] documentation = {new String(data, "UTF-8")};
		DataEntry documentationEntry = dataCache.newEntry("README");
		
		

		for(int i=0; i<documentation.length; i++)
		{
			JSONObject row = new JSONObject();
			JSONObject rowData = new JSONObject();
			JSONObject entry = new JSONObject();
			entry.put("type", java.sql.Types.VARCHAR);
			entry.put("value", documentation[i]);
			//documentationEntry.namemap.put("text",entry);
			rowData.put("text", entry);
			row.put("data", rowData);
			documentationEntry.rows.put(row);
		}
		if(dataCache.getHasTable("README"))
			dataCache.dropTable("README");
		dataCache.enter(documentationEntry).commit();
		
		
		final SQL userInput = new SQL()
		{
			{
				//SELECT("CHR"); //hg19chrc,	r0
				//SELECT("BP1"); //six1, 		r1
				//SELECT("BP2"); //six2, 		r2
				SELECT("_USER_INPUT.*"); //WORK
				SELECT("chr||':'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("bp1"),",", 3)+"||'-'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("bp2"),",", 3)+" AS location");
				//SELECT(dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("bp1"),",", 3)+"||'-'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("bp2"),",", 3)+" AS ll");
				//SELECT("chr||':'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("bp1"),",", 3)+"||'-'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("bp2"),",", 3)+" AS cc");
				SELECT("'HYPERLINK(\"http://genome.ucsc.edu/cgi-bin/hgTracks?&org=Human&db=hg19&position='||chr||'%3A'||"+dataCache.scriptDoubleToVarchar("bp1")+"||'-'||"+dataCache.scriptDoubleToVarchar("bp2")+"||'\",\"ucsc\")' AS UCSC_LINK");
				
				FROM(schemaName+"._USER_INPUT");
				
				//WHERE("(PVALUE>0 AND PVALUE<1e-5) OR PVALUE IS NULL");
				//ORDER_BY("PVALUE,CHR,BP1,BP2");
				ORDER_BY("_IO,CHR,BP1,BP2");
			}
		};
		
		/*
		SQL candidateOuter = new SQL()
		{
			{
				SELECT("ROWNUM() AS RANK,SUB.*");
				FROM("("+candidateInner+") AS SUB");
				ORDER_BY("CHR,BP1,BP2");
			}
		};
		*/
		
		q=userInput.toString();
		dataCache.table("USER_INPUT", q).commit(); //candidate
		dataCache.index("USER_INPUT", "_IO");
		dataCache.index("USER_INPUT", "CHR");
		dataCache.index("USER_INPUT", "BP1");
		dataCache.index("USER_INPUT", "BP2");
		dataCache.index("USER_INPUT", "GENENAME");
		dataCache.index("USER_INPUT", "SNPID");
		dataCache.index("USER_INPUT", "PVALUE");
		
		printTimeMeasure();
		System.out.println("USER_INPUT");
		
		
		//GENE_MASTER EXPANDED
		q=new SQL()
		{
			{
				SELECT("g.*");
				SELECT("(g.bp1-20000) AS bp1s20k_gm"); //expand by 20kb;
				SELECT("(g.bp2+20000) AS bp2a20k_gm");
				SELECT("(g.bp1-10e6) AS bp1s10m_gm"); //expand by 10mb;
				SELECT("(g.bp2+10e6) AS bp2a10m_gm");
				FROM(schemaName+".GENE_MASTER g");
			}
		}.toString();
		dataCache.view("GENE_MASTER_EXPANDED", q).commit();
		
		
		//*====== Candidate genes (all) for bioinformatics ======;
		//*=== Genes: GENCODE v17 genes in bin, expand by 20kb;
		
		q=new SQL()
		{
			{
				//SELECT("c.chr AS chr_in"); //hg19chrc,	r0
				//SELECT("c.bp1 AS bp1_in"); //six1, 		r1
				//SELECT("c.bp2 AS bp2_in"); //six2, 		r2
				SELECT("c.*");
				//SELECT("c.pvalue");
				SELECT("g.chr AS chr_gm");
				SELECT("g.bp1 AS bp1_gm");
				SELECT("g.bp2 AS bp2_gm");
				SELECT("g.genename AS genename_gm");
				SELECT("g.entrez AS entrez_gm");
				SELECT("g.ensembl AS ensembl_gm");
				SELECT("g.strand AS strand_gm");
				
				FROM(schemaName+".USER_INPUT c");
				
				INNER_JOIN(schemaName+".GENE_MASTER_EXPANDED g ON (c.chr=g.chr AND "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp1s20k_gm","g.bp2a20k_gm")+")");
				ORDER_BY("_IO,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("GENES_IN_INTERVAL", q).commit();
		dataCache.index("GENES_IN_INTERVAL", "_IO");
		dataCache.index("GENES_IN_INTERVAL", "chr");
		dataCache.index("GENES_IN_INTERVAL", "bp1");
		dataCache.index("GENES_IN_INTERVAL", "bp2");
		dataCache.index("GENES_IN_INTERVAL", "pvalue");
		dataCache.index("GENES_IN_INTERVAL", "chr_gm");
		dataCache.index("GENES_IN_INTERVAL", "bp1_gm");
		dataCache.index("GENES_IN_INTERVAL", "bp2_gm");
		dataCache.index("GENES_IN_INTERVAL", "genename_gm");
		
		printTimeMeasure();
		System.out.println("GENES_IN_INTERVAL"); //allgenes20kb
		
		//*====== Candidate genes, PC & distance ======;
		//* expand by 10mb;
		
		
		//* join;
		q=new SQL()
		{
			{
				//SELECT("_IO");
				//SELECT("c.chr AS chr_in"); //hg19chrc,	r0
				//SELECT("c.bp1 AS bp1_in"); //six1, 		r1
				//SELECT("c.bp2 AS bp2_in"); //six2, 		r2
				//SELECT("ROWNUM() AS _id");
				//SELECT("c.RANK");
				//SELECT("c.pvalue");
				//SELECT("c.CC");
				//SELECT("c.NUCSC");
				SELECT("c.*");
				SELECT("g.chr AS chr_gm");
				SELECT("g.bp1 AS bp1_gm");
				SELECT("g.bp2 AS bp2_gm");
				SELECT("g.genename AS genename_gm");
				SELECT("g.entrez AS entrez_gm");
				SELECT("g.ensembl AS ensembl_gm");
				SELECT("g.ttype AS ttype_gm");
				SELECT("g.strand AS strand_gm");
				//SELECT("g.STATUS");
				SELECT("g.product AS product_gm");
				SELECT("( CASE WHEN ("+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp1","g.bp2")+") THEN 0 WHEN c.bp1 IS NULL OR c.bp2 IS NULL THEN 9e9 ELSE NUM_MAX_INTEGER(ABS(c.bp1-g.bp2),ABS(c.bp2-g.bp1)) END) dist");
				//SELECT("( CASE WHEN ("+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp1s10m_gm","g.bp2a10m_gm")+") THEN 0 WHEN c.bp1 IS NULL OR c.bp2 IS NULL THEN 9e9 ELSE NUM_MIN_INTEGER(NUM_MIN_INTEGER(ABS(c.bp1-g.bp1),ABS(c.bp2-g.bp1)),NUM_MIN_INTEGER(ABS(c.bp1-g.bp2),ABS(c.bp2-g.bp2))) END) dist");
				FROM(schemaName+".USER_INPUT c");
				INNER_JOIN(schemaName+".GENE_MASTER_EXPANDED g ON (g.ttype='protein_coding' AND c.chr=g.chr AND ("+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp1s10m_gm","g.bp1")+" OR "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp2","g.bp2a10m_gm")+"))");
				//INNER_JOIN(schemaName+".GENE_MASTER_EXPANDED g ON (g.ttype='protein_coding' AND c.chr=g.chr AND "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp1s10m_gm","g.bp2a10m_gm")+")");
				//LEFT_OUTER_JOIN
				ORDER_BY("_IO,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("GENES_PROTEIN_CODING", q).commit();
		dataCache.index("GENES_PROTEIN_CODING", "_IO");
		dataCache.index("GENES_PROTEIN_CODING", "chr");
		dataCache.index("GENES_PROTEIN_CODING", "bp1");
		dataCache.index("GENES_PROTEIN_CODING", "bp2");
		dataCache.index("GENES_PROTEIN_CODING", "pvalue");
		dataCache.index("GENES_PROTEIN_CODING", "chr_gm");
		dataCache.index("GENES_PROTEIN_CODING", "bp1_gm");
		dataCache.index("GENES_PROTEIN_CODING", "bp2_gm");
		dataCache.index("GENES_PROTEIN_CODING", "genename_gm");
		dataCache.index("GENES_PROTEIN_CODING", "ttype_gm");
		dataCache.index("GENES_PROTEIN_CODING", "strand_gm");
		dataCache.index("GENES_PROTEIN_CODING", "dist");
		
		printTimeMeasure();
		System.out.println("GENES_PROTEIN_CODING"); //genesPC10m
		
		//*====== PC genes near======;
		
		q=new SQL()
		{
			{
				SELECT("*");
				SELECT("dist AS r2"); //* for column position;
				FROM(schemaName+".GENES_PROTEIN_CODING");
				WHERE("dist<100000");
				ORDER_BY("_IO,chr,bp1,bp2");
			}
		}.toString();
		dataCache.view("GENES_PROTEIN_CODING_NEAR", q).commit(); //genesPCnear
		
		printTimeMeasure();
		System.out.println("GENES_PROTEIN_CODING_NEAR");
		
		
		
		/* LINKING */
		
		
		
		
		//*=== gwas catalog;
				
		q=new SQL()
		{
			{
				SELECT("c.*");
				//SELECT("c._IO,c.pvalue,c.chr,c.bp1,c.bp2");
				FROM(schemaName+".USER_INPUT c");
				INNER_JOIN(schemaName+".gwas_catalog r ON c.chr=r.chr AND "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1", "c.bp2", "r.bp1", "r.bp2"));
				ORDER_BY("_IO,c.chr,c.bp1,c.bp2");
			}
		}.toString();
		dataCache.table("link_gwas_catalog", q).commit();
		
		printTimeMeasure();
		System.out.println("link_gwas_catalog");
		
		//*=== nhgri gwas;
		/*
		q=new SQL()
		{
			{
				SELECT("c._IO");
				SELECT("c.pvalue");
				SELECT("c.chr");
				SELECT("c.bp1");
				SELECT("c.bp2");
				FROM(schemaName+".candidate c");
				INNER_JOIN(schemaName+".nhgri_gwas r ON c.chr=r.chr AND c.bp1<=r.bp1 AND r.bp1<=c.bp2");
				ORDER_BY("_IO,c.chr,c.bp1,c.bp2");
			}
		}.toString();
		dataCache.table("link_nhgri_gwas", q).commit();
		
		printTimeMeasure();
		System.out.println("link_nhgri_gwas");
		*/
		
		
		//*=== omim;
		q=new SQL()
		{
			{
				SELECT("g.*, r.OMIMgene, r.OMIMDisease, r.type");
				FROM(schemaName+".GENES_PROTEIN_CODING_NEAR g");
				INNER_JOIN(schemaName+".omim r ON g.genename_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''");
				ORDER_BY("_IO,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("link_omim", q).commit();
		
		printTimeMeasure();
		System.out.println("link_omim");
		
		//*=== aut_loci_hg19 ;
		/*
		q=new SQL()
		{
			{
				SELECT("_IO, pvalue, g.geneName_gm, g.product_gm");
				FROM(schemaName+".genesPCnear g");
				INNER_JOIN(schemaName+".aut_loci_hg19 r ON g.geneName_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''");
				ORDER_BY("_IO,chr_in,bp1_in,bp2_in");
			}
		}.toString();
		dataCache.table("link_aut_loci_hg19", q).commit();
		
		printTimeMeasure();
		System.out.println("link_aut_loci_hg19");
		*/
		
		//*=== psych CNVs;
		q=new SQL()
		{
			{
				SELECT("c.*, r.disease, r.type, r.note");
				FROM(schemaName+".USER_INPUT c");
				INNER_JOIN(schemaName+".psychiatric_cnvs r ON c.chr=r.chr AND "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1", "c.bp2", "r.bp1", "r.bp2"));
				ORDER_BY("_IO,c.chr,c.bp1,c.bp2");
			}
		}.toString();
		dataCache.table("link_psychiatric_cnvs", q).commit();
		
		printTimeMeasure();
		System.out.println("link_psychiatric_cnvs");
		
		
		//*=== asd genes;
		q=new SQL()
		{
			{
				SELECT("g.*, r.type");
				FROM(schemaName+".GENES_PROTEIN_CODING_NEAR g");
				INNER_JOIN(schemaName+".asd_genes r ON g.genename_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''");
				ORDER_BY("_IO,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("link_asd_genes", q).commit();
		
		printTimeMeasure();
		System.out.println("link_asd_genes");
		
		
		//*=== id/dev delay ;
		q=new SQL()
		{
			{
				SELECT("g.*");
				FROM(schemaName+".GENES_PROTEIN_CODING_NEAR g");
				INNER_JOIN(schemaName+".id_devdelay_genes r ON g.geneName_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''");
				ORDER_BY("_IO,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("link_id_devdelay_genes", q).commit();
		
		printTimeMeasure();
		System.out.println("link_id_devdelay_genes");
		
		
		//*=== mouse knockout, jax;
		q=new SQL()
		{
			{
				SELECT("g.*, r.musName, r.phenotype");
				FROM(schemaName+".GENES_PROTEIN_CODING_NEAR g");
				INNER_JOIN(schemaName+".mouse_knockout r ON g.geneName_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''");
				ORDER_BY("_IO,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("link_mouse_knockout", q).commit();
		
		printTimeMeasure();
		System.out.println("link_mouse_knockout");
		
		
		
		//*=== g1000 sv;
		/*
		q=new SQL()
		{
			{
				SELECT("_IO, pvalue, c.chr, c.bp1, c.bp2, SVTYPE, EURAF, ASIAF, AFRAF");
				SELECT("(NUM_MIN_INTEGER(c.bp2,r.bp2)-NUM_MAX_INTEGER(c.bp1,r.bp1))/(NUM_MAX_INTEGER(c.bp2,r.bp2)-NUM_MIN_INTEGER(c.bp1,r.bp1)) AS recipoverlap");
				SELECT("'=HYPERLINK(\"http://genome.ucsc.edu/cgi-bin/hgTracks?&org=Human&db=hg19&position='||c.chr||'%3A'||"+dataCache.scriptDoubleToVarchar("c.bp1")+"||'-'||"+dataCache.scriptDoubleToVarchar("c.bp2")+"||'\",\"g1000sv\")' AS g1000sv");
				FROM(schemaName+".candidate c");
				INNER_JOIN(schemaName+".g1000sv r ON euraf>0.01 AND c.chr=r.chr AND "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1", "c.bp2", "r.bp1", "r.bp2"));
				ORDER_BY("_IO,c.chr,c.bp1,c.bp2");
			}
		}.toString();
		dataCache.table("link_g1000sv", q).commit();
		
		printTimeMeasure();
		System.out.println("link_g1000sv");
		*/
		
		//*=== GPCRs;
		/*
		q=new SQL()
		{
			{
				SELECT("_IO");
				SELECT("pvalue");
				SELECT("g.chr_in");
				SELECT("g.bp1_in");
				SELECT("g.bp2_in");
				SELECT("g.r2");		//TODO  ???
				SELECT("g.geneName_gm");
				SELECT("CLASS, FAMILY, r.BP1 AS bp1_r, r.BP2 AS bp2_r, MAPTOTAL, r.PRODUCT");
				FROM(schemaName+".genesPCnear g");
				INNER_JOIN(schemaName+".gproteincoupledreceptors r ON g.geneName_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''");
				ORDER_BY("_IO,chr_in,bp1_in,bp2_in");
			}
		}.toString();
		dataCache.table("link_gproteincoupledreceptors", q).commit();
		
		printTimeMeasure();
		System.out.println("link_gproteincoupledreceptors");
		*/
		
		
		//*=== psych linkage meta;
		/*
		q=new SQL()
		{
			{
				SELECT("_IO, pvalue, c.chr, c.bp1, c.bp2, r.bp1 AS bp1_r, r.bp2 AS bp2_r");
				SELECT("(NUM_MIN_INTEGER(c.bp2,r.bp2)-NUM_MAX_INTEGER(c.bp1,r.bp1))/(NUM_MAX_INTEGER(c.bp2,r.bp2)-NUM_MIN_INTEGER(c.bp1,r.bp1)) AS recipoverlap");
				FROM(schemaName+".candidate c");
				INNER_JOIN(schemaName+".psych_linkage r ON r.study='GWL' AND r.type='MetaAnal' AND c.chr=r.chr AND "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1", "c.bp2", "r.bp1", "r.bp2"));
				ORDER_BY("_IO,c.chr,c.bp1,c.bp2");
			}
		}.toString();
		dataCache.table("link_psychlinkage", q).commit();
		printTimeMeasure();
		System.out.println("link_psychlinkage");
		 */
		System.out.println("Operations done");
		System.out.println("Operations time: "+(System.nanoTime()- operationStartTimeNanos)/1E9+" seconds");
		
	}
	
}
