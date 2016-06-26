package org.ki.meb.tiefighter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
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
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONObject;
import org.jakz.common.ApplicationException;
import org.jakz.common.DataCache;
import org.jakz.common.IndexedMap;
import org.jakz.common.Util;
import org.jakz.common.DataCache.DataEntry;
import org.jakz.common.formatter.CustomFormatter;
import org.jakz.common.formatter.CustomFormatter.IOType;


public class TIEFighter
{
	
	
	
	private static String clHelp = TextMap.help;
	private static String clInputFileFolder = TextMap.input;
	private static String clOutputFileFolder = TextMap.output;
	private static String clReference = "reference";
	private static String clGene = "gene";
	private static String clNonames = "nonames";
	private static String clGet = "get";
	private static String clGetall = "getall";
	private static String clInputFormat = "iformat";
	private static String clOutputFormat = "oformat";
	private static String clOverwrite = "overwrite";
	private static String clOperate= "operate";
	private static String clTimeout = "timeout";
	private static String clDatabaseLocation = "db";
	private static String clConfigFile = "config";
	private static String clTemplate = "template";
	
	private static String confInputfolderpath = clInputFileFolder;
	private static String confOutputfolderpath = clOutputFileFolder;
	private static String confTempfolderpath = "temp";
	private static String confDatabaseCacheSizeKb = "dbcachesizekb";
	
	
	
	private CommandLine commandLine;
	private long startTimeNanos;
	
	private static Options clOptions = new Options();
	
	private File settingConfigFile, settingInputFileFolder, settingOutputFileFolder, settingDBFolder, settingTempFolder, settingDocumentationTemplate;
	private CustomFormatter.IOType settingInputFormat, settingOutputFormat;
	private boolean settingReference, settingGene, settingOverwriteExistingTables, settingFirstRowVariableNames;
	private Integer settingDBCacheSizeKB;
	private DataCache dataCache;
	private FilenameFilter filterExcelXlsx, filterCSV, filterTSV, filterJSON;
	private DataCache.DataEntry referenceEntryTemplate, linkEntryTemplate;
	private IndexedMap<String, DataCache.DataEntry> entryTemplate;
	private IndexedMap<String,XSSFCellStyle> excelStyle;
	
	static
	{
		//clOptions.addOption(OptionBuilder.create(TextMap.regtest)); //inactivated as default
		clOptions.addOption(clHelp,false,"Print usage help.");
		
		clOptions.addOption(OptionBuilder.withArgName("file/folder path").withDescription("Input from the specified file or folder.").hasArg().create(clInputFileFolder));
		clOptions.addOption(OptionBuilder.withArgName("file/folder path").withDescription("Output to the specified file or folder.").hasArg().create(clOutputFileFolder));
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Documentation template for excel output.").hasArg().create(clTemplate));
		clOptions.addOption(clReference,false,"Enter reference data.");
		clOptions.addOption(clGene,false,"Enter gene (reference) data.");
		clOptions.addOption(clNonames,false,"The first row of data in the input files contains NO column names.");
		clOptions.addOption(OptionBuilder.withArgName("dataset name").withDescription("Get specific database content (table/view) as exported output.").hasArg().create(clGet));
		clOptions.addOption(clGetall,false,"Output all database content.");
		
		clOptions.addOption(OptionBuilder.withArgName("format - DATACACHE,EXCEL,CSV,TSV").withDescription("Force output format.").hasArg().create(clOutputFormat));
		clOptions.addOption(OptionBuilder.withArgName("format - DATACACHE,EXCEL,CSV,TSV").withDescription("Force input format.").hasArg().create(clInputFormat));
		clOptions.addOption(OptionBuilder.withArgName("true/false").withDescription("Overwrite existing tables with the same names. Default - true.").hasArg().create(clOverwrite));
		clOptions.addOption(OptionBuilder.withArgName("true/false").withDescription("Perform operation specifics or not. Default - true.").hasArg().create(clOperate));
		clOptions.addOption(OptionBuilder.withArgName("time limit in milliseconds").withDescription("Database connection timeout. Default 30000 milliseconds.").hasArg().create(clTimeout));
		clOptions.addOption(OptionBuilder.withArgName("folder path").withDescription("Database location.").hasArg().create(clDatabaseLocation));
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Config file.").hasArg().create(clConfigFile));
	}

	public TIEFighter()
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
		if(settingConfigFile.exists())
		{
			System.out.println("Using file config from "+settingConfigFile);
			XMLConfiguration config = new XMLConfiguration(settingConfigFile);
			ConfigurationNode rootNode = config.getRootNode();
			if(!rootNode.getChildren(confInputfolderpath).isEmpty())
				settingInputFileFolder = new File((String)((ConfigurationNode)rootNode.getChildren(confInputfolderpath).get(0)).getValue()).getAbsoluteFile();
			if(!rootNode.getChildren(confOutputfolderpath).isEmpty())
				settingOutputFileFolder = new File((String)((ConfigurationNode)rootNode.getChildren(confOutputfolderpath).get(0)).getValue()).getAbsoluteFile();
			if(!rootNode.getChildren(confTempfolderpath).isEmpty())
				settingTempFolder = new File((String)((ConfigurationNode)rootNode.getChildren(confTempfolderpath).get(0)).getValue()).getAbsoluteFile();
			if(!rootNode.getChildren(confDatabaseCacheSizeKb).isEmpty())
				settingDBCacheSizeKB = Integer.parseInt((String)((ConfigurationNode)rootNode.getChildren(confDatabaseCacheSizeKb).get(0)).getValue());
		}
		
		if(settingInputFileFolder==null)
			settingInputFileFolder=new File("input").getAbsoluteFile();
		if(settingOutputFileFolder==null)
			settingOutputFileFolder=new File("output").getAbsoluteFile();
		if(settingDBCacheSizeKB==null)
			settingDBCacheSizeKB=2000000;
		if(settingTempFolder==null)
			settingTempFolder=settingOutputFileFolder;
		//settingDBFolder = new File(Paths.get(".").toAbsolutePath().normalize().toString()); //not for older java
		settingDBFolder = new File("");
		
		settingInputFormat=null;
		settingOutputFormat=IOType.CSV;
		settingReference=false;
		settingGene=false;
		settingFirstRowVariableNames=true;
		
		if(commandLine.hasOption(clDatabaseLocation))
		{
			settingDBFolder=new File(commandLine.getOptionValue(clDatabaseLocation));
		}
		
		//dataCache=new DataCache("./TIEFighter");
		String path = settingDBFolder.getAbsolutePath()+File.separator+"TIEFighter";
		dataCache=new DataCache(path);
		
		settingDocumentationTemplate = new File("documentation.xlsx"); //default
		if(commandLine.hasOption(clTemplate))
		{
			settingDocumentationTemplate=new File(commandLine.getOptionValue(clTemplate));
		}
		
		if(commandLine.hasOption(clInputFileFolder))
		{
			settingInputFileFolder=new File(commandLine.getOptionValue(clInputFileFolder));
		}
		
		if(commandLine.hasOption(clOutputFileFolder))
		{
			settingOutputFileFolder=new File(commandLine.getOptionValue(clOutputFileFolder));
		}
		
		
		if(commandLine.hasOption(clInputFormat))
		{
			String ov = commandLine.getOptionValue(clInputFormat).toUpperCase();
			try
			{
				settingInputFormat=IOType.valueOf(ov);
			}
			catch (Exception e)
			{
				throw new ApplicationException("Input format error. Provided:"+ov,e);
			}
		}
		
		if(commandLine.hasOption(clOutputFormat))
		{
			String ov = commandLine.getOptionValue(clOutputFormat).toUpperCase();
			try
			{
				settingOutputFormat=IOType.valueOf(ov);
			}
			catch (Exception e)
			{
				throw new ApplicationException("Output format error. Provided:"+ov,e);
			}
		}
		
		if(commandLine.hasOption(clReference))
		{
			settingReference=true;
		}
		
		if(commandLine.hasOption(clGene))
		{
			settingGene=true;
		}
		
		if(commandLine.hasOption(clNonames))
		{
			settingFirstRowVariableNames=false;
		}
		
		settingOverwriteExistingTables=true;
		if(commandLine.hasOption(clOverwrite))
		{
			settingOverwriteExistingTables=Boolean.parseBoolean(commandLine.getOptionValue(clOverwrite));
		}
		
		if(commandLine.hasOption(clTimeout))
		{
			dataCache.setConnectionTimeoutMilliseconds(Long.parseLong(commandLine.getOptionValue(clTimeout)));
		}
		
		//tempfiles - for poi
		System.out.println("Tempfolder was: "+System.getProperty("java.io.tmpdir"));
		System.setProperty("java.io.tmpdir", settingTempFolder.getAbsolutePath());
		System.out.println("Tempfolder is now set to: "+System.getProperty("java.io.tmpdir"));
		
		
		//formalized entry templates, names in UPPER CASE!!!!
		DataEntry ne; JSONObject element;
		entryTemplate=new IndexedMap<String, DataCache.DataEntry>();
				
		ne=dataCache.newEntry("_USER_INPUT"); //WORK
		ne.local=true;
		//ne.temporary=true;
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("INPUTID",element);

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
		ne.local=true;
		
		element=new JSONObject();
		element.put("type", java.sql.Types.INTEGER);
		ne.namemap.put("INPUTID",element);

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
		element.put("isHyperlink", true);
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
		ne.namemap.put("INPUTID",element);

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
		element.put("isHyperlink", true);
		ne.namemap.put("UCSC_LINK",element);
		
		linkEntryTemplate=ne;
		
		excelStyle = new IndexedMap<String, XSSFCellStyle>();
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
			commandLine = parser.parse(clOptions, new String[]{"-"+clHelp});
		}
		return commandLine;
	}
	
	
	public static void main(String[] args) throws Exception
	{
		System.out.println("//¤//TIEFighter//¤//");
		new TIEFighter().setCommandLine(constructCommandLine(args)).runCommands();
	}
	
	
	private TIEFighter runCommands() throws Exception
	{
		if(commandLine.hasOption(TextMap.help) || commandLine.getOptions().length==0
				//||(!commandLine.hasOption(TextMap.config)&&!commandLine.hasOption("reference")&&!commandLine.hasOption(TextMap.operate)&&!commandLine.hasOption(TextMap.get)&&!commandLine.hasOption("getall"))
				)
		{
			printHelp();
			return this;
		}
		
		if(commandLine.hasOption(clConfigFile))
		{
			settingConfigFile = new File(commandLine.getOptionValue(clConfigFile));
		}
		else
		{
			settingConfigFile = new File("config.xml");
		}
		
		init();
		
		System.out.println("Waiting for database connection...");
		dataCache.createCacheConnectionEmbedded();
		System.out.println("Database connected");
		dataCache.setDBCacheSizeKB(settingDBCacheSizeKB);
		dataCache.commit();
		
		//if((!commandLine.hasOption(TextMap.operate)&&!settingReference&&!settingGene)||Boolean.parseBoolean(commandLine.getOptionValue(TextMap.operate))==true)
		if(commandLine.hasOption(clInputFileFolder))
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
		formatter.printHelp("java -jar \"TIEFighter.jar\"", "", clOptions, "", true);
	}
	
	public TIEFighter setCommandLine(CommandLine nCommandLine)
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
		
		if(settingInputFileFolder.isFile())
		{
			 inputDataFromFile(settingInputFileFolder, settingInputFormat, inputReader, currentEntryTemplate);
		}
		else if(settingInputFileFolder.isDirectory())
		{
			
			//import all files in input
			if(settingInputFormat==null)
			{
				File[] inputFilesJSON = settingInputFileFolder.listFiles(filterJSON);
				for(int iFile=0; iFile<inputFilesJSON.length; iFile++)
				{
					inputDataFromFile(inputFilesJSON[iFile], IOType.DATACACHE, inputReader, currentEntryTemplate);
				}
				
				File[] inputFilesCsv = settingInputFileFolder.listFiles(filterCSV);
				for(int iFile=0; iFile<inputFilesCsv.length; iFile++)
				{
					inputDataFromFile(inputFilesCsv[iFile], IOType.CSV, inputReader, currentEntryTemplate);
				}
				
				File[] inputFilesTsv = settingInputFileFolder.listFiles(filterTSV);
				for(int iFile=0; iFile<inputFilesTsv.length; iFile++)
				{
					inputDataFromFile(inputFilesTsv[iFile], IOType.TSV, inputReader, currentEntryTemplate);
				}
				
				File[] inputFilesXlsx = settingInputFileFolder.listFiles(filterExcelXlsx);
				for(int iFile=0; iFile<inputFilesXlsx.length; iFile++)
				{
					inputDataFromFile(inputFilesXlsx[iFile], IOType.EXCEL, inputReader, currentEntryTemplate);
				}
			}
			else
			{
				File[] inputFiles = settingInputFileFolder.listFiles();
				for(int iFile=0; iFile<inputFiles.length; iFile++)
				{
					try 
					{
						inputDataFromFile(inputFiles[iFile], settingInputFormat, inputReader, currentEntryTemplate);
					}
					catch (Exception e)
					{
						System.err.println("Failed to parse file "+inputFiles[iFile].getAbsolutePath()+".\nReason:\n"+Util.getStackTraceString(e));
					}
				}
			}
			
		}
		else throw new ApplicationException("Wrong type of input; it is not a file nor a directory.");
		
		dataCache.commit();
	}
	
	private void inputDataFromFile(File inputFile, IOType usedInputFormat, CustomFormatter inputReader, DataEntry currentEntryTemplate) throws InvalidFormatException, IOException, ApplicationException, SQLException
	{
		if(inputFile.isDirectory())
			throw new ApplicationException("Can't input data from directory.");
		
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
			//custom naming
			currentEntry.path=inputFile.getName();
			int dotIndex = currentEntry.path.lastIndexOf('.');
			if(dotIndex>=0)
				currentEntry.path="_"+currentEntry.path.substring(0,dotIndex).replace('.', '_');
			
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
		
		if(commandLine.hasOption(clGetall))
		{
			System.out.println("Outputting to files...");
			outputAllData();
			System.out.println("Outputted files done");
		}
		else if(commandLine.hasOption(clGet))
		{
			System.out.println("Outputting to file...");
			outputDataToFile(commandLine.getOptionValue(TextMap.get),null,false,entryTemplate.getValue(commandLine.getOptionValue(TextMap.get)),null);
			System.out.println("Outputted file done");
		}
		else if(commandLine.hasOption(clOutputFileFolder)||commandLine.hasOption(clOutputFormat))
		{
			//Outputting result data
			System.out.println("Outputting to file...");
			outputAllResultData();
			System.out.println("Outputted file done");
		}
		
		printTimeMeasure();
		
	}
	
	private void outputDataToFile(String datasetName, String filename, boolean appendToExcel, DataEntry currentEntryTemplate, File nof) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException, InvalidFormatException, IOException
	{

		CustomFormatter outputWriter = new CustomFormatter().setDataCache(dataCache).setInputType(IOType.DATACACHE).setExcelStyle(excelStyle);
		File of;
		if(filename==null)
		{
			if(datasetName==null)
				filename="output";
			else
				filename=datasetName;
		}
		
		if(nof!=null&&!nof.isDirectory())
		{
			of=nof;
		}
		else if(!settingOutputFileFolder.isDirectory())
		{
			of=settingOutputFileFolder;
		}
		else
		{
			String outputFolderPath="";
			if(settingOutputFileFolder.isDirectory())
				outputFolderPath= settingOutputFileFolder.getAbsolutePath()+File.separator;
			
			if(settingOutputFormat==IOType.CSV)
			{
				of=new File(outputFolderPath+filename+".csv");
			}
			else if(settingOutputFormat==IOType.TSV)
			{
				of=new File(outputFolderPath+filename+".tsv");
			}
			else if(settingOutputFormat==IOType.EXCEL)
			{
				of=new File(outputFolderPath+filename+".xlsx");
			}
			else
			{
				of=new File(outputFolderPath+filename+".json");
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
		
		//excel composite file as standard
		if(!commandLine.hasOption(clOutputFormat)||settingOutputFormat==IOType.EXCEL)
		{
			String filename = clOutputFileFolder+"_excel";
			settingOutputFormat=IOType.EXCEL;
		
			if(!commandLine.hasOption(clOutputFileFolder))
			{
				if(settingInputFileFolder!=null&&!settingInputFileFolder.isDirectory())
				{
					filename = settingInputFileFolder.getName();
					String outputFolderPath="";
					if(settingOutputFileFolder.isDirectory())
						outputFolderPath= settingOutputFileFolder.getAbsolutePath()+File.separator;
					if(filename.indexOf('.')>=0)
						filename = filename.substring(0,filename.lastIndexOf("."));
					
					settingOutputFileFolder = new File(outputFolderPath+filename+".xlsx");
				}
			}
			
			
		}
		
		if(!settingOutputFileFolder.isDirectory())
			Util.deleteFileIfExistsOldCompatSafe(settingOutputFileFolder);
		
		
		//append documentation/READ ME
		if(settingDocumentationTemplate.exists() && !settingDocumentationTemplate.isDirectory())
		{
			System.out.println("Importing documentation...");
			XSSFWorkbook documentationExcelWorkbook = new XSSFWorkbook(settingDocumentationTemplate);
			XSSFWorkbook outputFileWorkbook = new XSSFWorkbook();
			XSSFSheet readMeSheetSource = documentationExcelWorkbook.getSheet("readme");
			XSSFSheet readMeSheetTarget  = outputFileWorkbook.createSheet("README");
			
			//Style copy
			for(int i=0; i<documentationExcelWorkbook.getNumCellStyles();i++)
			{
				String styleCallsign = "read_me"+i;
				XSSFCellStyle newCellStyle;
				newCellStyle = outputFileWorkbook.createCellStyle();
				newCellStyle.cloneStyleFrom(documentationExcelWorkbook.getCellStyleAt(i));
				excelStyle.put(styleCallsign,newCellStyle);
			}
			
			//Iterator<Row> iRow = readMeSheetSource.rowIterator();
			int irow =0;
			for (Row crow : readMeSheetSource)
			{
				XSSFRow trow = readMeSheetTarget.createRow(irow++);
				for(Cell ccell : crow)
				{
					
					XSSFCell tcell = trow.createCell(ccell.getColumnIndex());
					int type = ccell.getCellType();
					tcell.setCellType(ccell.getCellType());
					//tcell.setCellStyle(ccell.getCellStyle()); //not working
					int cindex = ccell.getCellStyle().getIndex();
					//System.out.println("Style index: "+cindex);
					
					XSSFCellStyle newCellStyle;
					newCellStyle=excelStyle.getValueAt(cindex);
					tcell.setCellStyle(newCellStyle);
					
					if(type==XSSFCell.CELL_TYPE_BLANK||type==XSSFCell.CELL_TYPE_ERROR)
					{
						//nothing
					}
					else if(type==Cell.CELL_TYPE_BOOLEAN || (type==Cell.CELL_TYPE_FORMULA && ccell.getCachedFormulaResultType()==Cell.CELL_TYPE_BOOLEAN))
					{
						tcell.setCellValue(ccell.getBooleanCellValue());
					}
					else if(type==Cell.CELL_TYPE_NUMERIC || (type==Cell.CELL_TYPE_FORMULA && ccell.getCachedFormulaResultType()==Cell.CELL_TYPE_NUMERIC))
					{
						if (DateUtil.isCellDateFormatted(ccell))
							tcell.setCellValue(ccell.getDateCellValue());
						else 
							tcell.setCellValue(ccell.getNumericCellValue());
					}
					else if(type==Cell.CELL_TYPE_STRING || (type==Cell.CELL_TYPE_FORMULA && ccell.getCachedFormulaResultType()==Cell.CELL_TYPE_STRING))
					{
						tcell.setCellValue(ccell.getRichStringCellValue());
					}
					
				}
			}
			
			//column sizes
			for(int i=0; i<10;i++)
			{
				readMeSheetTarget.autoSizeColumn(i);
			}
			//System.out.println("Num styles after read me import:"+outputFileWorkbook.getNumCellStyles());
			FileOutputStream fileOut = new FileOutputStream(settingOutputFileFolder);
			outputFileWorkbook.write(fileOut);
		    fileOut.close();
			//outputFileWorkbook.close(); //unclear if we need to close these
			//documentationExcelWorkbook.close();
			System.out.println("Documentation import done. Continuing with outputting files...");
		}
		
		//outputDataToFile("README",null,true, null, settingOutputFileFolder);
		outputDataToFile("USER_INPUT",null,true, entryTemplate.getValue("USER_INPUT"), settingOutputFileFolder);
		outputDataToFile("GENES_PROTEIN_CODING_NEAR",null,true, linkEntryTemplate, settingOutputFileFolder);
		outputDataToFile("gwas_catalog",null,true, linkEntryTemplate, settingOutputFileFolder);
		outputDataToFile("omim",null,true, linkEntryTemplate, settingOutputFileFolder);
		outputDataToFile("psychiatric_cnvs",null,true, linkEntryTemplate, settingOutputFileFolder);
		outputDataToFile("asd_genes",null,true, linkEntryTemplate, settingOutputFileFolder);
		outputDataToFile("id_devdelay_genes",null,true, linkEntryTemplate, settingOutputFileFolder);
		outputDataToFile("mouse_knockout",null,true, linkEntryTemplate, settingOutputFileFolder);
		
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
		/*
		File documentationTxt = new File("documentation.txt");
		FileInputStream fis = new FileInputStream(documentationTxt);
		byte[] data = new byte[(int) documentationTxt.length()];
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
		*/
		
		dataCache.index("_USER_INPUT", "INPUTID");
		dataCache.index("_USER_INPUT", "CHR");
		dataCache.index("_USER_INPUT", "BP1");
		dataCache.index("_USER_INPUT", "BP2");
		dataCache.index("_USER_INPUT", "GENENAME");
		dataCache.index("_USER_INPUT", "SNPID");
		dataCache.index("_USER_INPUT", "PVALUE");
		
		
		
		final SQL userInput = new SQL()
		{
			{
				//SELECT("CHR"); //hg19chrc,	r0
				//SELECT("BP1"); //six1, 		r1
				//SELECT("BP2"); //six2, 		r2
				SELECT("_USER_INPUT.*"); //WORK
				SELECT("chr||':'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("bp1"),",", 3)+"||'-'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("bp2"),",", 3)+" AS location");
				SELECT("'HYPERLINK(\"http://genome.ucsc.edu/cgi-bin/hgTracks?&org=Human&db=hg19&position='||chr||'%3A'||"+dataCache.scriptDoubleToVarchar("bp1")+"||'-'||"+dataCache.scriptDoubleToVarchar("bp2")+"||'\",\"ucsc\")' AS UCSC_LINK");
				FROM(schemaName+"._USER_INPUT");
				ORDER_BY("INPUTID,CHR,BP1,BP2");
			}
		};
		
		q=userInput.toString();
		dataCache.table("USER_INPUT", q).commit(); //candidate
		dataCache.index("USER_INPUT", "INPUTID");
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
		
		/*
		//*====== Candidate genes (all) for bioinformatics ======;
		//*=== Genes: GENCODE v17 genes in bin, expand by 20kb;
		
		q=new SQL()
		{
			{
				//SELECT("c.chr AS chr_in"); //hg19chrc,	r0
				//SELECT("c.bp1 AS bp1_in"); //six1, 		r1
				//SELECT("c.bp2 AS bp2_in"); //six2, 		r2
				SELECT("c.*");
				SELECT("g.chr AS chr_gm");
				SELECT("g.bp1 AS bp1_gm");
				SELECT("g.bp2 AS bp2_gm");
				SELECT("g.genename AS genename_gm");
				SELECT("g.entrez AS entrez_gm");
				SELECT("g.ensembl AS ensembl_gm");
				SELECT("g.strand AS strand_gm");
				
				FROM(schemaName+".USER_INPUT c");
				
				INNER_JOIN(schemaName+".GENE_MASTER_EXPANDED g ON (c.chr=g.chr AND "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp1s20k_gm","g.bp2a20k_gm")+")");
				ORDER_BY("INPUTID,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("GENES_IN_INTERVAL", q).commit();
		dataCache.index("GENES_IN_INTERVAL", "INPUTID");
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
		*/
		
		//*====== Candidate genes, PC & distance ======;
		//* expand by 10mb;
		
		
		//* join;
		q=new SQL()
		{
			{
				SELECT("c.*");
				SELECT("g.genename AS genename_gm");
				SELECT("( CASE WHEN ("+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp1","g.bp2")+") THEN 0 WHEN c.bp1 IS NULL OR c.bp2 IS NULL THEN 9e9 ELSE NUM_MAX_INTEGER(ABS(c.bp1-g.bp2),ABS(c.bp2-g.bp1)) END) dist");
				FROM(schemaName+"._USER_INPUT c");
				//INNER_JOIN(schemaName+".GENE_MASTER_EXPANDED g ON (g.ttype='protein_coding' AND c.chr=g.chr AND ("+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp1s10m_gm","g.bp1")+" OR "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp2","g.bp2a10m_gm")+"))");
				INNER_JOIN(schemaName+".GENE_MASTER_EXPANDED g ON (g.ttype='protein_coding' AND c.chr=g.chr AND "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1","c.bp2","g.bp1s10m_gm","g.bp2a10m_gm")+")");
				ORDER_BY("INPUTID,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("GENES_PROTEIN_CODING", q).commit();
		dataCache.index("GENES_PROTEIN_CODING", "INPUTID");
		dataCache.index("GENES_PROTEIN_CODING", "chr");
		dataCache.index("GENES_PROTEIN_CODING", "bp1");
		dataCache.index("GENES_PROTEIN_CODING", "bp2");
		dataCache.index("GENES_PROTEIN_CODING", "pvalue");
		dataCache.index("GENES_PROTEIN_CODING", "genename_gm");
		
		printTimeMeasure();
		System.out.println("GENES_PROTEIN_CODING"); //genesPC10m
		
		//*====== PC genes near======;
		
		q=new SQL()
		{
			{
				SELECT("g.*");
				FROM(schemaName+".GENES_PROTEIN_CODING g");
				WHERE("dist<100000");
				ORDER_BY("INPUTID,chr,bp1,bp2");
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
				SELECT("c.*, r.bp1 AS bp1_gwc, r.bp2 AS bp2_gwc, r.snpid AS snpid_gwc, r.pvalue AS pvalue_gwc, r.pmid, r.trait");
				FROM(schemaName+"._USER_INPUT c");
				INNER_JOIN(schemaName+"._gwas_catalog r ON c.chr=r.chr AND "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1", "c.bp2", "r.bp1", "r.bp2"));
				ORDER_BY("INPUTID,c.chr,c.bp1,c.bp2");
			}
		}.toString();
		dataCache.table("gwas_catalog", q).commit();
		
		printTimeMeasure();
		System.out.println("gwas_catalog");
		
		
		//*=== omim;
		q=new SQL()
		{
			{
				SELECT("g.*, r.OMIMgene, r.OMIMDisease, r.type");
				FROM(schemaName+".GENES_PROTEIN_CODING_NEAR g");
				INNER_JOIN(schemaName+"._omim r ON g.genename_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''");
				ORDER_BY("INPUTID,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("omim", q).commit();
		
		printTimeMeasure();
		System.out.println("omim");
		
		//*=== psych CNVs;
		q=new SQL()
		{
			{
				SELECT("c.*, r.disease, r.type, r.note");
				FROM(schemaName+"._USER_INPUT c");
				INNER_JOIN(schemaName+"._psychiatric_cnvs r ON c.chr=r.chr AND "+dataCache.scriptTwoSegmentOverlapCondition("c.bp1", "c.bp2", "r.bp1", "r.bp2"));
				ORDER_BY("INPUTID,c.chr,c.bp1,c.bp2");
			}
		}.toString();
		dataCache.table("psychiatric_cnvs", q).commit();
		
		printTimeMeasure();
		System.out.println("psychiatric_cnvs");
		
		
		//*=== asd genes;
		q=new SQL()
		{
			{
				SELECT("g.*, r.type");
				FROM(schemaName+".GENES_PROTEIN_CODING_NEAR g");
				INNER_JOIN(schemaName+"._asd_genes r ON g.genename_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''");
				ORDER_BY("INPUTID,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("asd_genes", q).commit();
		
		printTimeMeasure();
		System.out.println("asd_genes");
		
		
		//*=== id/dev delay ;
		q=new SQL()
		{
			{
				SELECT("g.*");
				FROM(schemaName+".GENES_PROTEIN_CODING_NEAR g");
				INNER_JOIN(schemaName+"._id_devdelay_genes r ON g.geneName_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''");
				ORDER_BY("INPUTID,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("id_devdelay_genes", q).commit();
		
		printTimeMeasure();
		System.out.println("id_devdelay_genes");
		
		
		//*=== mouse knockout, jax;
		q=new SQL()
		{
			{
				SELECT("g.*, r.musName, r.phenotype");
				FROM(schemaName+".GENES_PROTEIN_CODING_NEAR g");
				INNER_JOIN(schemaName+"._mouse_knockout r ON g.geneName_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''");
				ORDER_BY("INPUTID,chr,bp1,bp2");
			}
		}.toString();
		dataCache.table("mouse_knockout", q).commit();
		
		printTimeMeasure();
		System.out.println("mouse_knockout");
		
		
		System.out.println("Operations done");
		System.out.println("Operations time: "+(System.nanoTime()- operationStartTimeNanos)/1E9+" seconds");
		
	}
	
}
