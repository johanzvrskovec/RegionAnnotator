package org.ki.meb.geneconnector;
import java.io.File;
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
import org.ki.meb.common.ApplicationException;
import org.ki.meb.common.DataCache;
import org.ki.meb.common.DataCache.DataEntry;
import org.ki.meb.common.formatter.CustomFormatter;
import org.ki.meb.common.formatter.CustomFormatter.IOType;

//import getl.proc.Flow;

public class GwasBioinf //extends ParallelWorker
{
	
	private CommandLine commandLine;
	
	private static Options clOptions = new Options();
	
	private File settingConfigFile, settingInputFile, settingOutputFile;
	private CustomFormatter.IOType settingInputFormat, settingOutputFormat;
	private boolean settingReference, settingGene, settingOverwriteExistingTables;
	private DataCache dataCache;
	private FilenameFilter filterExcelXlsx, filterCSV, filterTSV, filterJSON;
	
	static
	{
		//clOptions.addOption(OptionBuilder.create(TextMap.regtest)); //inactivated as default
		clOptions.addOption(TextMap.help,false,"Print usage help.");
		
		clOptions.addOption(OptionBuilder.withArgName("file/folder path").withDescription("Input from the specified file or folder.").hasArg().create(TextMap.input));
		clOptions.addOption("reference",false,"Enter as reference data");
		clOptions.addOption("gene",false,"Enter as gene data");
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Output to the specified file - Excel").hasArg().create(TextMap.output+"_text"));
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Output to the specified file - Excel").hasArg().create(TextMap.output+"_excel"));
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Output to the specified file - Excel").hasArg().create(TextMap.output+"_datacache"));
		clOptions.addOption(OptionBuilder.withArgName("dataset name").withDescription("Get specific database content (table/view) as exported output.").hasArg().create(TextMap.get));
		clOptions.addOption(TextMap.output+"all",false,"Output all database content.");
		
		clOptions.addOption(OptionBuilder.withArgName("format - csv(default), tsv, datacache, excel").withDescription("Force output format.").hasArg().create("of"));
		clOptions.addOption(OptionBuilder.withArgName("format - csv(default), tsv, datacache, excel").withDescription("Force input format.").hasArg().create("if"));
		clOptions.addOption(OptionBuilder.withArgName("true/false").withDescription("Overwrite existing tables with the same names. Default - true for work and reference, false for gene").hasArg().create("overwrite"));
		clOptions.addOption(OptionBuilder.withArgName("true/false").withDescription("Perform operation specifics or not. Default - true.").hasArg().create(TextMap.operate));
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Config file").hasArg().create(TextMap.config));
		
		//clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Path to the folder of the input files used for initiation").hasArg().create("ipath"));
		
		
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

	private void init() throws ConfigurationException, ApplicationException
	{
		XMLConfiguration config = new XMLConfiguration(settingConfigFile);
		ConfigurationNode rootNode = config.getRootNode();
		settingInputFile = new File((String)((ConfigurationNode)rootNode.getChildren(TextMap.inputfolderpath).get(0)).getValue());
		settingOutputFile = new File((String)((ConfigurationNode)rootNode.getChildren(TextMap.outputfolderpath).get(0)).getValue());
		dataCache=new DataCache("./GwasBioinf");
		settingInputFormat=null;
		settingOutputFormat=IOType.CSV;
		settingReference=false;
		settingGene=false;
		
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
		
		settingOverwriteExistingTables=true;
		if(commandLine.hasOption("overwrite"))
		{
			settingOverwriteExistingTables=Boolean.parseBoolean(commandLine.getOptionValue("overwrite"));
		}
		//else if(settingGene)
			//settingOverwriteExistingTables=false;
		
		
		
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
		if(commandLine.hasOption(TextMap.help)
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
		
		
		inputDataFromFiles();
		
		
		if((!commandLine.hasOption(TextMap.operate)&&!settingReference&&!settingGene)||Boolean.parseBoolean(commandLine.getOptionValue(TextMap.operate))==true)
		{
			operate();
		}
		
		outputDataToFiles();
		
		dataCache.shutdownCacheConnection();
		
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
		IOType usedInputFormat = settingInputFormat;
			
		CustomFormatter inputReader = new CustomFormatter().setDataCache(dataCache).setOverwriteExistingTables(settingOverwriteExistingTables);
		
		String pathToUse = "WORK"; //for standard input
		if(settingGene)
			pathToUse="GENE_MASTER";
		else if(settingReference)
			pathToUse=null;
		
		inputReader.setPath(pathToUse);
		
		if(settingInputFile.isFile())
		{
			if(usedInputFormat==null)
			{
				if(settingInputFile.getName().toLowerCase().matches("^.+\\.xlsx$"))
				{
					usedInputFormat=IOType.EXCEL;
				}
				else if(settingInputFile.getName().toLowerCase().matches("^.+\\.csv$"))
				{
					usedInputFormat=IOType.CSV;
				}
				else if(settingInputFile.getName().toLowerCase().matches("^.+\\.tsv$"))
				{
					usedInputFormat=IOType.TSV;
				}
				else if(settingInputFile.getName().toLowerCase().matches("^.+\\.json$"))
				{
					usedInputFormat=IOType.DATACACHE;
				}
			}
			
			if(settingGene)
			{
				inputReader.setInputType(usedInputFormat).setInputFile(settingInputFile).read();
				dataCache.index("GENE_MASTER", "f1");
				dataCache.index("GENE_MASTER", "f4");
				dataCache.index("GENE_MASTER", "f5");
				dataCache.index("GENE_MASTER", "f7");
				dataCache.index("GENE_MASTER", "genename");
			}
			else if(settingReference)
			{
				inputReader.setInputType(usedInputFormat).setInputFile(settingInputFile).read();
			}
			else
			{
				//WORK settings
				DataEntry entryTemplate = dataCache.newEntry(pathToUse);
				entryTemplate.memory=true;
				entryTemplate.temporary=false;
				entryTemplate.local=false;
				inputReader.setInputType(usedInputFormat).setInputFile(settingInputFile).read(entryTemplate);
				dataCache.index("WORK", "RANK");
				dataCache.index("WORK", "six1");
				dataCache.index("WORK", "six2");
				dataCache.index("WORK", "r1");
				dataCache.index("WORK", "r2");
			}
				
				
		}
		else if(settingInputFile.isDirectory())
		{
			
			//import all files in input
			File[] inputFilesJSON = settingInputFile.listFiles(filterJSON);
			for(int iFile=0; iFile<inputFilesJSON.length; iFile++)
			{
				inputReader.setInputType(IOType.DATACACHE).setInputFile(inputFilesJSON[iFile]).read();
			}
			
			File[] inputFilesCsv = settingInputFile.listFiles(filterCSV);
			for(int iFile=0; iFile<inputFilesCsv.length; iFile++)
			{
				inputReader.setInputType(IOType.CSV).setInputFile(inputFilesCsv[iFile]).read();
			}
			
			File[] inputFilesTsv = settingInputFile.listFiles(filterTSV);
			for(int iFile=0; iFile<inputFilesTsv.length; iFile++)
			{
				inputReader.setInputType(IOType.TSV).setInputFile(inputFilesTsv[iFile]).read();
			}
			
			File[] inputFilesXlsx = settingInputFile.listFiles(filterExcelXlsx);
			for(int iFile=0; iFile<inputFilesXlsx.length; iFile++)
			{
				inputReader.setInputType(IOType.EXCEL).setInputFile(inputFilesXlsx[iFile]).read();
			}
			
		}
		else throw new ApplicationException("Wrong type of input; it is not a file nor a directory.");
		
		dataCache.commit();
	}
	
	
	private void outputDataToFiles() throws InstantiationException, IllegalAccessException, ClassNotFoundException, InvalidFormatException, SQLException, ApplicationException, IOException
	{
		if(commandLine.hasOption(TextMap.output+"all"))
		{
			outputAllData();
		}
		else if(commandLine.hasOption(TextMap.get))
		{
			outputData(commandLine.getOptionValue(TextMap.get),null);
		}
		else if(commandLine.hasOption(TextMap.output+"_text"))
		{
			settingOutputFormat=IOType.CSV;
			settingOutputFile = new File(commandLine.getOptionValue(TextMap.output+"_text"));
			outputData("WORK",null);
		}
		else if(commandLine.hasOption(TextMap.output+"_excel"))
		{
			settingOutputFormat=IOType.EXCEL;
			settingOutputFile = new File(commandLine.getOptionValue(TextMap.output+"_excel"));
			outputData("WORK",null);
		}
		else if(commandLine.hasOption(TextMap.output+"_datacache"))
		{
			settingOutputFormat=IOType.DATACACHE;
			settingOutputFile = new File(commandLine.getOptionValue(TextMap.output+"_datacache"));
			outputData("WORK",null);
		}
	}
	
	private void outputData(String datasetName, String filename) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException, InvalidFormatException, IOException
	{

		CustomFormatter outputWriter = new CustomFormatter().setDataCache(dataCache).setInputType(IOType.DATACACHE);
		File of;
		if(filename==null)
			filename=datasetName;
		
		if(filename==null&&settingOutputFile.isFile())
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
		
		outputWriter.setPath(datasetName);
		outputWriter.setOutputType(settingOutputFormat);
		outputWriter.setOutputFile(of);
		outputWriter.write();
	}
	
	private void outputAllData() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException, InvalidFormatException, ApplicationException, IOException
	{
		ArrayList<String> datasets =dataCache.listDatasets();
		for(int i=0; i<datasets.size(); i++)
		{
			outputData(datasets.get(i),null);
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
	
	
	
	
	private void operate() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException
	{
		
		
		
		
		//String schemaName = "APP"; 
		final String schemaName = "PUBLIC";
		
		String q;
		
		//operations
		final SQL candidateInner = new SQL()
		{
			{
				SELECT("RANK AS RANK_MDD2");
				SELECT("SNPID");
				SELECT("A1A2");
				SELECT("OR");
				SELECT("P");
				SELECT("UCSC");
				SELECT("hg19chrc");
				SELECT("CHR");
				SELECT("BP");
				SELECT("ONE1");
				SELECT("ONE2");
				SELECT("SIX1");
				SELECT("SIX2");
				SELECT("SE");
				SELECT("FRQ_A_44194");
				SELECT("FRQ_U_110741");
				SELECT("INFO");
				SELECT("NGT");
				SELECT(dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("six1"),",", 3)+"||'-'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("six2"),",", 3)+" AS ll");
				SELECT("hg19chrc AS r0");
				SELECT("six1 AS r1");
				SELECT("six2 AS r2");
				SELECT("hg19chrc||':'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("six1"),",", 3)+"||'-'||"+dataCache.scriptSeparateFixedSpacingRight(dataCache.scriptDoubleToVarchar("six2"),",", 3)+" AS cc");
				SELECT("'=HYPERLINK(\"http://genome.ucsc.edu/cgi-bin/hgTracks?&org=Human&db=hg19&position='||hg19chrc||'%3A'||"+dataCache.scriptDoubleToVarchar("six1")+"||'-'||"+dataCache.scriptDoubleToVarchar("six2")+"||'\",\"ucsc\")' AS nucsc");
				
				FROM(schemaName+".WORK");
				
				WHERE("P>0 AND P<1e-1");
				ORDER_BY("P");
			}
		};
		
		SQL candidateOuter = new SQL()
		{
			{
				SELECT("ROWNUM() AS RANK, SUB.*");
				FROM("("+candidateInner+") AS SUB");
			}
		};
		
		q=candidateOuter.toString();
		dataCache.view("candidate", q).commit();
		
		//*====== genes for bioinformatics ======;
		//*=== Genes: GENCODE v17 genes in bin, expand by 20kb;
		
		q=new SQL()
		{
			{
				SELECT("GENE_MASTER.*");
				SELECT("(f4-20000) AS e4");
				SELECT("(f5+20000) AS e5");
				FROM(schemaName+".GENE_MASTER");
			}
		}.toString();
		dataCache.view("t2", q).commit();
		
		q=new SQL()
		{
			{
				SELECT("*");
				FROM(schemaName+".candidate");
				INNER_JOIN(schemaName+".t2 ON (r0=f1 AND ((r1<=e4 AND e4<=r2) OR (r1<=e5 AND e5<=r2) OR (e4<=r1 AND r1<=e5) OR (e4<=r2 AND r2<=e5)))");
				
			}
		}.toString();
		dataCache.view("t3", q).commit();
		
		q=new SQL()
		{
			{
				SELECT("RANK");
				SELECT("p");
				SELECT("hg19chrc");
				SELECT("six1");
				SELECT("six2");
				SELECT("f1 AS g0");
				SELECT("f4 AS g1");
				SELECT("f5 AS g2");
				SELECT("f7 AS gstr");
				SELECT("gid AS ensgene");
				FROM(schemaName+".t3");
			}
		}.toString();
		dataCache.view("allgenes20kb", q).commit();
		
		//*====== PC genes & distance ======;
		//* expand by 10mb;
		
		q=new SQL()
		{
			{
				SELECT("GENE_MASTER.*");
				SELECT("(f4-10e6) AS e4");
				SELECT("(f5+10e6) AS e5");
				FROM(schemaName+".GENE_MASTER");
				WHERE("ttype='protein_coding'");
			}
		}.toString();
		dataCache.view("r1", q).commit();
		
		//* join;
		q=new SQL()
		{
			{
				SELECT("ROWNUM() AS _id");
				SELECT("RANK");
				SELECT("p");
				SELECT("hg19chrc");
				SELECT("six1");
				SELECT("six2");
				SELECT("r0");
				SELECT("r1");
				SELECT("r2");
				SELECT("CC");
				SELECT("NUCSC");
				SELECT("GENENAME");
				SELECT("F1");
				SELECT("F4");
				SELECT("F5");
				SELECT("F7");
				SELECT("GID");
				SELECT("TTYPE");
				SELECT("STATUS");
				SELECT("HUGOPRODUCT");
				SELECT("HUGOALIAS");
				FROM(schemaName+".candidate");
				LEFT_OUTER_JOIN(schemaName+".r1 ON (r0=f1 AND ((r1<=e4 AND e4<=r2) OR (r1<=e5 AND e5<=r2) OR (e4<=r1 AND r1<=e5) OR (e4<=r2 AND r2<=e5)))");
			}
		}.toString();
		dataCache.view("r2", q).commit();
		
		//* classify;
		q=new SQL()
		{
			{
				SELECT("r2.*");
				SELECT("( CASE WHEN ((r1<=f4) AND (f4<=r2)) OR ((r1<=f5) AND (f5<=r2)) OR ((f4<=r1) AND (r1<=f5)) OR ((r2<=f5) AND (f4<=r2)) THEN 0 WHEN f4 IS NULL OR f5 IS NULL THEN 9e9 ELSE NUM_MIN_INTEGER(NUM_MIN_INTEGER(ABS(r1-f4),ABS(r2-f4)),NUM_MIN_INTEGER(ABS(r1-f5),ABS(r2-f5))) END) dist");
				FROM(schemaName+".r2");
				ORDER_BY("RANK, dist");
			}
		}.toString();
		dataCache.view("r3", q).commit();
		
		
		//TODO
		/*
		 * Check if this is correct
		 */
		q=new SQL()
		{
			{
				SELECT("RANK");
				SELECT("p");
				SELECT("hg19chrc");
				SELECT("six1");
				SELECT("six2");
				SELECT("dist AS r2"); //* for column position;
				SELECT("GENENAME");
				SELECT("F1");
				SELECT("F4");
				SELECT("F5");
				SELECT("F7");
				SELECT("GID");
				SELECT("TTYPE");
				SELECT("STATUS");
				SELECT("HUGOPRODUCT");
				SELECT("HUGOALIAS");
				SELECT("dist AS distance");
				FROM(schemaName+".r3");
				WHERE("dist<100000");
				ORDER_BY("RANK,p");
			}
		}.toString();
		dataCache.table("genesPCnear", q).commit();
		
		//*=== nhgri gwas;
		q=new SQL()
		{
			{
				SELECT("RANK");
				SELECT("p");
				SELECT("hg19chrc");
				SELECT("six1");
				SELECT("six2");
				SELECT("r0");
				SELECT("r1");
				SELECT("r2");
				//SELECT("bb.*");
				FROM(schemaName+".candidate aa");
				INNER_JOIN(schemaName+".nhgri_gwas bb ON aa.r0=bb.hg19chrom AND r1<=bb.bp AND bb.bp<=r2");
			}
		}.toString();
		dataCache.table("link_nhgri", q).commit();
		
		//*=== omim;
		q=new SQL()
		{
			{
				SELECT("RANK, p, aa.geneName, hugoproduct, type");
				FROM(schemaName+".genesPCnear aa");
				INNER_JOIN(schemaName+".omim bb ON aa.geneName=bb.geneName");
			}
		}.toString();
		dataCache.table("link_omim", q).commit();
		
		//*=== aut_loci_hg19 ;
		q=new SQL()
		{
			{
				SELECT("RANK, p, aa.geneName, hugoproduct");
				FROM(schemaName+".genesPCnear aa");
				INNER_JOIN(schemaName+".aut_loci_hg19 bb ON aa.geneName=bb.geneName AND aa.geneName IS NOT NULL AND aa.geneName!='' AND bb.geneName IS NOT NULL AND bb.geneName!=''");
			}
		}.toString();
		dataCache.table("link_aut", q).commit();
		
		//*=== id/dev delay ;
		q=new SQL()
		{
			{
				SELECT("RANK, p, aa.geneName, hugoproduct");
				FROM(schemaName+".genesPCnear aa");
				INNER_JOIN(schemaName+".id_devdelay bb ON aa.geneName=bb.geneName AND aa.geneName IS NOT NULL AND aa.geneName!='' AND bb.geneName IS NOT NULL AND bb.geneName!=''");
			}
		}.toString();
		dataCache.table("link_iddd", q).commit();
		
		//*=== jax;
		q=new SQL()
		{
			{
				SELECT("rank, p, aa.geneName, hugoproduct, musName, KOphenotype");
				FROM(schemaName+".genesPCnear aa");
				INNER_JOIN(schemaName+".mouse bb ON aa.geneName=bb.geneName AND aa.geneName IS NOT NULL AND aa.geneName!='' AND bb.geneName IS NOT NULL AND bb.geneName!=''");
			}
		}.toString();
		dataCache.table("link_jax", q).commit();
		
		//*=== psych CNVs;
		q=new SQL()
		{
			{
				SELECT("rank, p, hg19chrc, six1, six2, bb.*");
				FROM(schemaName+".candidate aa");
				INNER_JOIN(schemaName+".psych_cnv_hg19 bb ON aa.r0=bb.c0 AND "+dataCache.scriptTwoSegmentOverlapCondition("r1", "r2", "c1", "c2"));
			}
		}.toString();
		dataCache.table("link_cnv", q).commit();
		
		//*=== g1000 sv;
		q=new SQL()
		{
			{
				SELECT("rank, p, bb.hg19chrc, six1, six2, bp1, BP2, SVTYPE, EURAF, ASIAF, AFRAF");
				SELECT("(NUM_MIN_INTEGER(six2,bp2)-NUM_MAX_INTEGER(six1,bp1))/(NUM_MAX_INTEGER(six2,bp2)-NUM_MIN_INTEGER(six1,bp1)) AS recipoverlap");
				SELECT("'=HYPERLINK(\"http://genome.ucsc.edu/cgi-bin/hgTracks?&org=Human&db=hg19&position='||bb.hg19chrc||'%3A'||"+dataCache.scriptDoubleToVarchar("bp1")+"||'-'||"+dataCache.scriptDoubleToVarchar("bp2")+"||'\",\"g1000sv\")' AS g1000sv");
				FROM(schemaName+".candidate aa");
				INNER_JOIN(schemaName+".g1000sv bb ON euraf>0.01 AND aa.r0=bb.hg19chrc AND "+dataCache.scriptTwoSegmentOverlapCondition("r1", "r2", "bp1", "bp2"));
			}
		}.toString();
		dataCache.table("link_g1000sv", q).commit();
		
		//*=== GPCRs;
		q=new SQL()
		{
			{
				SELECT("RANK");
				SELECT("p");
				SELECT("hg19chrc");
				SELECT("six1");
				SELECT("six2");
				SELECT("r2");
				SELECT("aa.GENENAME");
				SELECT("FAMILY, HG19CHROM, BP1, BP2, MAPTOTAL, bb.HUGOPRODUCT");
				FROM(schemaName+".genesPCnear aa");
				INNER_JOIN(schemaName+".gproteincoupledreceptors bb ON aa.geneName=bb.geneName AND aa.geneName IS NOT NULL AND aa.geneName!='' AND bb.geneName IS NOT NULL AND bb.geneName!=''");
			}
		}.toString();
		dataCache.table("link_gpcr", q).commit();
		
		
		//*=== psych linkage meta;
		q=new SQL()
		{
			{
				SELECT("rank, p, hg19chrc, six1, six2, bp1, bp2");
				SELECT("(NUM_MIN_INTEGER(six2,bp2)-NUM_MAX_INTEGER(six1,bp1))/(NUM_MAX_INTEGER(six2,bp2)-NUM_MIN_INTEGER(six1,bp1)) AS recipoverlap");
				FROM(schemaName+".candidate aa");
				INNER_JOIN(schemaName+".psych_linkage bb ON bb.study='GWL' AND bb.type='MetaAnal' AND aa.r0=hg19chrc AND "+dataCache.scriptTwoSegmentOverlapCondition("r1", "r2", "bp1", "bp2"));
			//six1 IS NOT NULL AND six2 IS NOT NULL AND bp1 IS NOT NULL AND bp2 IS NOT NULL AND
			}
		}.toString();
		dataCache.table("link_psychlinkage", q).commit();
		
	}
	
}
