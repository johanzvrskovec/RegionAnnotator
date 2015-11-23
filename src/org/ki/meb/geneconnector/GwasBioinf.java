package org.ki.meb.geneconnector;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.ConfigurationNode;

import javax.jms.ConnectionFactory;
import org.apache.camel.component.jms.JmsComponent;

import org.apache.taverna.scufl2.api.container.WorkflowBundle;
import org.apache.taverna.scufl2.api.core.Workflow;
import org.apache.taverna.scufl2.api.io.ReaderException;
import org.apache.taverna.scufl2.api.io.WorkflowBundleIO;

//import getl.proc.Flow;

public class GwasBioinf
{
	
	private CommandLine commandLine;
	
	private static Options clOptions = new Options();
	private PrintStream directedDataOutputStream;
	private PrintStream directedMessageOutputStream;
	
	private String settingCharsetEncodingText;
	private File settingInputFolder, settingOutputFolder, settingConfigFile;
	private GwasBioinfDataCache dataCache;
	private FilenameFilter filterExcelXlsx;
	
	static
	{
		//clOptions.addOption(OptionBuilder.create(TextMap.regtest)); //inactivated as default
		clOptions.addOption(TextMap.help,false,"Print usage help.");
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Config file").hasArg().create(TextMap.config));
		//clOptions.addOption(OptionBuilder.withArgName(TextMap.username).withDescription("Username to use for authentication and authorization.").hasArg().create(TextMap.username));
		//clOptions.addOption(OptionBuilder.withArgName(TextMap.password).withDescription("Password for authentication.").hasArg().create(TextMap.password));
		//clOptions.addOption(OptionBuilder.withArgName(TextMap.command).withDescription("A JSON-formatted command to configure or send.").hasArg().withLongOpt(TextMap.command).create("c"));
		//clOptions.addOption(OptionBuilder.withArgName(TextMap.type).withDescription("The type of structure to create. The alternatives are: "+TextMap.command+"|"+TextMap.activity+"|"+TextMap.object).hasArg().withLongOpt(TextMap.create).create("cr"));
		//clOptions.addOption(OptionBuilder.withArgName("property=value" ).hasArgs().withValueSeparator().withDescription( "Set a command structure property value." ).create(TextMap.set));
		//clOptions.addOption(OptionBuilder.withArgName("type=structure" ).hasArgs().withValueSeparator().withDescription( "Add a type structure. The types are: "+TextMap.activity ).create(TextMap.add));
		clOptions.addOption(TextMap.init,false,"Initiate the database content from input files");
		clOptions.addOption(TextMap.refresh,false,"Refresh the database content from input files");
		clOptions.addOption(TextMap.link,false,"Perform gene linkage steps");
		clOptions.addOption(TextMap.get,false,"Get the database content as exported output");
		
		
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("Path to the folder of the input files used for initiation").hasArg().create("ipath"));
		clOptions.addOption(OptionBuilder.withArgName("file path").withDescription("File to be used as output").hasArg().withLongOpt("outputfile").create("of"));
		
		
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
	}

	private void init() throws ConfigurationException
	{
		XMLConfiguration config = new XMLConfiguration(settingConfigFile);
		ConfigurationNode rootNode = config.getRootNode();
		//setDBConnectionString((String)((ConfigurationNode)rootNode.getChildren(TextMap.database_connectionstring).get(0)).getValue());
		//setResourceFolder((String)((ConfigurationNode)rootNode.getChildren(TextMap.resourcefolderpath).get(0)).getValue());
		settingInputFolder = new File((String)((ConfigurationNode)rootNode.getChildren(TextMap.inputfolderpath).get(0)).getValue());
		settingOutputFolder = new File((String)((ConfigurationNode)rootNode.getChildren(TextMap.outputfolderpath).get(0)).getValue());
		settingCharsetEncodingText="UTF-8";
		dataCache=new GwasBioinfDataCache().setRefreshExistingTables(commandLine.hasOption(TextMap.refresh));
		
	}
	
	public static CommandLine constructCommandLine(String[] args) throws ParseException
	{
		CommandLineParser parser = new org.apache.commons.cli.GnuParser();
		CommandLine commandLine = parser.parse(clOptions, args);
		return commandLine;
	}
	
	
	public static void main(String[] args) throws Exception
	{
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
	
	//Legacy POI version
	private void initDataFromFiles() throws ApplicationException, Exception
	{
		GwasBioinfCustomInputReader inputReader = new GwasBioinfCustomInputReader();
		//import all files in input
		File[] inputFiles = settingInputFolder.listFiles(filterExcelXlsx);
		for(int iFile=0; iFile<inputFiles.length; iFile++)
		{
			inputReader.setOutputDataCache(dataCache).setInputFile(inputFiles[iFile]).read();
		}
	}
	
	private void getData() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException
	{

		//TODO
	}
	
	private void performTavernaWorkflow() throws ReaderException, IOException
	{
		//TODO
		WorkflowBundleIO io = new WorkflowBundleIO();
		WorkflowBundle ro = io.readBundle(new File("workflow.t2flow"), null);
		ro.getMainWorkflow();
	}
	
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
		
		if(commandLine.hasOption(TextMap.link))
		{
			linkGeneActions();
		}
		
		if(commandLine.hasOption(TextMap.get))
		{
			getData();
		}
		
		dataCache.shutdownCacheConnection();
		
		return this;
	}
	
	
	private void linkGeneActions() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, ApplicationException
	{
		dataCache.linkGenes();
	}
	
}
