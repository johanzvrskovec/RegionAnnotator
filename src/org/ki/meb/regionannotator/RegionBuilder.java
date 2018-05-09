package org.ki.meb.regionannotator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.jakz.common.ApplicationException;
import org.jakz.common.JSONArray;
import org.jakz.common.JSONObject;
import org.jakz.common.OperationException;

public class RegionBuilder
{
	private static CSVFormat csvFormatTSV = CSVFormat.DEFAULT.withDelimiter('\t').withAllowMissingColumnNames(false);
	private File infile,outfile;

	private BufferedReader inreader;
	private BufferedWriter outwriter;
	private int rowIndexCounter;
	private CSVParser csvParser;
	private CSVPrinter csvPrinter;
	
	Client restWSClient;
	WebTarget t;

	public static void main(String[] args) throws IOException, ApplicationException
	{
		
		new RegionBuilder(args[0]);

	}
	
	public RegionBuilder(String infilepath) throws IOException, ApplicationException
	{
		restWSClient = ClientBuilder.newClient();
		t = restWSClient.target("http://grch37.rest.ensembl.org/variation/human/");
		
		
		infile = new File(infilepath);
		outfile = new File("newSNPMapping.tsv");
		inreader = new BufferedReader(new FileReader(infile));
		outwriter = new BufferedWriter(new FileWriter(outfile));
		
		rowIndexCounter=0;
		
		csvParser = csvFormatTSV.parse(inreader);
		Iterator<CSVRecord> rowIt = csvParser.iterator();
		csvPrinter = csvFormatTSV.print(outwriter);
		
		//remove header row
		if(rowIt.hasNext())
			rowIt.next();
		
		while(rowIt.hasNext())
		{
			if(rowIndexCounter % 1000 ==0)
				System.out.println("1k rows passed");
			
			operateRow(rowIt.next());
			rowIndexCounter++;
			//Dev settings
			//if(rowIndexCounter>100)
				//break;
		}
		
		outwriter.flush();
		csvPrinter.close();
		outwriter.close();
		csvParser.close();
		inreader.close();
	}
	
	private void operateRow(CSVRecord csvRecord) throws ApplicationException, IOException
	{
		if(!csvRecord.isConsistent())
			throw new ApplicationException("Row index "+rowIndexCounter+" is of non consistent size");
		Iterator<String> cellIt = csvRecord.iterator();
		ArrayList<String> rowToWrite = new ArrayList<String>();
		
		String rsId = null;
		String newPos=null;
		
		for(int icell=0; cellIt.hasNext(); icell++)
		{
			String cellContent = cellIt.next().trim();
			
			//skip comments
			if(icell==0 && cellContent.indexOf("#")==0)
			{
				System.out.println(cellContent);
				return;
			}
			
			
			
			if(icell==23)
			{
				rsId="rs"+cellContent.trim();
			}
				
		}
		
		if(rsId!=null)
		{
			rowToWrite.add(rsId);
			newPos="";
			if(rsId.length()>2)
			{
				Integer newCoordinate = fetchNewCoordinate(rsId);
				if(newCoordinate!=null)
					newPos=""+newCoordinate;
			}
			rowToWrite.add(newPos);
			csvPrinter.printRecord(rowToWrite);
		}
	}
	
	private Integer fetchNewCoordinate(String rsId) throws ApplicationException
	{
		Response response = null;
		
		try
		{
			response = t.path(rsId).request(MediaType.APPLICATION_JSON).get();
		}
		catch (Exception e)
		{
			throw new ApplicationException("Exception when communicating with external.", e);
		}
		
		if(response!=null&&response.getStatus()==200&&response.hasEntity())
		{
			String readResponse = response.readEntity(String.class);
			JSONObject jsonResponse = new JSONObject(readResponse);
			if(jsonResponse.has("mappings"))
			{
				JSONArray mappings = jsonResponse.getJSONArray("mappings");
				if(mappings.length()>0)
				{
					JSONObject m0 = mappings.getJSONObject(0);
					if(m0.getString("assembly_name").equals("GRCh37"))
					{
						return m0.getInt("start");
					}
					else
						return null;
				}
				else return null;
			}
			else
				return null;
		}
		else if(response!=null&&response.getStatus()==400)
		{
			if(response.hasEntity())
			{
				String readResponse = response.readEntity(String.class);
				System.err.println("Status code 400, rsid="+rsId+" response:\n"+readResponse);
			}
			return null;
		}
		else if(response!=null)
		{
			System.err.println("No working connection with external. Status code "+response.getStatus());
			return null;
		}
		else
		{
			System.err.println("No working connection with external. No response available.");
			return null;
		}
	}

}
