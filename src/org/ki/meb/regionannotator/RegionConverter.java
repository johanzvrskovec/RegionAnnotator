package org.ki.meb.regionannotator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.jakz.common.ApplicationException;

/**
 * Converts GFF3 into tsv
 * @author johkal
 *
 */
public class RegionConverter
{
	private static CSVFormat csvFormatTSVGff3 = CSVFormat.DEFAULT.withDelimiter('\t').withAllowMissingColumnNames(true);
	private static CSVFormat csvFormatTSV = CSVFormat.DEFAULT.withDelimiter('\t').withAllowMissingColumnNames(false);
	private File infile,outfile;
	private BufferedReader inreader;
	private BufferedWriter outwriter;
	private int rowIndexCounter;
	private CSVParser csvParser;
	private CSVPrinter csvPrinter;
	
	/**
	 * Converts GFF3 into tsv
	 * @param args
	 * @throws IOException 
	 * @throws ApplicationException 
	 */
	public static void main(String[] args) throws IOException, ApplicationException
	{
		new RegionConverter(args[0]);
	}
	
	public RegionConverter(String infilepath) throws IOException, ApplicationException
	{
		infile = new File(infilepath);
		outfile = new File("RCOUT.TSV");
		inreader = new BufferedReader(new FileReader(infile));
		outwriter = new BufferedWriter(new FileWriter(outfile));
		
		rowIndexCounter=0;
		
		csvParser = csvFormatTSVGff3.parse(inreader);
		Iterator<CSVRecord> rowIt = csvParser.iterator();
		csvPrinter = csvFormatTSV.print(outwriter);
		
		while(rowIt.hasNext())
		{
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
		String[] readRow = new String[12];
		for(int icell=0; cellIt.hasNext(); icell++)
		{
			String cellContent = cellIt.next().trim();
			
			//skip comments
			if(icell==0 && cellContent.indexOf("#")==0)
			{
				System.out.println(cellContent);
				return;
			}
			
			
			
			if(icell==8)
			{
				String[] attrPart = cellContent.split(";");
				HashMap<String, String> partMap = new HashMap<String,String>();
				for(int ipart=0; ipart<attrPart.length; ipart++)
				{
					String[] partKeyValue = attrPart[ipart].split("=");
					if(partKeyValue.length==2)
					{
						partMap.put(partKeyValue[0].trim(), partKeyValue[1].trim());
					}
					else throw new ApplicationException("Part key value pair is of strange size at row index "+rowIndexCounter);
				}
				
				//gene_id
				if(partMap.containsKey("gene_id"))
					readRow[8]=partMap.get("gene_id");
				else
					readRow[8]=".";
				
				//transcript_id
				if(partMap.containsKey("transcript_id"))
					readRow[9]=partMap.get("transcript_id");
				else
					readRow[9]=".";
				
				//gene_name
				if(partMap.containsKey("gene_name"))
					readRow[10]=partMap.get("gene_name");
				else
					readRow[10]=".";
				
				//transcript_type
				if(partMap.containsKey("transcript_type"))
					readRow[11]=partMap.get("transcript_type");
				else
					readRow[11]=".";
			}
			else
				readRow[icell]=cellContent;
				
		}
		
		if(readRow[2].equals("gene"))
		{
			csvPrinter.printRecord(Arrays.asList(readRow));
		}
	}

}
