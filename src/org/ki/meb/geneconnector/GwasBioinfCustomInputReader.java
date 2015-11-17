package org.ki.meb.geneconnector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONObject;


public class GwasBioinfCustomInputReader
{
	
	public static enum InputOutputType {database,excel};
	
	//private OutputStream output;
	//private InputStream input;
	private File inputFile;
	private GwasBioinfDataCache dataCache;
	private boolean settingFirstRowVariableNames;
	
	//conversion variables
	private XSSFWorkbook currentWorkbook;
	private XSSFSheet currentSheet;
	private Row currentRow;
	private Cell currentCell;
	private JSONObject elementMeta;
	private JSONObject elementNameMap;
	private JSONObject elementNameIndexMap;
	private JSONObject elementIndexNameMap;
	//private HashMap<Integer, String> columnIndexVariableNameMap;
	//private HashMap<Integer, Integer> columnIndexVariableTypeMap;
	private Iterator<Row> rowIt;
	private Iterator<Cell> cellIt;
	private Short numVariablesPerRow;
	private JSONObject rowToAdd;
	private JSONObject variableValues;
	
	
	private InputOutputType inputType,outputType;
	
	public GwasBioinfCustomInputReader() 
	{
		inputType=InputOutputType.excel;
		outputType=InputOutputType.database;
		settingFirstRowVariableNames=true;
	}
	
	public GwasBioinfCustomInputReader setInputType(InputOutputType nInputType)
	{
		inputType=nInputType;
		return this;
	}
	
	public GwasBioinfCustomInputReader setOutputType(InputOutputType nOutputType)
	{
		outputType=nOutputType;
		return this;
	}
	
	public GwasBioinfCustomInputReader setInputFile(File nFile) throws ApplicationException
	{
		if(inputType!=InputOutputType.excel)
			throw new ApplicationException("Wrong input for the configured input type. The input type is "+inputType.toString()+" and an attempt was made to set an InputFile.");
			
		inputFile = nFile;
		return this;
	}
	
	public GwasBioinfCustomInputReader setOutputDataCache(GwasBioinfDataCache nOutputDataCache) throws ApplicationException
	{
		if(outputType!=InputOutputType.database)
			throw new ApplicationException("Wrong output for the configured output type. The output type is "+outputType.toString()+" and an attempt was made to set an OutputDataCache.");
			
		dataCache = nOutputDataCache;
		return this;
	}
	
	

	public GwasBioinfCustomInputReader read() throws Exception 
	{
		
		if(inputType==InputOutputType.excel&&outputType==InputOutputType.database)
		{
			int rowBufferSize = 100000;
			
			currentWorkbook = new XSSFWorkbook(inputFile);
			for(int iSheet = 0; iSheet<currentWorkbook.getNumberOfSheets(); iSheet++)
			{
				currentSheet = currentWorkbook.getSheetAt(iSheet);
				if(dataCache.getHasTable(currentSheet.getSheetName())&&!dataCache.getRefreshExistingTables())
					continue;
				
				JSONObject entry = new JSONObject();
				elementMeta = new JSONObject();
				elementNameMap = new JSONObject();
				elementNameIndexMap = new JSONObject();
				elementIndexNameMap = new JSONObject();
				HashSet<String> typeset = new HashSet<String>();
				JSONArray elementNameArray = new JSONArray();
				elementMeta.put("path", currentSheet.getSheetName());
				elementMeta.put("namemap",elementNameMap);
				elementMeta.put("nameindexmap",elementNameIndexMap);
				elementMeta.put("elementnamearray",elementNameArray);
				
				JSONArray rowBuffer=new JSONArray();
				//columnIndexVariableNameMap = new HashMap<Integer, String>();
				//columnIndexVariableTypeMap = new HashMap<Integer, Integer>();
				numVariablesPerRow = null;
				
				rowIt = currentSheet.rowIterator();
				//set variable names and number of variables
				if(settingFirstRowVariableNames && rowIt.hasNext())
				{
					currentRow = rowIt.next();
					numVariablesPerRow= currentRow.getLastCellNum();
					cellIt = currentRow.cellIterator();
					while(cellIt.hasNext())
					{
						currentCell = cellIt.next();
						if(currentCell.getCellType()!=Cell.CELL_TYPE_STRING)
							throw new ApplicationException("Excel error at row number "+currentRow.getRowNum()+" and column index "+currentCell.getColumnIndex());
						
						
						if(!elementNameMap.has(currentCell.getStringCellValue()))
						{
							JSONObject element = new JSONObject();
							//element.put("name", currentCell.getStringCellValue());
							element.put("index", currentCell.getColumnIndex());
							elementNameMap.putOnce(currentCell.getStringCellValue(), element);
							elementNameIndexMap.putOnce(currentCell.getStringCellValue(), ""+currentCell.getColumnIndex());
							elementIndexNameMap.putOnce(""+currentCell.getColumnIndex(), currentCell.getStringCellValue());
							elementNameArray.put(currentCell.getStringCellValue());
						}
					}
				}
				
				//set variable type from the first element that contains data
				while(rowIt.hasNext())
				{
					currentRow = rowIt.next();
					cellIt = currentRow.cellIterator();
					while(cellIt.hasNext())
					{
						currentCell = cellIt.next();
						if(currentCell.getCellType()!=Cell.CELL_TYPE_ERROR && currentCell.getCellType()!=Cell.CELL_TYPE_BLANK)
						{
							String name = elementIndexNameMap.getString(""+currentCell.getColumnIndex());
							JSONObject element = elementNameMap.getJSONObject(name);
							short typeToPut;
							
							if(currentCell.getCellType()==Cell.CELL_TYPE_BOOLEAN || (currentCell.getCellType()==Cell.CELL_TYPE_FORMULA && currentCell.getCachedFormulaResultType()==Cell.CELL_TYPE_BOOLEAN))
							{
								typeToPut= GwasBioinfDataCache.DATATYPE_BOOLEAN;
							}
							else if(currentCell.getCellType()==Cell.CELL_TYPE_NUMERIC || (currentCell.getCellType()==Cell.CELL_TYPE_FORMULA && currentCell.getCachedFormulaResultType()==Cell.CELL_TYPE_NUMERIC))
							{
								typeToPut= GwasBioinfDataCache.DATATYPE_DOUBLE;
							}
							else if(currentCell.getCellType()==Cell.CELL_TYPE_STRING || (currentCell.getCellType()==Cell.CELL_TYPE_FORMULA && currentCell.getCachedFormulaResultType()==Cell.CELL_TYPE_STRING))
							{
								typeToPut= GwasBioinfDataCache.DATATYPE_STRING;
							}
							else throw new ApplicationException("Column error - the cell is of an incompatible type. At row number "+currentRow.getRowNum()+" and column index "+currentCell.getColumnIndex());
							
							if(!element.has("type"))
								element.put("type", typeToPut);
							typeset.add(name);
						}
					}
					
					if(typeset.size()>=elementNameMap.length())
						break;
				}
				
				
				
				
				//add metadata
				entry.put("elementmeta", elementMeta);
				
				//restart
				rowIt = currentSheet.rowIterator();
				if(settingFirstRowVariableNames)
					currentRow = rowIt.next();
				
				//convert the rest of the data
				while(rowIt.hasNext())
				{
					currentRow = rowIt.next();
					commonRowConversionActions();
					
					cellIt = currentRow.cellIterator();
					while(cellIt.hasNext())
					{
						currentCell = cellIt.next();
						commonCellConversionActions();
					}
					
					rowToAdd.put("data", variableValues);
					if(rowBuffer.length()<rowBufferSize)
					{
						rowBuffer.put(rowToAdd);
					}
					else
					{
						entry.put("rows", rowBuffer);
						dataCache.enter(entry);
						rowBuffer=new JSONArray();
					}
				}
				
				if(rowBuffer.length()>0)
				{
					entry.put("rows", rowBuffer);
					dataCache.enter(entry);	
				}
			}
			currentWorkbook.close();
			return this;
		}
		else throw new ApplicationException("Input or output format is unsupported by the custom input reader");
	}
	
	private void commonRowConversionActions() throws ApplicationException
	{
		rowToAdd=new JSONObject();
		//rowToAdd.put("path", inputFile.getName()+"_"+currentSheet.getSheetName());
		rowToAdd.put("path", currentSheet.getSheetName());
		variableValues = new JSONObject();
		
		//This should be handled when entering data if sufficient column info
		//if(currentRow.getLastCellNum()!=numVariablesPerRow)
			//throw new ApplicationException("Excel error at row number "+currentRow.getRowNum()+". The row has an inconsistent length of "+currentRow.getLastCellNum()+", should be "+numVariablesPerRow);
	}
	
	private void commonCellConversionActions() throws ApplicationException
	{
		int currentCellType = currentCell.getCellType();
		
		//if(!(currentCellType==Cell.CELL_TYPE_BOOLEAN||currentCellType==Cell.CELL_TYPE_NUMERIC||currentCellType==Cell.CELL_TYPE_STRING))
		if(currentCellType==Cell.CELL_TYPE_ERROR)
			throw new ApplicationException("Excel error at row number "+currentRow.getRowNum()+" and column index "+currentCell.getColumnIndex()+". The cell contains an error or is of an incompatible type.");
		
		String name = elementIndexNameMap.getString(""+currentCell.getColumnIndex());
		JSONObject element =  elementNameMap.getJSONObject(name);
		
		JSONObject variableValueToAdd = new JSONObject();
		
		int type = element.getInt("type");
		
		//variableValueToAdd.put("name", name);
		
		if(type==GwasBioinfDataCache.DATATYPE_BOOLEAN)
		{
			variableValueToAdd.put("value", currentCell.getBooleanCellValue());
		}
		else if(type==GwasBioinfDataCache.DATATYPE_DOUBLE)
		{
			variableValueToAdd.put("value", currentCell.getNumericCellValue());
		}
		else if(type==GwasBioinfDataCache.DATATYPE_STRING)
		{
			variableValueToAdd.put("value", currentCell.getStringCellValue());
		}
		else throw new ApplicationException("Column error - the cell is of an incompatible type. At row number "+currentRow.getRowNum()+" and column index "+currentCell.getColumnIndex());
		
		variableValueToAdd.put("type", type);
		variableValues.put(name,variableValueToAdd);
	}


}
