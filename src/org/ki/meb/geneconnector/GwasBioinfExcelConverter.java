package org.ki.meb.geneconnector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.json.JSONArray;
import org.json.JSONObject;


public class GwasBioinfExcelConverter
{
	
	public static enum InputOutputType {database,excel};
	
	//private OutputStream output;
	//private InputStream input;
	private File inputFile;
	private GwasBioinfDataCache outputDataCache;
	private boolean settingFirstRowVariableNames;
	
	//conversion variables
	private XSSFWorkbook currentWorkbook;
	private XSSFSheet currentSheet;
	private Row currentRow;
	private Cell currentCell;
	private HashMap<Integer, String> columnIndexVariableNameMap;
	private HashMap<Integer, Integer> columnIndexVariableTypeMap;
	private Iterator<Row> rowIt;
	private Iterator<Cell> cellIt;
	private Short numVariablesPerRow;
	private JSONObject rowToAdd;
	private JSONArray variableValues;
	
	
	private InputOutputType inputType,outputType;
	
	public GwasBioinfExcelConverter() 
	{
		inputType=InputOutputType.excel;
		outputType=InputOutputType.database;
		settingFirstRowVariableNames=true;
	}
	
	public GwasBioinfExcelConverter setInputType(InputOutputType nInputType)
	{
		inputType=nInputType;
		return this;
	}
	
	public GwasBioinfExcelConverter setOutputType(InputOutputType nOutputType)
	{
		outputType=nOutputType;
		return this;
	}
	
	public GwasBioinfExcelConverter setInputFile(File nFile) throws ApplicationException
	{
		if(inputType!=InputOutputType.excel)
			throw new ApplicationException("Wrong input for the configured input type. The input type is "+inputType.toString()+" and an attempt was made to set an InputFile.");
			
		inputFile = nFile;
		return this;
	}
	
	public GwasBioinfExcelConverter setOutputDataCache(GwasBioinfDataCache nOutputDataCache) throws ApplicationException
	{
		if(outputType!=InputOutputType.database)
			throw new ApplicationException("Wrong output for the configured output type. The output type is "+outputType.toString()+" and an attempt was made to set an OutputDataCache.");
			
		outputDataCache = nOutputDataCache;
		return this;
	}
	
	

	public GwasBioinfExcelConverter convert() throws Exception 
	{
		currentWorkbook = new XSSFWorkbook(inputFile);
		for(int iSheet = 0; iSheet<currentWorkbook.getNumberOfSheets(); iSheet++)
		{
			currentSheet = currentWorkbook.getSheetAt(iSheet);
			columnIndexVariableNameMap = new HashMap<Integer, String>();
			columnIndexVariableTypeMap = new HashMap<Integer, Integer>();
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
					columnIndexVariableNameMap.put(currentCell.getColumnIndex(),currentCell.getStringCellValue());
				}
			}
			
			//set variable type from the first data row
			if(rowIt.hasNext())
			{
				currentRow = rowIt.next();
				commonRowConversionActions();
				
				cellIt = currentRow.cellIterator();
				while(cellIt.hasNext())
				{
					currentCell = cellIt.next();
					columnIndexVariableTypeMap.put(currentCell.getColumnIndex(),currentCell.getCellType());
					commonCellConversionActions();
				}
				
				rowToAdd.append("data", variableValues);
				outputDataCache.enterRow(rowToAdd);
			}
			
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
				
				rowToAdd.append("data", variableValues);
				outputDataCache.enterRow(rowToAdd);
			}
		}
		
		return this;
	}
	
	private void commonRowConversionActions() throws ApplicationException
	{
		rowToAdd=new JSONObject();
		rowToAdd.put("path", currentSheet.getSheetName());
		variableValues = new JSONArray();
		
		if(currentRow.getLastCellNum()!=numVariablesPerRow)
			throw new ApplicationException("Excel error at row number "+currentRow.getRowNum()+". The row has an inconsistent length of "+currentRow.getLastCellNum()+", should be "+numVariablesPerRow);
	}
	
	private void commonCellConversionActions() throws ApplicationException
	{
		
		if(!(currentCell.getCellType()==Cell.CELL_TYPE_BOOLEAN||currentCell.getCellType()==Cell.CELL_TYPE_NUMERIC||currentCell.getCellType()==Cell.CELL_TYPE_STRING))
			throw new ApplicationException("Excel error at row number "+currentRow.getRowNum()+" and column index "+currentCell.getColumnIndex()+". The cell is of an incompatible type, or contains an error.");
		
		JSONObject variableValueToAdd = new JSONObject();
		
		variableValueToAdd.put("name", columnIndexVariableNameMap.get(currentCell.getColumnIndex()));
		
		if(columnIndexVariableTypeMap.get(currentCell.getColumnIndex())==Cell.CELL_TYPE_BOOLEAN)
		{
			variableValueToAdd.put("type", GwasBioinfDataCache.DATATYPE_BOOLEAN);
			variableValueToAdd.put("value", currentCell.getBooleanCellValue());
		}
		else if(columnIndexVariableTypeMap.get(currentCell.getColumnIndex())==Cell.CELL_TYPE_NUMERIC)
		{
			variableValueToAdd.put("type", GwasBioinfDataCache.DATATYPE_DOUBLE);
			variableValueToAdd.put("value", currentCell.getNumericCellValue());
		}
		else if(columnIndexVariableTypeMap.get(currentCell.getColumnIndex())==Cell.CELL_TYPE_STRING)
		{
			variableValueToAdd.put("value", currentCell.getStringCellValue());
			variableValueToAdd.put("type", GwasBioinfDataCache.DATATYPE_STRING);
		}
		else throw new ApplicationException("Column error. At row number "+currentRow.getRowNum()+" and column index "+currentCell.getColumnIndex());
		
		variableValues.put(variableValueToAdd);
	}


}
