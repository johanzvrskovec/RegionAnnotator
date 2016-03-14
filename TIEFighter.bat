@REM java -jar "TIEFighter.jar" -help
@REM java -jar "TIEFighter.jar" -input ".\input" -reference
@REM java -jar "TIEFighter.jar" -input "input\gencode.genes.tsv" -gene
java -jar "TIEFighter.jar" -input "input\test.txt" -if tsv -output_excel "output\output.xlsx" -timeout 60000