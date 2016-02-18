@REM java -jar "GwasBioinf-0.8.0.jar" -help
@REM java -jar "GwasBioinf-0.8.0.jar" -input ".\input" -reference
@REM java -jar "GwasBioinf-0.8.0.jar" -input "input\gencode.genes.tsv" -gene
java -jar "GwasBioinf-0.8.0.jar" -input "input\gwas.catalog.tsv" -output_excel "output\output.xlsx"