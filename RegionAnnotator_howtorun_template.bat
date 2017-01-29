@REM java -jar "RegionAnnotator.jar" -help
@REM java -jar "RegionAnnotator.jar" -input ".\inputReference" -reference -iformat TSV
@REM java -jar "RegionAnnotator.jar" -input "inputGene\gencode.genes.txt" -gene -iformat TSV
java -jar "RegionAnnotator.jar" -input "input\pgcscz3.txt" -iformat tsv -oformat EXCEL -timeout 60000