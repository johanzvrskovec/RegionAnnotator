#Basic usage template. How to run the most common scenario.
cd /home/pgcbioif/basicAnnotation

#Command line help
#sh RegionAnnotator.sh
#or
#sh RegionAnnotator.sh -help

#Load the database with gene data. One file. If .txt we need to specify the input format.
#sh RegionAnnotator.sh -input "inputGene/gencode.genes.txt" -gene -iformat TSV

#Load the database with reference data. Since it's many files, we can point to a directory. If .txt-files we need to specify the input format.
#sh RegionAnnotator.sh -input inputReference -reference -iformat TSV

#Run the program on an input file with regions/genes.
#The program operations will be run, using the previously loaded gene and reference data, and an output-file will be created (standardized EXCEL-file as default).
#If the input file is a .txt we need to specify the input format. If risk of the database being in use - you can use a timeout-flag
#- the program will then wait the specified number of seconds for the database to become available.
#sh RegionAnnotator.sh -input "input/pgcscz3.txt" -iformat TSV -timeout 60000
