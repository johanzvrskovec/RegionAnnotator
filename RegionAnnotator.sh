#set your program location dir here
#dir=/some/program/dir
#for example
#dir=/nas/depts/007/sullilab/shared/bioinf/regionAnnotator1.6.0
java -XX:MaxHeapSize=1024m -jar "$dir/RegionAnnotator.jar" $*
