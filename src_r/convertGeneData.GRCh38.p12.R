#run in inputGene

gencodeGenesNewTable<-read.csv("mart_export.txt",sep="\t",fill=TRUE)

gencodeGenesNewTable<-gencodeGenesNewTable[c("Gene.stable.ID","Transcript.stable.ID","Chromosome.scaffold.name","Gene.start..bp.","Gene.end..bp.","Gene.name","Strand","Transcript.type","Gene.description")]
colnames(gencodeGenesNewTable)[colnames(gencodeGenesNewTable)=="Gene.stable.ID"]<-"ensembl"
colnames(gencodeGenesNewTable)[colnames(gencodeGenesNewTable)=="Transcript.stable.ID"]<-"ensemblt"
colnames(gencodeGenesNewTable)[colnames(gencodeGenesNewTable)=="Chromosome.scaffold.name"]<-"chr"
colnames(gencodeGenesNewTable)[colnames(gencodeGenesNewTable)=="Gene.start..bp."]<-"bp1"
colnames(gencodeGenesNewTable)[colnames(gencodeGenesNewTable)=="Gene.end..bp."]<-"bp2"
colnames(gencodeGenesNewTable)[colnames(gencodeGenesNewTable)=="Gene.name"]<-"geneName"
colnames(gencodeGenesNewTable)[colnames(gencodeGenesNewTable)=="Strand"]<-"strand"
colnames(gencodeGenesNewTable)[colnames(gencodeGenesNewTable)=="Transcript.type"]<-"ttype"
colnames(gencodeGenesNewTable)[colnames(gencodeGenesNewTable)=="Gene.description"]<-"product"

gencodeGenesNewTableClean<-subset(gencodeGenesNewTable, ensembl!="" & chr!="" & nchar(as.character(chr))<3 & bp1!="" & bp2!="" & geneName!="" & strand!="" & ttype!="")
gencodeGenesNewTableClean$chr<-paste("chr",gencodeGenesNewTableClean$chr,sep = "")
gencodeGenesNewTableClean$strand[gencodeGenesNewTableClean$strand=="1"]<-"+"
gencodeGenesNewTableClean$strand[gencodeGenesNewTableClean$strand=="-1"]<-"-"

unique(gencodeGenesNewTableClean$chr)
unique(gencodeGenesNewTableClean$strand)
unique(gencodeGenesNewTableClean$ttype)

#entrez id's
entrezTable<-read.csv("gencode.v28.metadata.EntrezGene",sep="\t",fill=TRUE,header = FALSE)
colnames(entrezTable)<-c("ensembltfull","entrez")
entrezTable$ensemblt<-substr(entrezTableRaw$ensembltfull,0,15)
#entrezTable$ensemblt<-as.character(entrezTable$ensemblt)

#gencodeGenesNewTableClean$entrez[gencodeGenesNewTableClean$ensemblt %in% entrezTableRaw$tid]<-"rep"
#gencodeGenesNewTableClean$entrez[gencodeGenesNewTableClean$ensemblt %in% entrezTableRaw$tid]<-entrezTableRaw$V2

gencodeGenesNewTableCleanMerged<-merge(gencodeGenesNewTableClean,entrezTable,by="ensemblt",all.x = TRUE)

#todo
gencodeGenesFinal<-data.frame(
  chr=gencodeGenesNewTableClean$chr,
  bp1=gencodeGenesNewTableClean$bp1,
  bp2=gencodeGenesNewTableClean$bp2,
  geneName=gencodeGenesNewTableClean$geneName,
  entrez=gencodeGenesNewTableClean$entrez,
  ensembl=gencodeGenesNewTableClean$ensembl,
  ttype=gencodeGenesNewTableClean$ttype,
  strand=gencodeGenesNewTableClean$strand,
  product=gencodeGenesNewTableClean$product
)


fileConn<-file("gencode.genes_new.txt")
writeLines(c("## gencode.genes",
             "## GENCODE genes, although fetched from https://www.ensembl.org/biomart combined with entrez id's from GENCODE",
             "## GRCh38.p12, source=https://www.ensembl.org/biomart 20180509",
             "## PGC bioinformatics, PF Sullivan & Johan Kallberg Zvrskovec, Karolinska Institutet, 2018",
             "## Rows: 5 comments, header, 322425 entries. Columns: 9",
             "#chr	bp1	bp2	geneName	entrez	ensembl	ttype	strand	product")
           , fileConn)
write.table(gencodeGenesFinal,file = "gencode.genes_new.txt",quote = c(9),row.names = FALSE,sep = "\t",col.names = FALSE, append=TRUE)