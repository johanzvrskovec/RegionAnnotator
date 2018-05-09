gencodeGenesRaw<-read.table("RCOUT.TSV",sep="\t",fill=TRUE)

gencodeGenes<-gencodeGenesRaw[c("V1","V4","V5","V7","V9","V11")]
colnames(gencodeGenes)[colnames(gencodeGenes)=="V1"]<-"chr"
colnames(gencodeGenes)[colnames(gencodeGenes)=="V4"]<-"bp1"
colnames(gencodeGenes)[colnames(gencodeGenes)=="V5"]<-"bp2"
colnames(gencodeGenes)[colnames(gencodeGenes)=="V7"]<-"strand"
colnames(gencodeGenes)[colnames(gencodeGenes)=="V9"]<-"ensemblfull"
#colnames(gencodeGenes)[colnames(gencodeGenes)=="V10"]<-"ensemblt"
colnames(gencodeGenes)[colnames(gencodeGenes)=="V11"]<-"geneName"
#colnames(gencodeGenes)[colnames(gencodeGenes)=="V12"]<-"ttype"

gencodeGenesClean<-subset(gencodeGenes, ensemblfull!="" & chr!="" & nchar(as.character(chr))<6 & bp1!="" & bp2!="" & geneName!="" & strand!="")
gencodeGenesClean$ensembl<-substr(gencodeGenesClean$ensemblfull,0,15)

gencodeGenesClean<-unique(gencodeGenesClean)

unique(gencodeGenesClean$chr)
unique(gencodeGenesClean$strand)

ensemblGenes<-read.csv("mart_export.txt",sep="\t",fill=TRUE)
colnames(ensemblGenes)[colnames(ensemblGenes)=="Gene.stable.ID"]<-"ensembl"
colnames(ensemblGenes)[colnames(ensemblGenes)=="Transcript.stable.ID"]<-"ensemblt"
colnames(ensemblGenes)[colnames(ensemblGenes)=="Chromosome.scaffold.name"]<-"chr"
colnames(ensemblGenes)[colnames(ensemblGenes)=="Gene.start..bp."]<-"bp1"
colnames(ensemblGenes)[colnames(ensemblGenes)=="Gene.end..bp."]<-"bp2"
colnames(ensemblGenes)[colnames(ensemblGenes)=="Strand"]<-"strand"
colnames(ensemblGenes)[colnames(ensemblGenes)=="Transcript.type"]<-"ttype"
colnames(ensemblGenes)[colnames(ensemblGenes)=="Gene.description"]<-"product"
colnames(ensemblGenes)[colnames(ensemblGenes)=="Gene.name"]<-"geneName"

ensemblGenes$chr<-paste("chr",ensemblGenes$chr,sep = "")

ensemblGenesClean<-subset(ensemblGenes, ensembl!="" & chr!="" & nchar(as.character(chr))<6 & bp1!="" & bp2!="" & geneName!="" & strand!="")
ensemblGenesClean$strand[ensemblGenesClean$strand=="1"]<-"+"
ensemblGenesClean$strand[ensemblGenesClean$strand=="-1"]<-"-"

ensemblGenesClean<-unique(ensemblGenesClean)

#old gencode genes
#gencodeGenesOld<-read.csv("gencode.genes_20180509.txt",sep="\t",fill=TRUE,comment.char = "#",header=FALSE)
#gencodeGenesOld<-gencodeGenesOld[c("V1","V2","V3","V4","V6")]
#colnames(gencodeGenesOld)<-c("chr","bp1","bp2","geneName","ensembl")

#entrez id's
entrezTable<-read.csv("gencode.v28lift37.metadata.EntrezGene",sep="\t",fill=TRUE,header = FALSE)
colnames(entrezTable)<-c("ensembltfull","entrez")
entrezTable$ensemblt<-substr(entrezTable$ensembltfull,0,15)

ensemblGenesCleanMerged<-merge(ensemblGenesClean,entrezTable,by="ensemblt",all.x = TRUE)

#compare with gencode
gencodeGenesComparison<-data.frame(ensembl=gencodeGenesClean$ensembl,ensemblfull=gencodeGenesClean$ensemblfull,gencode_geneName=gencodeGenesClean$geneName,gencode_chr=gencodeGenesClean$chr,gencode_bp1=gencodeGenesClean$bp1,gencode_bp2=gencodeGenesClean$bp2, gencode_strand=gencodeGenesClean$strand)
gencodeGenesComparison<-unique(gencodeGenesComparison)

ensemblGenesCleanMerged<-merge(ensemblGenesCleanMerged,gencodeGenesComparison,by="ensembl",all.x = TRUE)

ensemblGenesCleanMerged$ensembltfull<-as.character(ensemblGenesCleanMerged$ensembltfull)

ensemblGenesCleanMerged[]<-lapply(ensemblGenesCleanMerged,as.character)

chrMismatch<-subset(ensemblGenesCleanMerged,chr!=gencode_chr)
#bp1Mismatch<-subset(ensemblGenesCleanMerged,chr==gencode_chr&&bp1!=gencode_bp1)
#bp2Mismatch<-subset(ensemblGenesCleanMerged,chr==gencode_chr&&bp2!=gencode_bp2)
strandMismatch<-subset(ensemblGenesCleanMerged,strand!=gencode_strand)

ensemblGenesCleanMerged$chr[is.na(ensemblGenesCleanMerged$gencode_chr)==FALSE]<-ensemblGenesCleanMerged$gencode_chr[is.na(ensemblGenesCleanMerged$gencode_chr)==FALSE]
ensemblGenesCleanMerged$bp1[is.na(ensemblGenesCleanMerged$gencode_bp1)==FALSE]<-ensemblGenesCleanMerged$gencode_bp1[is.na(ensemblGenesCleanMerged$gencode_bp1)==FALSE]
ensemblGenesCleanMerged$bp2[is.na(ensemblGenesCleanMerged$gencode_bp2)==FALSE]<-ensemblGenesCleanMerged$gencode_bp2[is.na(ensemblGenesCleanMerged$gencode_bp2)==FALSE]
ensemblGenesCleanMerged$strand[is.na(ensemblGenesCleanMerged$gencode_strand)==FALSE]<-ensemblGenesCleanMerged$gencode_strand[is.na(ensemblGenesCleanMerged$gencode_strand)==FALSE]
ensemblGenesCleanMerged$ensembl[is.na(ensemblGenesCleanMerged$ensemblfull)==FALSE]<-ensemblGenesCleanMerged$ensemblfull[is.na(ensemblGenesCleanMerged$ensemblfull)==FALSE]
ensemblGenesCleanMerged$geneName[is.na(ensemblGenesCleanMerged$gencode_geneName)==FALSE]<-ensemblGenesCleanMerged$gencode_geneName[is.na(ensemblGenesCleanMerged$gencode_geneName)==FALSE]


ensemblGenesCleanMerged[is.na(ensemblGenesCleanMerged)]<-"."

ensemblGenesCleanMergedFiltered<-subset(ensemblGenesCleanMerged,ttype=="protein_coding")



gencodeGenesFinal<-data.frame(
  chr=ensemblGenesCleanMergedFiltered$chr,
  bp1=ensemblGenesCleanMergedFiltered$bp1,
  bp2=ensemblGenesCleanMergedFiltered$bp2,
  geneName=ensemblGenesCleanMergedFiltered$geneName,
  entrez=ensemblGenesCleanMergedFiltered$entrez,
  ensembl=ensemblGenesCleanMergedFiltered$ensembl,
  ttype=ensemblGenesCleanMergedFiltered$ttype,
  strand=ensemblGenesCleanMergedFiltered$strand,
  product=ensemblGenesCleanMergedFiltered$product
)
gencodeGenesFinal<-unique(gencodeGenesFinal)

fileConn<-file("gencode.genes_new.txt")
writeLines(c("## gencode.genes",
             "## GENCODE protein coding genes, combined with entrez id's from GENCODE, and combined with Ensembl product description and transcript type from https://www.ensembl.org/biomart",
             "## GRCh37/hg19, date: 2018-03-27, evidence-based annotation of the human genome, version 28 (Ensembl 92), mapped to GRCh37 with gencode-backmap",
             "## PGC bioinformatics, PF Sullivan & Johan Kallberg Zvrskovec, Karolinska Institutet, 2018",
             "## Rows: 5 comments, header, 19724 entries. Columns: 9",
             "#chr	bp1	bp2	geneName	entrez	ensembl	ttype	strand	product")
           , fileConn)
write.table(gencodeGenesFinal,file = "gencode.genes_new.txt",quote = c(9),row.names = FALSE,sep = "\t",col.names = FALSE, append=TRUE)