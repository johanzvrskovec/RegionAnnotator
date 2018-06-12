#run in inputPreProcess


gwasCatalogNewTableRaw<-read.csv("gwas_catalog_v1.0.2-associations_e92_r2018-05-29.tsv",sep="\t",fill=TRUE)



gwasCatalogNewTable<-gwasCatalogNewTableRaw[c("DISEASE.TRAIT","CHR_ID","CHR_POS","SNP_ID_CURRENT","P.VALUE","PUBMEDID")]
colnames(gwasCatalogNewTable)[colnames(gwasCatalogNewTable)=="DISEASE.TRAIT"]<-"trait"
colnames(gwasCatalogNewTable)[colnames(gwasCatalogNewTable)=="CHR_ID"]<-"chr"
colnames(gwasCatalogNewTable)[colnames(gwasCatalogNewTable)=="CHR_POS"]<-"bp1"
colnames(gwasCatalogNewTable)[colnames(gwasCatalogNewTable)=="SNP_ID_CURRENT"]<-"snpid"
colnames(gwasCatalogNewTable)[colnames(gwasCatalogNewTable)=="P.VALUE"]<-"pvalue"
colnames(gwasCatalogNewTable)[colnames(gwasCatalogNewTable)=="PUBMEDID"]<-"pmid"

gwasCatalogNewTableSigClean<-subset(gwasCatalogNewTable, trait!="" & chr!="" & nchar(as.character(chr))<3 & bp1!="" & snpid!="" & pvalue!="" & pmid!="" & pvalue<5e-8)
gwasCatalogNewTableSigClean$chrn<-gwasCatalogNewTableSigClean$chr
gwasCatalogNewTableSigClean$chr<-paste("chr",gwasCatalogNewTableSigClean$chr,sep = "")
gwasCatalogNewTableSigClean$bp2<-gwasCatalogNewTableSigClean$bp1
gwasCatalogNewTableSigClean$snpid<- strtoi(gwasCatalogNewTableSigClean$snpid)
gwasCatalogNewTableSigClean$fullcoord<-paste(gwasCatalogNewTableSigClean$chr,gwasCatalogNewTableSigClean$bp1,sep=":")
gwasCatalogNewTableSigClean$fullcoord<-paste(gwasCatalogNewTableSigClean$fullcoord,gwasCatalogNewTableSigClean$bp1,sep="-")

unique(gwasCatalogNewTableSigClean$chrn)

coordMap<-gwasCatalogNewTableSigClean[c("chrn","bp1","bp2")]
#coordMap<-gwasCatalogNewTableSigClean[c("fullcoord","snpid")]
coordMap<-unique(coordMap)

write.table(coordMap,file = "coordMapToConvert.bed",row.names = FALSE,quote = FALSE, sep = " ",col.names = FALSE, append=FALSE)
#FOR USIGN THE ONLINE LIFTOVER TOOL
#NOT WORKING

#fix snp coordinates
gwasCatalogNewSNPMApping<-read.table("newSNPMapping.tsv",sep="\t",fill=TRUE,header = FALSE)
colnames(gwasCatalogNewSNPMApping)<-c("snpid","bp1_new")
gwasCatalogNewSNPMApping<-subset(gwasCatalogNewSNPMApping,nchar(as.character(snpid))>2)
gwasCatalogNewSNPMApping$snpid<-as.character(gwasCatalogNewSNPMApping$snpid)
gwasCatalogNewSNPMApping$snpid<-substr(gwasCatalogNewSNPMApping$snpid,3,nchar(gwasCatalogNewSNPMApping$snpid))
gwasCatalogNewSNPMApping$snpid<-strtoi(gwasCatalogNewSNPMApping$snpid)
gwasCatalogNewSNPMApping<-unique(gwasCatalogNewSNPMApping)

gwasCatalogNewTableSigCleanMerged<-merge(gwasCatalogNewTableSigClean,gwasCatalogNewSNPMApping,by="snpid",all.x = TRUE)

gwasCatalogOldRaw<-read.csv("gwas.catalog_20180509.txt",sep="\t",fill=TRUE,comment.char = "#",header=FALSE)
gwasCatalogOld<-gwasCatalogOldRaw[c("V2","V4")]
colnames(gwasCatalogOld)<-c("bp1_old","snpid")
gwasCatalogOld$snpid<-as.character(gwasCatalogOld$snpid)
gwasCatalogOld$snpid<-substr(gwasCatalogOld$snpid,3,nchar(gwasCatalogOld$snpid))
gwasCatalogOld$snpid<-strtoi(gwasCatalogOld$snpid)
gwasCatalogOld<-unique(gwasCatalogOld)

gwasCatalogNewTableSigCleanMerged<-merge(gwasCatalogNewTableSigCleanMerged,gwasCatalogOld,by="snpid",all.x = TRUE)

gwasCatalogNewTableSigCleanMerged$bp1_final<-gwasCatalogNewTableSigCleanMerged$bp1_new
gwasCatalogNewTableSigCleanMerged$snpid<-as.character(gwasCatalogNewTableSigCleanMerged$snpid)
gwasCatalogNewTableSigCleanMerged$snpid<-paste("rs",gwasCatalogNewTableSigCleanMerged$snpid,sep = "")

gwasCatalogNewTableSigCleanMerged<-subset(gwasCatalogNewTableSigCleanMerged,is.na(bp1_final)==FALSE)

gwasCatalogFinal<-data.frame(
  chr=gwasCatalogNewTableSigCleanMerged$chr,
  bp1=gwasCatalogNewTableSigCleanMerged$bp1_final,
  bp2=gwasCatalogNewTableSigCleanMerged$bp1_final,
  snpid=gwasCatalogNewTableSigCleanMerged$snpid,
  pvalue=gwasCatalogNewTableSigCleanMerged$pvalue,
  pmid=gwasCatalogNewTableSigCleanMerged$pmid,
  trait=gwasCatalogNewTableSigCleanMerged$trait
                             )
fileConn<-file("gwas.catalog_new.txt")
writeLines(c("## gwas.catalog",
             "## NHGRI/EBI GWAS catalog, gwsig findings",
             "## GRCh37/hg19, source=gwas_catalog_v1.0.2-associations_e92_r2018-05-03.tsv from https://www.ebi.ac.uk/gwas/docs/file-downloads, new GRCh37/hg19 bp coordinates from Ensembl rest api http://grch37.rest.ensembl.org/variation/human/",
             "## PGC bioinformatics, PF Sullivan & Johan Kallberg Zvrskovec, Karolinska Institutet, 2018",
             "## Rows: 5 comments, header, 28500 entries. Columns: 7",
             "#chr	bp1	bp2	snpid	pvalue	pmid	trait")
           , fileConn)
write.table(gwasCatalogFinal,file = "gwas.catalog_new.txt",quote = c(7),row.names = FALSE,sep = "\t",col.names = FALSE, append=TRUE)
