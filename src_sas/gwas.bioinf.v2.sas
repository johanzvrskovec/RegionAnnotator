*====== gwas2.bioinf.v2.sas by pfs 10/2015 ======;
* pgc mdd2 + 23andMe.v3 + gera + converge;
* table & bioinformatics;
* FRQ_A_44194   FRQ_U_110741;

*====== initial stuff ======;
* %include '~/routines/macro_include.sas';
* libname dat 'C:\Media\work\eclipse\gwas_bioinf\dat_sas';
libname gm 'C:\Media\work\eclipse\gwas_bioinf\dat_sas\gm'; * ~/GenomeMaster/_data/;
libname md 'C:\Media\work\eclipse\gwas_bioinf\dat_sas\md'; * ~/pgc/mdd2/data/;




*====== set up table ======;

* NOTE - this is the data input section. Is not general. ;
* needs to be made general;
* here, input is a list of regions, r0:r1-r2 (r0=chromosome, r1-r2 is hg19 base range);

proc import datafile="C:\Media\work\eclipse\gwas_bioinf\dat_sas\bioinformatics_pgc_mdd2_23me_gera_conv.xlsx" out=md.mdd2clumpraw dbms=xlsx replace;
sheet="table";
guessingrows=32767;
run;

* Export existing data *;
/*
proc export data=md.mdd2clumpraw outfile="C:\Media\work\eclipse\gwas_bioinf\input\md.xlsx" dbms=xlsx replace;
sheet="mdd2clumpraw";
run;
proc export data=gm.aut_loci_hg19 outfile="C:\Media\work\eclipse\gwas_bioinf\input\gm.xlsx" dbms=xlsx replace;
sheet="aut_loci_hg19";
run;
proc export data=gm.g1000sv outfile="C:\Media\work\eclipse\gwas_bioinf\input\gm.xlsx" dbms=xlsx replace;
sheet="g1000sv";
run;
proc export data=gm.gencode_master outfile="C:\Media\work\eclipse\gwas_bioinf\input\gm.xlsx" dbms=xlsx replace;
sheet="gencode_master";
run;
proc export data=gm.gproteincoupledreceptors outfile="C:\Media\work\eclipse\gwas_bioinf\input\gm.xlsx" dbms=xlsx replace;
sheet="gproteincoupledreceptors";
run;
proc export data=gm.id_devdelay outfile="C:\Media\work\eclipse\gwas_bioinf\input\gm.xlsx" dbms=xlsx replace;
sheet="id_devdelay";
run;
proc export data=gm.mouse outfile="C:\Media\work\eclipse\gwas_bioinf\input\gm.xlsx" dbms=xlsx replace;
sheet="mouse";
run;
proc export data=gm.nhgri_gwas outfile="C:\Media\work\eclipse\gwas_bioinf\input\gm.xlsx" dbms=xlsx replace;
sheet="nhgri_gwas";
run;
proc export data=gm.omim outfile="C:\Media\work\eclipse\gwas_bioinf\input\gm.xlsx" dbms=xlsx replace;
sheet="omim";
run;
proc export data=gm.psych_cnv_hg19 outfile="C:\Media\work\eclipse\gwas_bioinf\input\gm.xlsx" dbms=xlsx replace;
sheet="psych_cnv_hg19";
run;
proc export data=gm.psych_linkage outfile="C:\Media\work\eclipse\gwas_bioinf\input\gm.xlsx" dbms=xlsx replace;
sheet="psych_linkage";
run;
*/

data candidate;
  length snpid $20 a1a2 $6 ll $40 or p 8 cc $40 ucsc $200 r0 $5;
  set md.mdd2clumpraw;
  if .<p<1e-5;
  ll=catx('-',put(six1,comma15.0),put(six2,comma15.0));
  r0=hg19chrc; r1=six1; r2=six2;
  cc=cats(hg19chrc,':',put(six1,comma14.0),'-',put(six2,comma14.0));
  ucsc=cats('=HYPERLINK("http://genome.ucsc.edu/cgi-bin/hgTracks?&org=Human&db=hg19&position=',hg19chrc,"%3A",six1,"-",six2,'","ucsc")' );
  format p e9.2 or se 8.3;
  *drop ll clump;
run;
proc sort data=candidate; by p;
data candidate; length rank 8; set candidate; rank=_n_;
proc sort data=candidate; by chr six1 six2;
run;

*====== genes for bioinformatics ======;
*=== Genes: GENCODE v17 genes in bin, expand by 20kb;
data t2; set gm.gencode_master; e4=f4-20000; e5=f5+20000; run;
proc sql;
  create table t3 as 
	select * from candidate INNER join t2
  on r0=f1 & (r1<=e4<=r2 | r1<=e5<=r2 | e4<=r1<=e5 | e4<=r2<=e5)
;quit;
data allgenes20kb;
  set t3;
  rename f1=g0 f4=g1 f5=g2 f7=gstr gid=ensgene;
  keep rank p hg19chrc six1 six2 geneName--hugoAlias;
run;
%dump(t2 t3);

*====== PC genes & distance ======;
* expand by 10mb;
data r1; set gm.gencode_master;
  if ttype='protein_coding';
  e4=f4-10e6; e5=f5+10e6;
run;
* join;
proc sql;
  create table r2 (keep=rank p hg19chrc six1 six2 r0--hugoalias) as select *
  from candidate LEFT join r1
  on r0=f1 & (r1<=e4<=r2 | r1<=e5<=r2 | e4<=r1<=e5 | e4<=r2<=e5)
;quit;
* classify;
data r3; set r2;
       if r1<=f4<=r2 | r1<=f5<=r2 then dist=0;
  else if f4<=r1<=f5 | f4<=r2<=f5 then dist=0;
  else if n(f4,f5)^=2 then dist=9e9;
  else dist=min(abs(r1-f4),abs(r2-f4),abs(r1-f5),abs(r2-f5));
run;
proc sort data=r3; by rank dist;
data genesPCnear;
  set r3;
  by rank;
  r2=dist;  * for column position;
  if dist=0 then output;
  else if dist<100000 then output;
  else if first.rank then output;
  rename r2=distance;
  keep rank--six2 r2 geneName--hugoAlias;
run;
%dump(r:);

*=== nhgri gwas;
proc sql;
  create table nhgri (drop=r0 r1 r2 link ispooled--hg19chrom) as select *
  from candidate (keep=rank p hg19chrc six1 six2 r0 r1 r2) as aa INNER join gm.nhgri_gwas as bb
  on aa.r0=bb.hg19chrom & r1<=bp<=r2
;quit;

*=== omim;
proc sql;
  create table omim (keep=rank p geneName hugoproduct type) as select *
  from genesPCnear as aa INNER join gm.omim (keep=geneName type) as bb
  on aa.geneName=bb.geneName
;quit;

*=== aut_loci_hg19 ;
proc sql;
  create table aut (keep=rank p geneName hugoproduct) as select *
  from genesPCnear as aa INNER join gm.aut_loci_hg19 (where=(geneName^='')) as bb
  on aa.geneName=bb.geneName
;quit;

*=== id/dev delay ;
proc sql;
  create table iddd (keep=rank p geneName hugoproduct) as select *
  from genesPCnear as aa INNER join gm.id_devdelay (keep=geneName where=(geneName^='')) as bb
  on aa.geneName=bb.geneName
;quit;

*=== jax;
proc sql;
  create table jax (keep=rank p geneName hugoproduct musName KOphenotype) as select *
  from genesPCnear as aa INNER join gm.mouse as bb
  on aa.geneName=bb.geneName
;quit;

*=== psych CNVs;
proc sql;
  create table cnv (keep=rank p hg19chrc six1 six2 dz--note) as select *
  from candidate as aa INNER join gm.psych_cnv_hg19 as bb
  on aa.r0=bb.c0 & ( r1<=c1<=r2 | r1<=c2<=r2 | c1<=r1<=c2 | c1<=r2<=c2 )
;quit;
data cnv; set cnv;
run;

*=== g1000 sv;
proc sql;
  create table g1000sv (keep=rank p hg19chrc six1 six2 bp1--afraf) as select *
  from candidate as aa INNER join gm.g1000sv (where=(euraf>0.01)) as bb
  on aa.r0=bb.hg19chrc & ( r1<=bp1<=r2 | r1<=bp2<=r2 | bp1<=r1<=bp2 | bp1<=r2<=bp2 )
;quit;
data g1000sv; set g1000sv;
  recipoverlap=(min(six2,bp2)-max(six1,bp1))/(max(six2,bp2)-min(six1,bp1));
  length g1000sv $200;
  g1000sv=cats('=HYPERLINK("http://genome.ucsc.edu/cgi-bin/hgTracks?&org=Human&db=hg19&position=',hg19chrc,"%3A",bp1,"-",bp2,'","g1000sv")' );
run;

*=== GPCRs;
proc sql;
  create table gpcr (keep=rank--geneName hugoproduct--family) as select *
  from genesPCnear as aa INNER join gm.gproteincoupledreceptors as bb
  on aa.geneName=bb.geneName
;quit;

*=== psych linkage meta;
proc sql;
  create table linkage (keep=rank p hg19chrc six1 six2
    group--year scoretype scorestat pval bp1 bp2) as select *
  from candidate as aa INNER join gm.psych_linkage (where=(study='GWL' & type='MetaAnal')) as bb
  on aa.r0=bb.hg19chrom & ( r1<=bp1<=r2 | r1<=bp2<=r2 | bp1<=r1<=bp2 | bp1<=r2<=bp2 )
;quit;
data linkage; set linkage;
  recipoverlap=(min(six2,bp2)-max(six1,bp1))/(max(six2,bp2)-min(six1,bp1));
run;

*=== export ===;
%macro xo(dset,srt);
proc sort data=&dset.; by &srt.;
%exportTXT(&dset.);
%mend;

%xo(candidate,rank);
%xo(allgenes20kb,rank geneName);
%xo(genesPCnear,rank geneName);
%xo(nhgri,rank six1 six2);
%xo(omim,rank geneName);
%xo(aut,rank geneName);
%xo(iddd,rank geneName);
%xo(jax,rank geneName);
%xo(cnv,rank c1 c2 type dz);
%xo(g1000sv,rank bp1 bp2);
%xo(gpcr,rank geneName);
%xo(linkage,rank bp1 bp2);
