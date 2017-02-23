#RegionAnnotator
##Source
https://github.com/ivankosmos/RegionAnnotator

##Data input needed
Daner clumps or similar generated from PGC GWAS Summary Statistics

##What the program does
Creates a number of tables with gene entries (rows) and psychiatric related annotations (columns). Includes gencode location, gene-based p-values, SFARI ASD annotation, GENCODE annotation, GWAS catalog annotation, OMIM annotation and manual curation among other things. Genes/regions are annotated based on an expanded (10 Mbases) segment overlap condition or a gene name comparison condition.

##What the data output will look like
Multiple .csv/.tsv-files or one MS excel-file or one json-file. Creates a Java H2 database file that can be accessed directly instead of file export.

##Algorithm

### Enter reference data, gene data or user data input
1. The input data is read from its source format.
2. The input data is checked against its pre-configured templates and is configured and completed from the templates. Templates contain information on table columns, column data types and output formatting.
3. The input data rows are read into tables. Reference data is read into tables that are named corresponding to their file names or excel-sheets, prefixed by `_`. Gene data is read into a table named GENE\_MASTER. User data is read into a table named \_USER\_INPUT.
4. Templated columns are indexed for improved database performance.

### Operate
The program runs operation actions after every user input.

- TwoSegmentOverlapcondition(a0,a1,b0,b1) = ``((a0<=b0 AND b0<=a1) OR (a0<=b1 AND b1<=a1) OR (b0<=a0 AND a0<=b1) OR (b0<=a1 AND a1<=b1))``

1. Computes an enriched version of the user input in the table USER\_INPUT.
  - location : A coordinate composed of the chromosome (chr) and the basepair coordinates (bp1, bp2) that has been formatted into a string. A comma is used as a 3-character separator in the basepari coordinates.
  - UCSC\_LINK : A (MS Excel) hyperlink to UCSC Genome Browser on Human Feb. 2009 (GRCh37/hg19) Assembly.
2. Computes an enriched version of the GENE\_MASTER (g) table in GENE\_MASTER\_EXPANDED. Expanded basepair coordinates are calculated, one expanding 20 kbases, and one 10 Mbases.
  - bp1s20k_gm = ``(g.bp1-20000)``
  - bp2a20k_gm = ``(g.bp2+20000)``
  - bp1s10m_gm = ``(g.bp1-10e6)``
  - bp2a10m_gm = ``(g.bp2+10e6)``
3. Creates a joined table PROTEIN\_CODING\_GENES\_ALL of user input and protein coding genes from \_USER\_INPUT (c) and GENE\_MASTER\_EXPANDED (g) fulfilling the condition of
	``g.ttype='protein_coding' AND c.chr=g.chr AND TwoSegmentOverlapCondition(c.bp1,c.bp2,g.bp1s10m_gm,g.bp2a10m_gm)``
, that is: protein coding genes that fulfill the overlap condition, between user input regions and gene coordinates that were expanded 10MBases.
  - dist= ``CASE WHEN TwoSegmentOverlapCondition(c.bp1,c.bp2,g.bp1,g.bp2) THEN 0 WHEN c.bp1 IS NULL OR c.bp2 IS NULL THEN 9e9 ELSE NUM_MAX_INTEGER(ABS(c.bp1-g.bp2),ABS(c.bp2-g.bp1)) END)``
4. Creates a view PROTEIN\_CODING\_GENES from PROTEIN\_CODING\_GENES\_ALL
``WHERE dist<100000``
5. Creates all output datasets:
  - GWAS\_CATALOG by joining \_USER\_INPUT (c) and the reference \_GWAS\_CATALOG (r)
on ``c.chr=r.chr AND TwoSegmentOverlapCondition(c.bp1, c.bp2, r.bp1, r.bp2)``
  - OMIM by joining GENES\_PROTEIN\_CODING\_NEAR (g) and the reference \_OMIM (r)
on ``g.genename_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''``
  - PSYCHIATRIC\_CNVS by joining \_USER\_INPUT (c) and the reference \_PSYCHIATRIC\_CNVS (r)
on ``c.chr=r.chr AND TwoSegmentOverlapCondition(c.bp1, c.bp2, r.bp1, r.bp2)``
  - ASD\_GENES by joining GENES\_PROTEIN\_CODING\_NEAR (g) and the reference \_ASD\_GENES (r)
on ``g.genename_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''``
  - ID\_DEVDELAY\_GENES by joining GENES\_PROTEIN\_CODING\_NEAR (g) and the reference \_ID\_DEVDELAY\_GENES (r)
on ``g.genename_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''``
  - MOUSE\_KNOCKOUT by joining GENES\_PROTEIN\_CODING\_NEAR (g) and the reference \_MOUSE\_KNOCKOUT (r)
on ``g.genename_gm=r.geneName AND g.geneName_gm IS NOT NULL AND g.geneName_gm!='' AND r.geneName IS NOT NULL AND r.geneName!=''``

### Output
1. The chosen tables are outputted to the chosen file(s) and in the chosen format.
2. Output is automatically done after user input.