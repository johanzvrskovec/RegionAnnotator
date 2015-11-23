CALL SQLJ.REPLACE_JAR('C:\Media\work\eclipse\gwas_bioinf\target\GwasBioinf-0.0.1-SNAPSHOT.jar', 'GwasBioinf');

DROP FUNCTION stringSeparateFixedSpacing;

CREATE FUNCTION stringSeparateFixedSpacing
(
	target VARCHAR(32672),
	separator VARCHAR(50),
	spacing	INT
)
RETURNS VARCHAR(32672)
PARAMETER STYLE JAVA
NO SQL LANGUAGE JAVA
EXTERNAL NAME 'DbUtils.stringSeparateFixedSpacing';