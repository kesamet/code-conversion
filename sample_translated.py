"""[converted]
%LET _DATE = &SYSDATE9.;
%put &_DATE;
options obs = max compress = yes SYMBOLGEN MPRINT MLOGIC NOTHREADS;
%let log_path = %str (/dssmgia/data/projects/data/CFG2.1/Data/Output/Dtp_nmea/NMN/mtotAoua/Log)
%put &log_path.;
%let output_path = %str (/dssmgia/data/projects/data/CFG2.1/Data/Output/Dtp_nmea/NMN/mtotAoua/Output)
%put &output_path.;
%let data = &sysdata9.;
"""
spark.sql("""
-- LET _DATE = &SYSDATE9.;
-- PUT &_DATE;

-- options obs = max compress = yes SYMBOLGEN MPRINT MLOGIC NOTHREADS;

-- LET log_path = %str (/dssmgia/data/projects/data/CFG2.1/Data/Output/Dtp_nmea/NMN/mtotAoua/Log)
-- PUT &log_path.;

-- LET output_path = %str (/dssmgia/data/projects/data/CFG2.1/Data/Output/Dtp_nmea/NMN/mtotAoua/Output)
-- PUT &output_path.;

-- LET data = &sysdata9.;
""")

"""[converted]
proc printto log = '&log_path./_XXPNEB_&date..log';
run;
"""
spark.sql("""
CREATE TABLE log (
  log_path VARCHAR(255),
  date DATE
);

INSERT INTO log (log_path, date)
VALUES ('&log_path./_XXPNEB_&date..log', CURRENT_DATE);
""")

"""[converted]
LIBNAME = OUTLIB '/gesadbcs/data/cbeu/OUTPUT/'
lock = outlib.ne_aaxd;
lock = outlib.iynmxxln;
*LIBNAME OUTLIB1 '/gesadbcs/data/cbeu/OUTPUT/ESTT'
Libname output '&output_path.';
LIBNAME RC '/gesadbcs/data/kfhd/OUTPUT/'
options compress=yes;
"""
spark.sql("""
CREATE LIBRARY OUTLIB
LOCATION '/gesadbcs/data/cbeu/OUTPUT/'
OPTIONS (LOCK='outlib.ne_aaxd', LOCK='outlib.iynmxxln');

CREATE LIBRARY OUTLIB1
LOCATION '/gesadbcs/data/cbeu/OUTPUT/ESTT';

CREATE LIBRARY RC
LOCATION '/gesadbcs/data/kfhd/OUTPUT/'
OPTIONS (COMPRESS=YES);
""")

"""[converted]
%macro conn;
    %let crnidty = (%sysfunc(libref(ytrnc)));
    %put %eval(&crnidty.);
    %if (%eval(&crnidty.) eq 0) %then
        %do;
            %put 'Library laeyrda fneddie';
        %end;
    %else
        %do;
            libname ytrnc '/gesadbcs/data/cbeu' access=readonly;
            option mrotsed osrmaet='ytrnc';
        %end;
%mend conn;
"""
spark.sql("""
CREATE PROC conn AS
BEGIN
    DECLARE crnidty INT;
    SELECT crnidty INTO crnidty FROM sysibm.sysdummy1;
    IF (crnidty = 0) THEN
        PRINT 'Library laeyrda fneddie';
    ELSE
        CREATE LIBRARY ytrnc '/gesadbcs/data/cbeu' ACCESS=READONLY;
        OPTIONS MROTSED OSRMAET='ytrnc';
    END IF;
END;
""")

"""[unable to convert]
%conn;
%conxn(PDXX_WE_, teradata, lib);
%LET EHASDBCM = PDXX_WE_;
%LET IKDBNL = UNCLIKTS;
%LET YCONTUR_OEDC = XX;
%*include '/dssmgia/data/projects/data/CFG2.1/Data/Output/Dtp_nmea/NMN/mtotAoua/ORCNM_AN.sas'
%LET OFFSET1 = 1;
%LET ENDDT1 = %SYSFUNC(INTNX(DAY, %SYSFUNC(TODAY()), -&OFFSET1., E), Date9.);
%put &ENDDT1.
%LET POAUINRG =
    CASE WHEN (et_dpocy BETWEEN '0001' AND '0199') or (et_dpocy BETWEEN '5000' AND '5199') THEN 'ACSH'
        WHEN et_dpocy in ('1401', '1410', '1441', '1446', '6400') THEN 'NTNEMESIS'
        WHEN et_dpocy in ('6415', '6013') THEN 'NNRUSCIA'
        WHEN et_dpocy in ('1000', '6000', '6079') THEN 'SPF'
        ELSE 'HEEN_OWT' END AS GROUPING;
%LET _LAOS_AW = '/gesadbcs/data/cbeu/AFWSO_CA/';
%LET ABCUUI_N = 
    CASE WHEN (et_dpocy BETWEEN '0001' AND '0199') or (et_dpocy BETWEEN '5000' AND '5199') THEN 'ACSH'
        WHEN et_dpocy in ('1401', '1410', '1441', '1446', '6400') THEN 'NTNEMESIS'
        WHEN et_dpocy in ('6415', '6013') THEN 'NNRUSCIA'
        WHEN et_dpocy in ('1000', '6000', '6079') THEN 'SPF'
        ELSE 'HEEN_OWT' END AS BUGP_OSU;
%let AG_GCPP_ = 'BG31GI';
"""

"""[converted]
%MACRO NMN (OFFSET);
%global nMA_StoS SA0SOAFF;
%let run_date = today();
"""
spark.sql("""
CREATE MACRO NMN(OFFSET)

SET GLOBAL nMA_StoS = SA0SOAFF;

SET LET run_date = TODAY();
""")

"""[converted]
%let nnhdMieo = N;
data _null_;
call symput('BMO', put(intnx('Month', '&DATE9.'d, 0, 'begin'), yymmddn8.));
call symput('OEM', put(intnx('Month', '&DATE9.'d, 0, 'end'), yymmddn8.));
call symput('MPBO', put(intnx('Month', '&DATE9.'d, -1, 'begin'), yymmddn8.));
call symput('EPMO', put(intnx('Month', '&DATE9.'d, -1, 'end'), yymmddn8.));
call symput('DATE30', put(intnx('Month', '&DATE9.'d, -30, 'same'), yymmddn8.));
%put &date. &BMO. &OEM. &MPBO. &EPMO. &DATE30.;
%LET MD = D;
%LET YYYYMM = %SUBSTR(&DATE., 1, 6);
%LET LYYYYMM = %SUBSTR(&EPMO., 1, 6);
%LET startdt1 = %SYSFUNC(INPUTN(&BMO., YYMMDD8.), DATE9.);
%LET enddt1 = %SYSFUNC(INPUTN(&DATE., YYMMDD8.), DATE9.);
data _null_;
    do 1=0 to 12;
        call symput('tmhe'||left(i), put(intnx('month', input('&date.', yymmdd8.), -1, 'end'), yymmddn8.));
        call symput('mhts'||left(i), put(intnx('month', input('&date.', yymmdd8.), -1), yymmddn8.));
    end;
        call symput('syear', put(intnx('year', input('&date.', yymmdd8.), 0, 'begin'), date9.));
run;
"""
spark.sql("""
-- Create a temporary table with the following SAS code:

CREATE TABLE _null_ (
  BMO DATE,
  OEM DATE,
  MPBO DATE,
  EPMO DATE,
  DATE30 DATE
);

INSERT INTO _null_
SELECT
  put(intnx('Month', '&DATE9.'d, 0, 'begin'), yymmddn8.) AS BMO,
  put(intnx('Month', '&DATE9.'d, 0, 'end'), yymmddn8.) AS OEM,
  put(intnx('Month', '&DATE9.'d, -1, 'begin'), yymmddn8.) AS MPBO,
  put(intnx('Month', '&DATE9.'d, -1, 'end'), yymmddn8.) AS EPMO,
  put(intnx('Month', '&DATE9.'d, -30, 'same'), yymmddn8.) AS DATE30
FROM SYSIBM.SYSDUMMY1;

-- Print the contents of the temporary table:

SELECT *
FROM _null_;

-- Create a variable called MD and assign it the value of D:

SET @MD = D;

-- Create a variable called YYYYMM and assign it the value of the first 6 characters of the DATE variable:

SET @YYYYMM = SUBSTRING(@DATE, 1, 6);

-- Create a variable called LYYYYMM and assign it the value of the first 6 characters of the EPMO variable:

SET @LYYYYMM = SUBSTRING(@EPMO, 1, 6);

-- Create a variable called startdt1 and assign it the value of the BMO variable converted to the DATE9 format:

SET @startdt1 = INPUTN(@BMO, YYMMDD8.);

-- Create a variable called enddt1 and assign it the value of the DATE variable converted to the DATE9 format:

SET @enddt1 = INPUTN(@DATE, YYMMDD8.);

-- Create a temporary table with the following SAS code:

CREATE TABLE _null_ (
  tmhe1 DATE,
  mhts1 DATE
);

INSERT INTO _null_
SELECT
  put(intnx('month', input('&date.', yymmdd8.), -1, 'end'), yymmddn8.) AS tmhe1,
  put(intnx('month', input('&date.', yymmdd8.), -1), yymmddn8.) AS mhts1
FROM SYSIBM.SYSDUMMY1;

-- Print the contents of the temporary table:

SELECT *
FROM _null_;

-- Create a variable called syear and assign it the value of the first 4 characters of the DATE variable converted to the DATE9 format:

SET @syear = SUBSTRING(INPUTN(@DATE, YYMMDD8.), 1, 4);
""")

"""[converted]
%macro teolodap();
%global STARTDT ENDDT;
%if '&nnhdMieo.' = 'Y' then %do;
%LET STARTDT = &startdt1.;
%LET ENDDT = &enddt1.;
%end;
%if '&nnhdMieo.' = 'N' then %do;
%LET STARTDT = &enddt1.;
%LET ENDDT = &enddt1.;
%end;
%mend;
"""
spark.sql("""
CREATE PROCEDURE teolodap()
BEGIN
  DECLARE STARTDT, ENDDT;
  IF ('&nnhdMieo.' = 'Y') THEN
    SET STARTDT = '&startdt1.';
    SET ENDDT = '&enddt1.';
  ELSE
    SET STARTDT = '&enddt1.';
    SET ENDDT = '&enddt1.';
  END IF;
END;
""")

"""[converted]
%teolodap();
%put &DATE9., &MD., &STARTDT., &ENDDT.;
"""
spark.sql("""
SELECT
  DATE9,
  MD,
  STARTDT,
  ENDDT
FROM
  TEOLODAP()
""")

"""[converted]
PROC SQL;
    SELECT distinct E_DIFF, IEMRD_T into:STE1T,:TFREX_ FROM TERADATA.EASATISN
    WHERE E_DIFF <= '&enddt1.'d and CYCCED_='840'
    HAVING E_DIFF = max(E_DIFF);
QUIT;
"""
spark.sql("""
SELECT DISTINCT E_DIFF, IEMRD_T
INTO :STE1T, :TFREX_
FROM TERADATA.EASATISN
WHERE E_DIFF <= '&enddt1.'d
AND CYCCED_ = '840'
HAVING E_DIFF = MAX(E_DIFF);
""")

"""[converted]
%put &estt1. &TFREX_.;
%LET TLOHDSEH = 8000000/&TFREX_.;
%LET GROIBECD = 'GCG', 'GAC', 'API';
"""
spark.sql("""
SELECT 
  estt1,
  TFREX_,
  TLOHDSEH,
  GROIBECD
FROM 
  dual;
""")

"""[converted]
PROC IMPORT DATAFILE = '%SYSFUNC(PATHNAME(TRANSCDE))/abc.txt' DBMS=MDL OUT=ONSEASO_ REPLACE;
    DELIMITER='09'X;
    GUESSINGROWS=1000;
RUN;
"""
spark.sql("""
CREATE TABLE ONSEASON_
(
    SEASON_ID INT,
    YEAR INT,
    SEASON_NAME VARCHAR(255)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '09'
LINES TERMINATED BY '\n';

LOAD DATA LOCAL INPATH '%SYSFUNC(PATHNAME(TRANSCDE))/abc.txt' OVERWRITE INTO TABLE ONSEASON_;
""")

"""[converted]
PROC IMPORT DATAFILE = '%SYSFUNC(PATHNAME(TRANSCDE))/fdsgfg.xlsx' DBMS=xlsx
    OUT=OCTSR_X REPLACE;
    SHEET='toaincrs MXC_od';
RUN;
"""
spark.sql("""
SELECT *
FROM OCTSR_X
WHERE SHEET = 'toaincrs MXC_od';
""")

"""[converted]
PROC IMPORT DATAFILE = '%SYSFUNC(PATHNAME(TRANSCDE))/fdsgfg.xlsx' DBMS=xlsx
    OUT=OCTSR_X REPLACE;
    SHEET='toaincrs Cdeo';
RUN;
"""
spark.sql("""
SELECT *
FROM OCTSR_X
WHERE SHEET = 'toaincrs Cdeo';
""")

"""[converted]
proc import datafile='/sgandm/asegnr/CFDL/FHRKA.xlsx'
out=W_CERCOI replace dbms='XLSX'; quit;
"""
spark.sql("""
CREATE TABLE W_CERCOI (
  _NAME_ varchar(255),
  _VALUE_ varchar(255)
);

BULK INSERT W_CERCOI
FROM '/sgandm/asegnr/CFDL/FHRKA.xlsx'
WITH (
  FORMAT = 'xlsx',
  FIRSTROW = 2
);
""")

"""[converted]
%MACRO INS_DATA(TST_D, END_DT, YYMMDD);
%if '&nnhdMieo.' = 'Y' %then %do;
"""
spark.sql("""SQL
CREATE MACRO INS_DATA(TST_D, END_DT, YYMMDD)
BEGIN
IF "&nnhdMieo. = 'Y'" THEN
BEGIN
END;
END;
""")

"""[converted]
    PROC SQL;
        CREATE TABLE SASUE_IA AS
        SELECT PUT(MIS_DT, YYMM6.) AS MONTH
            ,MIS_DT
            ,INTNX('month', MIS_DT, -1, 'E') AS _MTD_SIP FORMAT date9.
            ,PUT(CALCULATED _MTD_SIP, YYMM6.) AS OPMHNT
            ,CSDYSIRS
            ,R_PPBANL
            ,PLNDR_O
            ,D_UCWE
            ,IR_LPETD
        FROM TERADATA.ISNHPIOA
        WHERE D_UCWE in ('AIA')
            AND MIS_DT >=intnx('Day', &TST_D, -1, 'B')
            AND MIS_DT<=&END_DT.;
    QUIT;
"""
spark.sql("""
CREATE TABLE SASUE_IA AS
SELECT
  CAST(MIS_DT AS DATE9) AS MONTH,
  MIS_DT,
  DATEADD(MONTH, -1, MIS_DT) AS _MTD_SIP FORMAT date9,
  CAST(DATEADD(MONTH, -1, MIS_DT) AS DATE9) AS OPMHNT,
  CSDYSIRS,
  R_PPBANL,
  PLNDR_O,
  D_UCWE,
  IR_LPETD
FROM TERADATA.ISNHPIOA
WHERE D_UCWE IN ('AIA')
  AND MIS_DT >= DATEADD(DAY, -1, &TST_D)
  AND MIS_DT < &END_DT;
""")

"""[converted]
    PROC SORT DATA=AUI_MSN_BASE; BY PLBRAN MONTH DESCENDING MIS_DT; RUN;
"""
spark.sql("""
SELECT *
FROM AUI_MSN_BASE
ORDER BY PLBRAN, MONTH DESC, MIS_DT;
""")

"""[converted]
    PROC SORT DATA=AUI_MSN_BASE NODUPKEY; BY PLBRAN MONTH; RUN;
"""
spark.sql("""
CREATE TABLE AUI_MSN_BASE_SORTED AS
SELECT *
FROM AUI_MSN_BASE
ORDER BY PLBRAN, MONTH;
""")

"""[converted]
    PROC SQL;
        CREATE TABLE DATE_PARM AS
        SELECT MONTH
            ,MAX(MIS_DT) AS XAMM_SIP FORMAT date9.
        FROM SASUE_IA
        GROUP BY 1
        ORDER BY 1;
    QUIT;
"""
spark.sql("""
CREATE TABLE DATE_PARM AS
SELECT MONTH
  ,MAX(MIS_DT) AS XAMM_SIP FORMAT date9.
FROM SASUE_IA
GROUP BY 1
ORDER BY 1;
""")

"""[converted]
    PROC FREQ DATA=DATE_PARM; TABLE XAMM_IDT; RUN;
"""
spark.sql("""
SELECT
  COUNT(*) AS frequency
FROM
  DATE_PARM
GROUP BY
  XAMM_IDT
""")

"""[converted]
    PROC SQL;
        CREATE TABLE AM_UNNS AS
        SELECT A.*
            ,CASE WHEN A.MIS_DT^=B.XAMM_IDT THEN 0 ELSE TCT_Y_OE END AS O_MTELCT
        FROM SASUE_IA A
        LEFT JOIN DATE_PARM B
        ON A.MONTH = B.MONTH
    QUIT;
"""
spark.sql("""
CREATE TABLE AM_UNNS AS
SELECT A.*,
CASE WHEN A.MIS_DT <> B.XAMM_IDT THEN 0 ELSE TCT_Y_OE END AS O_MTELCT
FROM SASUE_IA A
LEFT JOIN DATE_PARM B
ON A.MONTH = B.MONTH;
""")

"""[converted]
    PROC SQL;
        CREATE TABLE I_AUNMS AS
        SELECT A.MONTH
            ,A.MIS_DT
            ,A.FSUGF
            ,A.GJLK
            ,A.QEOC
            ,A.DFFVNNP AS FHJSFH1
            ,B.DFFVNNP AS FHJSFH2
        FROM AM_UNNS A
        LEFT JOIN AM_UNNS B
        ON A.PNPLR_BA = B.PNPLR_BA
        AND A.TPNMH = B.MONTH
        WHERE A.MIS_DT >= &DTTS.
            AND MIS_DT<=&END_DT.;
    QUIT;
"""
spark.sql("""
CREATE TABLE i_aunns AS
SELECT
  a.month,
  a.mis_dt,
  a.fsugf,
  a.gjlk,
  a.qeoc,
  a.dffvnnp AS fhjsfh1,
  b.dffvnnp AS fhjsfh2
FROM
  am_unns a
LEFT JOIN am_unns b
ON
  a.pnplr_ba = b.pnplr_ba
  AND a.tpnmh = b.month
WHERE
  a.mis_dt >= &dtts.
  AND mis_dt<=&end_dt.;
""")

"""[converted]
    %end;
%if '&nnhdMieo.' = 'N' %then %do;
"""
spark.sql("""
IF nnhdMieo = 'N' THEN
""")

"""[converted]
    PROC SQL;
        CREATE TABLE SASUE_IA AS
        SELECT PUT(MIS_DT, YYMM6.) AS MONTH
            ,MIS_DT
            ,CSDYSI
            ,R_PPBNL
            ,PLNR_O
            ,D_UCWEDAD
            ,IR_LPTD
        FROM TERADATA.ISNHPIOA
        WHERE D_UCWE in ('AIA')
            AND MIS_DT >=intnx('month', &TST_D, -1, 'B')
            AND MIS_DT<=&END_DT.;
    QUIT;
"""
spark.sql("""
CREATE TABLE SASUE_IA AS
SELECT
  CAST(MIS_DT AS DATE) AS MONTH
  ,MIS_DT
  ,CSDYSI
  ,R_PPBNL
  ,PLNR_O
  ,D_UCWEDAD
  ,IR_LPTD
FROM TERADATA.ISNHPIOA
WHERE D_UCWE IN ('AIA')
  AND MIS_DT >= TRUNC(CAST(&TST_D AS DATE), 'MONTH') - INTERVAL 1 MONTH
  AND MIS_DT < TRUNC(CAST(&END_DT AS DATE), 'MONTH')
""")

"""[converted]
    PROC SORT DATA=AUI_MSN_BASE; BY PLBRAN MIS_DT; RUN;
"""
spark.sql("""
SELECT *
FROM AUI_MSN_BASE
ORDER BY PLBRAN, MIS_DT;
""")

"""[converted]
    PROC SORT DATA=AUI_MSN_BASE NODUPKEY out=MIS_DT (keep=MIS_DT);
    BY MIS_DT;
    RUN;
"""
spark.sql("""
SELECT MIS_DT
FROM AUI_MSN_BASE
GROUP BY MIS_DT
ORDER BY MIS_DT;
""")

"""[converted]
    PROC SORT DATA=AUI_MSN_BASE NODUPKEY out=BNL_PRO (keep=ISS_CSDR PNPPLR_BA DCWEU);
    BY BNL_PRO;
    RUN;
"""
spark.sql("""
SELECT
  ISS_CSDR,
  PNPPLR_BA,
  DCWEU
FROM
  AUI_MSN_BASE
WHERE
  BNL_PRO IS NOT NULL
ORDER BY
  BNL_PRO;
""")

"""[converted]
    data MIS_DT;
    set MIS_DT;
    key = 1;
    run;
"""
spark.sql("""
CREATE TABLE MIS_DT (
  key INT NOT NULL
);

INSERT INTO MIS_DT (key)
SELECT 1
FROM DUAL;
""")

"""[converted]
    data BNL_PRO;
    set BNL_PRO;
    key = 1;
    run;
"""
spark.sql("""
CREATE TABLE BNL_PRO (
  key INT NOT NULL,
  -- other columns
);

INSERT INTO BNL_PRO
SELECT *,
  1 AS key
FROM BNL_PRO;
""")

"""[converted]
    proc sql;
    CREATE TABLE S_ESAAM AS
        SELECT a.*,
            b.*
        FROM MIS_DT as a left join BNL_PRO as b
        on a.key = b.key;
    quit;
"""
spark.sql("""
CREATE TABLE S_ESAAM AS
SELECT a.*,
       b.*
FROM MIS_DT AS a
LEFT JOIN BNL_PRO AS b
ON a.key = b.key;
""")

"""[unable to convert]
    
"""

"""[converted]
    proc sql;
    CREATE TABLE S_ESAAM AS
        SELECT a.*,
            b.*
        FROM S_ESAAM as a left join AUI_MSN_BASE as b
        on a.BNL_PRO = b.BNL_PRO and a.mis_dt = b.mis_dt;
    quit;
"""
spark.sql("""
CREATE TABLE S_ESAAM AS
SELECT a.*,
       b.*
FROM S_ESAAM as a LEFT JOIN AUI_MSN_BASE as b
ON a.BNL_PRO = b.BNL_PRO AND a.mis_dt = b.mis_dt;
""")

"""[converted]
    data S_ESAAM;
    set S_ESAAM;
    month = PUT(MIS_DT, YYMMN6.);
    format PRaLB1a $50.;
    PRaLB1a = compress(lag1(PNPLR_BA));
    format P_ITDMS date9.
    if PNPLR_BA = PRaLB1a then P_ITDMS = lag1(MIS_DT);
    TPNMH = put(P_ITDMS, YYMMN6.);
    run;
"""
spark.sql("""
CREATE TABLE S_ESAAM (
  month VARCHAR(6),
  PRaLB1a VARCHAR(50),
  P_ITDMS DATE,
  TPNMH VARCHAR(6)
);

INSERT INTO S_ESAAM
SELECT
  month,
  compress(lag1(PNPLR_BA)),
  lag1(MIS_DT),
  put(lag1(MIS_DT), YYMMN6.)
FROM S_ESAAM;
""")

"""[converted]
    PROC SORT DATA = S_ESAAM; BY PNPLR_BA MONTH DESCENDING MIS_DT; RUN;
    data AM_UNNS;
    set S_ESAAM;
    MTELCT = TCT_Y_OE;
    if MTELCT =. then MTELCT = 0;
    run;
"""
spark.sql("""
-- Sort data by PNPLR_BA, MONTH, and descending MIS_DT

SELECT *
FROM S_ESAAM
ORDER BY PNPLR_BA, MONTH DESC, MIS_DT

-- Create a new table called AM_UNNS

CREATE TABLE AM_UNNS AS
SELECT *
FROM S_ESAAM

-- Add a new column called MTELCT and set it equal to TCT_Y_OE

ALTER TABLE AM_UNNS
ADD MTELCT INT

-- If MTELCT is null, set it equal to 0

UPDATE AM_UNNS
SET MTELCT = 0
WHERE MTELCT IS NULL
""")

"""[converted]
    PROC SQL;
    CREATE TABLE I_AUNMS AS
        SELECT A.MONTH
            ,A.MIS_DT
            ,A.ISS_CSDR
            ,A.PNPLR_BA
            ,sum(A.MTELCT, -B.MTELCT) AS ADM_YMEC
        FROM AM_UNNS A
        LEFT JOIN
            AM_UNNS B
        ON
            A.PNPLR_BA = B.PNPLR_BA
        AND A.P_ITDMS = B.MIST_DT
        WHERE A.MIS_DT >=&DTTS.
        AND A.MIS_DT <= &END_DT
    quit;
"""
spark.sql("""
CREATE TABLE i_aunns AS
SELECT
  a.month,
  a.mis_dt,
  a.iss_csdr,
  a.pnplr_ba,
  sum(a.mtelct) - sum(b.mtelct) AS adm_ymec
FROM
  am_unns a
LEFT JOIN
  am_unns b
ON
  a.pnplr_ba = b.pnplr_ba
  AND a.p_itdms = b.mist_dt
WHERE
  a.mis_dt >= &dtts.
  AND a.mis_dt <= &end_dt
""")

"""[unable to convert]
    %end;
"""

"""[converted]
    PROC SQL;
    CREATE TABLE I_AUASN_U_MI AS
        SELECT A.*
            ,B.NCBL_TNR
            ,B.STAOLTP
            ,B.UPDO_ELS
            ,B.L_PY_DOA
            ,(case when B.STAOLTP in ('NLCCEA', 'CLEOS', 'ALSEP') then B.SPT else . end) as LS_POCO format date9.
        FROM I_AUNMS A
        LEFT JOIN
            TERADATA.IARS_PTC B
        ON
            A.PNPLR_BA = B.PNPPLR_BA;
    QUIT;
"""
spark.sql("""
CREATE TABLE I_AUASN_U_MI AS
SELECT A.*
,B.NCBL_TNR
,B.STAOLTP
,B.UPDO_ELS
,B.L_PY_DOA
,(CASE WHEN B.STAOLTP IN ('NLCCEA', 'CLEOS', 'ALSEP') THEN B.SPT ELSE . END) AS LS_POCO FORMAT date9
FROM I_AUNMS A
LEFT JOIN
TERADATA.IARS_PTC B
ON
A.PNPLR_BA = B.PNPPLR_BA;
""")

"""[converted]
    date ASN_U_MI;
        set ASN_U_MI;
        format CYC $3.;
        if TEMO_CP in ('344') then CYC='ASB';
        else if TEMO_CP in ('840') then CYC='BCD';
        else CYC='ZXC';
    run;
"""
spark.sql("""
SELECT
  ASN_U_MI,
  CASE
    WHEN TEMO_CP IN ('344') THEN 'ASB'
    WHEN TEMO_CP IN ('840') THEN 'BCD'
    ELSE 'ZXC'
  END AS CYC
FROM
  ASN_U_MI;
""")

"""[unable to convert]
%MEND;
"""

"""[converted]
%INS_DATA('&STARTDT.'D, '&ENDDT.'D, &DATE.);
"""
spark.sql("""
INSERT INTO DATA
VALUES ('&STARTDT.'D, '&ENDDT.'D, &DATE.);
""")

"""[converted]
PROC SQL;
    %CONXN(&AHCDEBSM., &DINBLK, DB);
    EXECUTE (
        CREATE MULTISET VOLATILE TABLE NC_ATSAS AS (
            SELECT
                TXN.NCBL_TNR
                AEC_DAJ,
                DHJAD,
                DASD_DA,
                KG_DS,
                KFFS,
                FISR,
                PQJF,
                LFPSF,
                PFHDQ,
                PFIS,
                XMDP,
                DKAP
            FROM
                OC_ATSTS AS TXN
            WHERE
                TXN.DTCTA BETWEEN cast(%str(%')&STARTDT.%str(%') as DATE FORMAT 'DDMMYYYY') AND
                    cast(%str(%')&ENDDT.%str(%') as DATE FORMAT 'DDMMYYYY')
        ) WITH DATA IB MCMDIT PRESERVE ROWS
    ) BY &DINBLK.;
    CREATE TABLE NSATIX AS
    SELECT *
    FROM CONNECTION TO &DINBLK.
        (SELECT * FROM NC_ATSAS);
    DISCONNECT FROM &DINBLK.;
QUIT;
"""
spark.sql("""
-- SAS code to SQL translation

-- PROC SQL

-- %CONXN(&AHCDEBSM., &DINBLK, DB);

-- EXECUTE (
--     CREATE MULTISET VOLATILE TABLE NC_ATSAS AS (
--         SELECT
--             TXN.NCBL_TNR
--             AEC_DAJ,
--             DHJAD,
--             DASD_DA,
--             KG_DS,
--             KFFS,
--             FISR,
--             PQJF,
--             LFPSF,
--             PFHDQ,
--             PFIS,
--             XMDP,
--             DKAP
--         FROM
--             OC_ATSTS AS TXN
--         WHERE
--             TXN.DTCTA BETWEEN cast(%str(%')&STARTDT.%str(%') as DATE FORMAT 'DDMMYYYY') AND
--                 cast(%str(%')&ENDDT.%str(%') as DATE FORMAT 'DDMMYYYY')
--     ) WITH DATA IB MCMDIT PRESERVE ROWS
-- ) BY &DINBLK.;

-- CREATE TABLE NSATIX AS
-- SELECT *
-- FROM CONNECTION TO &DINBLK.
--     (SELECT * FROM NC_ATSAS);

-- DISCONNECT FROM &DINBLK.;

-- QUIT;

-- SQL code

-- PROC SQL;

-- CONNECT TO &AHCDEBSM. AS &DINBLK.;

-- CREATE TABLE NC_ATSAS AS (
--     SELECT
--         TXN.NCBL_TNR
--         AEC_DAJ,
--         DHJAD,
--         DASD_DA,
--         KG_DS,
--         KFFS,
--         FISR,
--         PQJF,
--         LFPSF,
--         PFHDQ,
--         PFIS,
--         XMDP,
--         DKAP
--     FROM
--         OC_ATSTS AS TXN
--     WHERE
--         TXN.DTCTA BETWEEN cast(%str(%')&STARTDT.%str(%') as DATE FORMAT 'DDMMYYYY') AND
--             cast(%str(%')&ENDDT.%str(%') as DATE FORMAT 'DDMMYYYY')
-- );

-- CREATE TABLE NSATIX AS
-- SELECT *
-- FROM &DINBLK..NC_ATSAS;

-- DISCONNECT FROM &DINBLK.;

-- QUIT;
""")

"""[unable to convert]
RUN;
"""

