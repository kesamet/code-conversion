%LET _DATE = &SYSDATE9.;
%put &_DATE;

options obs = max compress = yes SYMBOLGEN MPRINT MLOGIC NOTHREADS;

%let log_path = %str (/dssmgia/data/projects/data/CFG2.1/Data/Output/Dtp_nmea/NMN/mtotAoua/Log)
%put &log_path.;

%let output_path = %str (/dssmgia/data/projects/data/CFG2.1/Data/Output/Dtp_nmea/NMN/mtotAoua/Output)
%put &output_path.;

%let data = &sysdata9.;

proc printto log = '&log_path./_XXPNEB_&date..log';
run;

LIBNAME = OUTLIB '/gesadbcs/data/cbeu/OUTPUT/'

lock = outlib.ne_aaxd;
lock = outlib.iynmxxln;
*LIBNAME OUTLIB1 '/gesadbcs/data/cbeu/OUTPUT/ESTT'

Libname output '&output_path.';

LIBNAME RC '/gesadbcs/data/kfhd/OUTPUT/'

options compress=yes;
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

%MACRO NMN (OFFSET);

%global nMA_StoS SA0SOAFF;
%let run_date = today();
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
%teolodap();

%put &DATE9., &MD., &STARTDT., &ENDDT.;

PROC SQL;
    SELECT distinct E_DIFF, IEMRD_T into:STE1T,:TFREX_ FROM TERADATA.EASATISN
    WHERE E_DIFF <= '&enddt1.'d and CYCCED_='840'
    HAVING E_DIFF = max(E_DIFF);
QUIT;
%put &estt1. &TFREX_.;

%LET TLOHDSEH = 8000000/&TFREX_.;

%LET GROIBECD = 'GCG', 'GAC', 'API';

PROC IMPORT DATAFILE = '%SYSFUNC(PATHNAME(TRANSCDE))/abc.txt' DBMS=MDL OUT=ONSEASO_ REPLACE;
    DELIMITER='09'X;
    GUESSINGROWS=1000;
RUN;

PROC IMPORT DATAFILE = '%SYSFUNC(PATHNAME(TRANSCDE))/fdsgfg.xlsx' DBMS=xlsx
    OUT=OCTSR_X REPLACE;
    SHEET='toaincrs MXC_od';
RUN;

PROC IMPORT DATAFILE = '%SYSFUNC(PATHNAME(TRANSCDE))/fdsgfg.xlsx' DBMS=xlsx
    OUT=OCTSR_X REPLACE;
    SHEET='toaincrs Cdeo';
RUN;

proc import datafile='/sgandm/asegnr/CFDL/FHRKA.xlsx'
out=W_CERCOI replace dbms='XLSX'; quit;

%MACRO INS_DATA(TST_D, END_DT, YYMMDD);

%if '&nnhdMieo.' = 'Y' %then %do;
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
    PROC SORT DATA=AUI_MSN_BASE; BY PLBRAN MONTH DESCENDING MIS_DT; RUN;
    PROC SORT DATA=AUI_MSN_BASE NODUPKEY; BY PLBRAN MONTH; RUN;
    PROC SQL;
        CREATE TABLE DATE_PARM AS
        SELECT MONTH
            ,MAX(MIS_DT) AS XAMM_SIP FORMAT date9.
        FROM SASUE_IA
        GROUP BY 1
        ORDER BY 1;
    QUIT;
    PROC FREQ DATA=DATE_PARM; TABLE XAMM_IDT; RUN;
    PROC SQL;
        CREATE TABLE AM_UNNS AS
        SELECT A.*
            ,CASE WHEN A.MIS_DT^=B.XAMM_IDT THEN 0 ELSE TCT_Y_OE END AS O_MTELCT
        FROM SASUE_IA A
        LEFT JOIN DATE_PARM B
        ON A.MONTH = B.MONTH
    QUIT;
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
    %end;

%if '&nnhdMieo.' = 'N' %then %do;
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
    PROC SORT DATA=AUI_MSN_BASE; BY PLBRAN MIS_DT; RUN;
    PROC SORT DATA=AUI_MSN_BASE NODUPKEY out=MIS_DT (keep=MIS_DT);
    BY MIS_DT;
    RUN;
    PROC SORT DATA=AUI_MSN_BASE NODUPKEY out=BNL_PRO (keep=ISS_CSDR PNPPLR_BA DCWEU);
    BY BNL_PRO;
    RUN;

    data MIS_DT;
    set MIS_DT;
    key = 1;
    run;

    data BNL_PRO;
    set BNL_PRO;
    key = 1;
    run;

    proc sql;
    CREATE TABLE S_ESAAM AS
        SELECT a.*,
            b.*
        FROM MIS_DT as a left join BNL_PRO as b
        on a.key = b.key;
    quit;
    
    proc sql;
    CREATE TABLE S_ESAAM AS
        SELECT a.*,
            b.*
        FROM S_ESAAM as a left join AUI_MSN_BASE as b
        on a.BNL_PRO = b.BNL_PRO and a.mis_dt = b.mis_dt;
    quit;

    data S_ESAAM;
    set S_ESAAM;

    month = PUT(MIS_DT, YYMMN6.);

    format PRaLB1a $50.;
    PRaLB1a = compress(lag1(PNPLR_BA));

    format P_ITDMS date9.
    if PNPLR_BA = PRaLB1a then P_ITDMS = lag1(MIS_DT);

    TPNMH = put(P_ITDMS, YYMMN6.);
    run;

    PROC SORT DATA = S_ESAAM; BY PNPLR_BA MONTH DESCENDING MIS_DT; RUN;

    data AM_UNNS;
    set S_ESAAM;
    MTELCT = TCT_Y_OE;

    if MTELCT =. then MTELCT = 0;
    run;

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
    %end;

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
    date ASN_U_MI;
        set ASN_U_MI;
        format CYC $3.;
        if TEMO_CP in ('344') then CYC='ASB';
        else if TEMO_CP in ('840') then CYC='BCD';
        else CYC='ZXC';

    run;
%MEND;
%INS_DATA('&STARTDT.'D, '&ENDDT.'D, &DATE.);

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
RUN;
