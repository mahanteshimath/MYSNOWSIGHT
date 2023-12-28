# MYSNOWSIGHT
   ![Alt Text](https://github.com/mahanteshimath/MYSNOWSIGHT/blob/main/MySnowsight.gif)


   ![Alt Text](https://github.com/mahanteshimath/MYSNOWSIGHT/blob/main/DocumentAI.gif)
## What's special about this app?
You can execute parallel queries in a single query window.

## Getting Started

  1. [App link](https://mysnowsight.streamlit.app/)

  2. [Youtube video](https://bit.ly/atozaboutdata) 

## code to try

#Step 1

```sql

CREATE OR REPLACE PROCEDURE SP_PARALLEL_1("P_LOAD_DATE" TIMESTAMP_NTZ(9))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE 
V_SP_NAME VARCHAR(50):='SP_PARALLEL_1';
ERR_MSG VARCHAR(500);
ERR_STEP VARCHAR(10);

BEGIN


create or replace TABLE PARALLEL_1 (
	SR_NO NUMBER(38,0),
	INSRT_TMS TIMESTAMP_NTZ(9)
);


CALL SYSTEM$WAIT(30);
INSERT INTO PARALLEL_1  (SELECT seq4()+1 SR_NO, CURRENT_TIMESTAMP FROM TABLE(GENERATOR(ROWCOUNT => 100)) T  ORDER BY 1);

 
RETURN :V_SP_NAME|| 'EXECUTED SUCCESSFULLY';
EXCEPTION
WHEN OTHER THEN
                       
RETURN 'ERROR IN CODE';                            
RAISE;                            
    
END;




CREATE OR REPLACE PROCEDURE SP_PARALLEL_2("P_LOAD_DATE" TIMESTAMP_NTZ(9))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE 
V_SP_NAME VARCHAR(50):='SP_PARALLEL_2';
ERR_MSG VARCHAR(500);
ERR_STEP VARCHAR(10);

BEGIN


create or replace TABLE PARALLEL_2 (
	SR_NO NUMBER(38,0),
	INSRT_TMS TIMESTAMP_NTZ(9)
);
CALL SYSTEM$WAIT(30);
INSERT INTO PARALLEL_2  (SELECT seq4()+1 SR_NO, CURRENT_TIMESTAMP FROM TABLE(GENERATOR(ROWCOUNT => 100)) T  ORDER BY 1);

 
RETURN :V_SP_NAME|| 'EXECUTED SUCCESSFULLY';
EXCEPTION
WHEN OTHER THEN
                       
RETURN 'ERROR IN CODE';                            
RAISE;                            
    
END;
```
#Step 2

```sql
CALL YT.MH.SP_PARALLEL_1(CURRENT_TIMESTAMP);
CALL YT.MH.SP_PARALLEL_2(CURRENT_TIMESTAMP);

SELECT TABLE_SCHEMA,
       TABLE_NAME,
       CREATED
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='MH' AND ( TABLE_NAME LIKE '%PARALLEL%');
```
