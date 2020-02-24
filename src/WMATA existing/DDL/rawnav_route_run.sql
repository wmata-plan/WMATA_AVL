--------------------------------------------------------
--  File created - Tuesday-February-11-2020   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table RAWNAV_ROUTE_RUN
--------------------------------------------------------

  CREATE TABLE "PLANAPI"."RAWNAV_ROUTE_RUN" 
   (	"ID" NUMBER, 
	"ROUTE_PATTERN" VARCHAR2(20 BYTE), 
	"BUS_ID" VARCHAR2(20 BYTE), 
	"THE_DATE" VARCHAR2(20 BYTE), 
	"THE_TIME" VARCHAR2(20 BYTE), 
	"SVC_DATE" VARCHAR2(20 BYTE), 
	"CORRECTED" NUMBER, 
	"INVALID" NUMBER
   ) SEGMENT CREATION IMMEDIATE 
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "PLANAPI_T" ;

   COMMENT ON COLUMN "PLANAPI"."RAWNAV_ROUTE_RUN"."CORRECTED" IS '1 = corrected run';
   COMMENT ON COLUMN "PLANAPI"."RAWNAV_ROUTE_RUN"."INVALID" IS '1 = run detected as invalid using certain rules in the loader';
