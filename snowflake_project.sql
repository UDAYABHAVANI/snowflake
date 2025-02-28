--RBAC

SNOWFLAKE_SAMPLE_DATA-- Create users
CREATE USER admin_user PASSWORD = '12345';
CREATE USER analyst_user PASSWORD = '123456';
CREATE USER engineer_user PASSWORD = '1234567';
CREATE USER viewer_user PASSWORD = '1234567';


CREATE USER admin_user PASSWORD = 'StrongPassword1';
CREATE USER analyst_user PASSWORD = 'StrongPassword2';
CREATE USER engineer_user PASSWORD = 'StrongPassword3';
CREATE USER viewer_user PASSWORD = 'StrongPassword4';

CREATE ROLE MOVIE_ADMIN;
CREATE ROLE MOVIE_ANALYST;
CREATE ROLE MOVIE_ENGINEER;
CREATE or replace ROLE MOVIE_VIEWER;

GRANT ROLE MOVIE_ADMIN TO USER admin_user;
GRANT ROLE MOVIE_ANALYST TO USER analyst_user;
GRANT ROLE MOVIE_ENGINEER TO USER engineer_user;
GRANT ROLE MOVIE_VIEWER TO USER viewer_user;

GRANT USAGE ON WAREHOUSE MOVIE_WH TO ROLE USERADMIN;
GRANT OPERATE ON WAREHOUSE MOVIE_WH TO ROLE USERADMIN;
GRANT USAGE ON WAREHOUSE MOVIE_WH TO ROLE SYSADMIN;
GRANT OPERATE ON WAREHOUSE MOVIE_WH TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE MOVIE_WH TO ROLE SECURITYADMIN;
GRANT OPERATE ON WAREHOUSE MOVIE_WH TO ROLE SECURITYADMIN;

GRANT MODIFY ON WAREHOUSE MOVIE_WH TO ROLE USERADMIN;
GRANT MODIFY ON WAREHOUSE MOVIE_WH TO ROLE SECURITYADMIN;
--1)user_admin
GRANT ROLE SYSADMIN TO ROLE MOVIE_ADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE MOVIE_ADMIN;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE MOVIE_ADMIN;
GRANT MONITOR USAGE ON ACCOUNT TO ROLE MOVIE_ADMIN;

--2) movie_engineer
GRANT USAGE, OPERATE ON WAREHOUSE MOVIE_WH TO ROLE MOVIE_ENGINEER;
GRANT USAGE ON DATABASE MOVIE_RECOMMENDATION TO ROLE MOVIE_ENGINEER;
GRANT USAGE ON SCHEMA MOVIE_RECOMMENDATION.MOVIE_SCHEMA TO ROLE MOVIE_ENGINEER;
GRANT USAGE ON SCHEMA MOVIE_RECOMMENDATION.CLEANED_SCHEMA TO ROLE MOVIE_ENGINEER;


GRANT CREATE TABLE, create file format, CREATE STAGE, create clone, CREATE PIPE, CREATE STREAM, CREATE TASK 
ON SCHEMA MOVIE_RECOMMENDATION.MOVIE_SCHEMA TO ROLE MOVIE_ENGINEER;
GRANT CREATE TABLE, CREATE STAGE, create clone, CREATE PIPE, CREATE STREAM, CREATE TASK 
ON SCHEMA MOVIE_RECOMMENDATION.CLEANED_SCHEMA TO ROLE MOVIE_ENGINEER;

GRANT INSERT ON TABLE CLEANED_SCHEMA.MOVIES_CLEANED TO ROLE movie_engineer;
GRANT SELECT ON ALL TABLES IN SCHEMA MOVIE_RECOMMENDATION.CLEANED_SCHEMA TO ROLE MOVIE_ENGINEER;
GRANT SELECT ON ALL TABLES IN SCHEMA MOVIE_RECOMMENDATION.MOVIE_SCHEMA TO ROLE MOVIE_ENGINEER;







--3)Analyst user to movie_analyst_role
GRANT USAGE ON DATABASE MOVIE_RECOMMENDATION TO ROLE MOVIE_ANALYST;
GRANT USAGE ON SCHEMA MOVIE_RECOMMENDATION.MOVIE_SCHEMA TO ROLE MOVIE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA MOVIE_RECOMMENDATION.CLEANED_SCHEMA TO ROLE MOVIE_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA MOVIE_RECOMMENDATION.ANALYSIS_SCHEMA TO ROLE MOVIE_ANALYST;
GRANT usage ON SCHEMA MOVIE_RECOMMENDATION.ANALYSIS_SCHEMA TO ROLE MOVIE_ANALYST;
GRANT USAGE ON SCHEMA MOVIE_RECOMMENDATION.CLEANED_SCHEMA TO ROLE MOVIE_ANALYST;
GRANT USAGE ON WAREHOUSE MOVIE_WH TO ROLE MOVIE_ANALYST;
--GRANT operate on SCHEMA MOVIE_RECOMMENDATION.ANALYSIS_SCHEMA TO ROLE MOVIE_ANALYST;
Grant create view on schema MOVIE_RECOMMENDATION.ANALYSIS_SCHEMA to ROLE MOVIE_ANALYST;


--4)Movie_viewer

GRANT USAGE ON DATABASE MOVIE_RECOMMENDATION TO ROLE MOVIE_VIEWER;
GRANT USAGE ON SCHEMA MOVIE_RECOMMENDATION.MOVIE_SCHEMA TO ROLE MOVIE_VIEWER;
GRANT USAGE ON SCHEMA MOVIE_RECOMMENDATION.cleaned_schema TO ROLE MOVIE_VIEWER;
GRANT USAGE ON SCHEMA MOVIE_RECOMMENDATION.analysis_schema TO ROLE MOVIE_VIEWER;
GRANT USAGE ON SCHEMA MOVIE_RECOMMENDATION.viewer_schema TO ROLE MOVIE_VIEWER;
GRANT SELECT ON ALL TABLES IN SCHEMA MOVIE_RECOMMENDATION.MOVIE_SCHEMA TO ROLE MOVIE_VIEWER;
GRANT SELECT ON ALL TABLES IN SCHEMA MOVIE_RECOMMENDATION.cleaned_schema TO ROLE MOVIE_VIEWER;
GRANT SELECT ON ALL VIEWS IN SCHEMA MOVIE_RECOMMENDATION.analysis_schema
TO role movie_viewer;

GRANT SELECT ON ALL VIEWS IN SCHEMA MOVIE_RECOMMENDATION.analysis_schema
TO role movie_viewer;


-- Grant minimal warehouse access for queries
GRANT USAGE ON WAREHOUSE MOVIE_WH TO ROLE MOVIE_VIEWER;

--granting all roles to sysadmin to create role hierarchy
GRANT ROLE MOVIE_ANALYST TO ROLE SYSADMIN;
GRANT ROLE MOVIE_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE MOVIE_VIEWER TO ROLE SYSADMIN;

SELECT CURRENT_ROLE();
GRANT EXECUTE TASK ON ACCOUNT TO ROLE movie_engineer;
GRANT EXECUTE TASK ON DATABASE movie_recommendation TO ROLE MOVIE_ENGINEER;
-- Or, for a specific task:
GRANT EXECUTE TASK ON TASK mytask TO ROLE MY_ROLE;
SHOW GRANTS TO ROLE MY_ROLE;

---REPLICATION

CREATE SHARE MOVIE_RECOMMENDATION_SHARE;
GRANT USAGE ON DATABASE MOVIE_RECOMMENDATION TO SHARE MOVIE_RECOMMENDATION_SHARE;
GRANT REPLICATION ON DATABASE MOVIE_RECOMMENDATION TO account 'YMBUIXJ.AL44903';

GRANT REPLICATION ON DATABASE MOVIE_RECOMMENDATION TO SHARE MOVIE_RECOMMENDATION_SHARE;

GRANT SELECT ON ALL SCHEMAS IN DATABASE MOVIE_RECOMMENDATION TO SHARE MOVIE_RECOMMENDATION_SHARE;

ALTER SHARE MOVIE_RECOMMENDATION_SHARE ADD ACCOUNT='YMBUIXJ.AL44903';

GRANT REPLICATION TO ROLE ACCOUNTADMIN;

--ADMIN_USER
DATABASE
create or replace database MOVIE_RECOMMENDATION;
create or replace schema MOVIE_SCHEMA ;
create or replace schema CLEANED_SCHEMA;
CREATE or replace SCHEMA ANALYSIS_SCHEMA;
CREATE or replace SCHEMA VIEWER_SCHEMA;

--ENGINEER_USER
clustered
CREATE or replace TABLE CLEANED_SCHEMA.MOVIES_CLEANED_CLUSTERED CLUSTER BY (RELEASE_DATE, GENRES) AS
SELECT * FROM CLEANED_SCHEMA.MOVIES_CLEANED;

select * from movies_cleaned_clustered;

--cloning
--Clone for Testing Data Cleaning Logic: Clone the MOVIE TABLE to experiment with cleaning transformations


CREATE TABLE MOVIES_CLONE CLONE MOVIES;
select * from movies_clone;

---Clone for Testing Analysis: Clone the MOVIES_CLEANED TABLE to test new queries or analysis workfloW


CREATE TABLE MOVIES_CLEANED_CLONE CLONE MOVIES_CLEANED;
select * from movies_cleaned_clone;
TIME TRAVEL
--Querying the state of the MOVIES_CLEANED table as it existed 3 hours ago.


SELECT * 
FROM CLEANED_SCHEMA.MOVIES_CLEANED 
AT (TIMESTAMP => CURRENT_TIMESTAMP - INTERVAL '3 HOURS');

--Restore a dropped table:
UNDROP TABLE MOVIE_SCHEMA.MOVIES;


----Querying the state of the Moreinfo_CLEANED table using query id .

 SELECT * FROM MOREINFO_CLEANED  before (STATEMENT => '01b98bb5-0000-d331-0008-78d200039032');
 
 
--01b99181-0000-d35e-0008-78d20003a26e
SELECT * FROM MOREINFO_CLEANED  before (STATEMENT => '01b99181-0000-d35e-0008-78d20003a26e');

SCD TYPE 2
select * from moreinfo where id = 2;
select * from cleaned_schema.moreinfo_cleaned where id = 2;


 UPDATE movie_SCHEMA.moreinfo
SET
    RUNTIME ='2',
    BUDGET =26666666,
    REVENUE = 2666666
WHERE id = 2;


SELECT 
    source.ID,
    REPLACE(REPLACE(source.Budget, '$', ''), ',', '')::NUMERIC AS Cleaned_Budget,
    REPLACE(REPLACE(target.Budget, '$', ''), ',', '')::NUMERIC AS Target_Cleaned_Budget
FROM movie_SCHEMA.MOREINFO AS source
LEFT JOIN CLEANED_SCHEMA.MOREINFO_CLEANED AS target
ON source.ID = target.ID
WHERE 
    target.Is_Current = 'Y'
    AND (
        REPLACE(REPLACE(source.Budget, '$', ''), ',', '')::NUMERIC <> REPLACE(REPLACE(target.Budget, '$', ''), ',', '')::NUMERIC
    );



    UPDATE CLEANED_SCHEMA.MOREINFO_CLEANED
SET 
    Effective_End_Date = CURRENT_DATE,
    Is_Current = 'N'
WHERE 
    ID = 2
    AND Is_Current = 'Y';


     INSERT INTO CLEANED_SCHEMA.MOREINFO_CLEANED 
(ID, Runtime, Budget,REVENUE, Film_ID, Effective_Start_Date, Effective_End_Date, Is_Current)
SELECT 
    2, 
 2,
 26666666, 
    2666666,
    2,
    CURRENT_DATE, 
    NULL, 
    'true';
    

    SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE Is_Current = 'y' and id = 2;


SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 2 and Is_Current = 'N'
ORDER BY Effective_Start_Date ;


SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 2 
ORDER BY Effective_Start_Date ;

select 
 *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 2 ;

INSERT
INSERT INTO MOVIES (id,
    title ,
    genres ,
    language ,
    user_score ,
    runtime_hour ,
    runtime_min ,
    release_date ,
    vote_count )
VALUES (9719 , 'pushpa 2' , 'action','en',8.8,3,22,'2024-12-05',34242);

select * from  CLEANED_SCHEMA.MOVIES_CLEANED where id = 9719 ;

INSERT INTO MOVIES (id,
    title ,
    genres ,
    language ,
    user_score ,
    runtime_hour ,
    runtime_min ,
    release_date ,
    vote_count )
VALUES (9720 , 'pushpa' , 'action','en',8.7,2,45,'2021-12-25',34222);

INSERT INTO MOVIES (id,
    title ,
    genres ,
    language ,
    user_score ,
    runtime_hour ,
    runtime_min ,
    release_date ,
    vote_count )
VALUES (9721 , 'RRR' , 'action','te',8.7,2,46,'2021-11-25',34223);


select * from movie_stream;

select * from movies_cleaned where id = 9721;
select * from movies;

SHOW TASKS IN SCHEMA CLEANED_SCHEMA;

insert into filmdetails  (id  ,
    director ,
    top_billed,
    budget_usd ,
    revenue_usd )
    values(9719,'rajamouli','prabhas,anushka,tamannah',234567,342567);


    insert into filmdetails  (id  ,
    director ,
    top_billed,
    budget_usd ,
    revenue_usd )
    values(9721,'rajamouli','prabhas,anushka,tamannah',234567,342567);

    insert into filmdetails  (id  ,
    director ,
    top_billed,
    budget_usd ,
    revenue_usd )
    values(9722,'rajamouli','prabhas,anushka',234567,342567);

UPDATE movie_SCHEMA.FILMDETAILS
SET
    director = 'Christopher Nolan', -- New value for director
    top_billed = 'Christian Bale, Heath Ledger, Aaron Eckhart', -- New value for top billed actors
    budget_usd = 185000000, -- New value for budget
    revenue_usd = 1005000000 -- New value for revenue
WHERE id = 9722; -- Replace '2' with the actual ID of the movie you want to update

----scd type 1

UPDATE movie_SCHEMA.FILMDETAILS
SET
    director = 'Udaya Bhavani', -- New value for director
    top_billed = 'Christian Bale, BTS', -- New value for top billed actors
    budget_usd = 1205000000, -- New value for budget
    revenue_usd = 2605000000 -- New value for revenue
WHERE id = 2;

UPDATE movie_SCHEMA.FILMDETAILS
SET
    director = 'Srivalli', -- New value for director
    top_billed = 'Christian Bale, BTS', -- New value for top billed actors
    budget_usd = 1205000000, -- New value for budget
    revenue_usd = 2605000000 -- New value for revenue
WHERE id = 3;

select * from MOVIE_SCHEMA.FILMDETAILS;

SELECT 
    id, 
    director, 
    top_billed, 
    budget_usd, 
    revenue_usd
FROM CLEANED_SCHEMA.FILM_DETAILS_CLEANED
WHERE id = 3;


select * from filmdetails;
select* from film_details_stream;

----- SCD type 1 end

select * from MOVIE_RECOMMENDATION.CLEANED_SCHEMA.FILM_DETAILS_CLEANED where id =9722;

insert into moreinfo(id ,
    runtime ,
    budget ,
    revenue ,
    film_id )
values(9719,3  , 22000000 , 33000000,9719); 

select * from moreinfo where id = 9719;
select * from CLEANED_SCHEMA.MOREINFO_CLEANED where id = 9719;

SELECT 
    id, 
    runtime, 
    budget, 
    revenue
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE id = 3;

SELECT 
    id, 
    MAX(budget) AS max_budget,
    MIN(budget) AS min_budget
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
GROUP BY id;




select * from moreinfo;
select * from moreinfo_cleaned;
select * from moreinfo_stream;

-- Resume the task to process updates
ALTER TASK TASK_CLEAN_MOREINFO RESUME;

-- Verify task is running
SHOW TASKS;

-- Check if the stream has captured changes
SELECT * 
FROM movie_SCHEMA.MOREINFO_STREAM;

ALTER TASK CLEAN_MOVIES_TASK RESUME;



--scd type 2
select * from moreinfo where id = 1;
select * from cleaned_schema.moreinfo_cleaned where id = 1;

UPDATE movie_SCHEMA.moreinfo
SET
    RUNTIME =3,
    BUDGET = 260000,
    REVENUE = 11000
WHERE id = 3;

--If a change is detected, update the existing record in MOREINFO_CLEANED to set Is_Current = 'N' and populate the End_Date.
UPDATE CLEANED_SCHEMA.MOREINFO_CLEANED
SET 
  EFFECTIVE_END_DATE = CURRENT_DATE,
    Is_Current = 'N'
WHERE 
    ID = 3;
    
--Insert the updated record into MOREINFO_CLEANED, with the new values and Start_Date set to the current date.
INSERT INTO CLEANED_SCHEMA.MOREINFO_CLEANED 
(ID, Runtime, Budget, REVENUE, Film_ID, Effective_Start_Date, Effective_End_Date, Is_Current)
SELECT 
    3, 
    3, 
    260000, 
    11000,
    3, 
    CURRENT_DATE, 
    NULL, 
    'Y';

  INSERT INTO CLEANED_SCHEMA.MOREINFO_CLEANED 
(ID, Runtime, Budget, REVENUE, Film_ID, Effective_Start_Date, Effective_End_Date, Is_Current)
SELECT 
    1, 
    1, 
    260000, 
    11000,
    3, 
    CURRENT_DATE, 
    NULL, 
    'Y';
    
--Retrieve Current Records Only
    SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE Is_Current = 'Y';


--Retrieve Historical Records for a Specific Movie
SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 3
ORDER BY EFFECTIVE_START_DATE;


--SCD TYPE 2 trial 2


select * from moreinfo where id = 2;
select * from cleaned_schema.moreinfo_cleaned where id = 2;


 UPDATE movie_SCHEMA.moreinfo
SET
    RUNTIME ='2',
    BUDGET =26666666,
    REVENUE = 2666666
WHERE id = 2;


SELECT 
    source.ID,
    REPLACE(REPLACE(source.Budget, '$', ''), ',', '')::NUMERIC AS Cleaned_Budget,
    REPLACE(REPLACE(target.Budget, '$', ''), ',', '')::NUMERIC AS Target_Cleaned_Budget
FROM movie_SCHEMA.MOREINFO AS source
LEFT JOIN CLEANED_SCHEMA.MOREINFO_CLEANED AS target
ON source.ID = target.ID
WHERE 
    target.Is_Current = 'Y'
    AND (
        REPLACE(REPLACE(source.Budget, '$', ''), ',', '')::NUMERIC <> REPLACE(REPLACE(target.Budget, '$', ''), ',', '')::NUMERIC
    );



    UPDATE CLEANED_SCHEMA.MOREINFO_CLEANED
SET 
    Effective_End_Date = CURRENT_DATE,
    Is_Current = 'N'
WHERE 
    ID = 2
    AND Is_Current = 'Y';


     INSERT INTO CLEANED_SCHEMA.MOREINFO_CLEANED 
(ID, Runtime, Budget,REVENUE, Film_ID, Effective_Start_Date, Effective_End_Date, Is_Current)
SELECT 
    2, 
 2,
 26666666, 
    2666666,
    2,
    CURRENT_DATE, 
    NULL, 
    'true';
    

    SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE Is_Current = 'y' and id = 2;


SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 2 and Is_Current = 'N'
ORDER BY Effective_Start_Date ;


SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 2 
ORDER BY Effective_Start_Date ;



    
   UPDATE movie_SCHEMA.moreinfo
SET
    RUNTIME ='7',
    BUDGET = 7111111,
    REVENUE = 71111111
WHERE id = 1;


UPDATE CLEANED_SCHEMA.MOREINFO_CLEANED
SET 
    Effective_End_Date = CURRENT_DATE,
    Is_Current = 'N'
WHERE 
    ID = 1
    AND Is_Current = 'true';

     INSERT INTO CLEANED_SCHEMA.MOREINFO_CLEANED 
(ID, Runtime, Budget,REVENUE, Film_ID, Effective_Start_Date, Effective_End_Date, Is_Current)
SELECT 
    1, 
 7,
    7111111, 
    71111111,
    1,
    CURRENT_DATE, 
    NULL, 
    'true';


    INSERT INTO CLEANED_SCHEMA.MOREINFO_CLEANED 
(ID, Runtime, Budget,REVENUE, Film_ID, Effective_Start_Date, Effective_End_Date, Is_Current)
SELECT 
    1, 
    1, 
    1111111, 
    11111111,
    1,
    CURRENT_DATE, 
    NULL, 
    'true';

   -- Retrieve Current Records Only
    SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE Is_Current = 'N' and id = 1;



--Retrieve Historical Records for a Specific Movie
SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 1 and IS_CURRENT = 'Y'
ORDER BY Effective_Start_Date;





SELECT 
    moreinfo.ID,
    moreinfo.Runtime,
    moreinfo.Budget,
    moreinfo_cleaned.Runtime AS Target_Runtime,
    moreinfo_cleaned.Budget AS Target_Budget
FROM MOVIE_SCHEMA.MOREINFO AS moreinfo
LEFT JOIN CLEANED_SCHEMA.MOREINFO_CLEANED AS moreinfo_cleaned
ON moreinfo.ID = moreinfo_cleaned.ID
WHERE 
    moreinfo_cleaned.Is_Current = 'true'
    AND (
        moreinfo.Runtime <> moreinfo_cleaned.Runtime
        OR moreinfo.Budget <> moreinfo_cleaned.Budget
    );


   SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 6
ORDER BY EFFECTIVE_START_DATE;


SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE  RUNTIME = 6;

select * from moreinfo_stream;

SHOW TASKS;

suspend task CLEANED_SCHEMA.CLEAN_MOVIES_TASK;



alter task CLEANED_SCHEMA.CLEAN_MOVIES_TASK suspend;

alter task CLEANED_SCHEMA.CLEAN_film_details_TASK suspend;

alter task CLEANED_SCHEMA.TASK_CLEAN_MOREINFO suspend;









UPDATE CLEANED_SCHEMA.MOREINFO_CLEANED
SET 
    Runtime = 150,  -- New runtime value
    Budget = 50000000  -- New budget value
WHERE 
    ID = 1  -- Replace with the ID of the row you want to update
    AND Is_Current = 'Y'; -- Ensure you only update the current record


SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE Is_Current = 'Y';

SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE Is_Current = 'N';
--ORDER BY EFFECTIVE_START_DATE DESC;


---If you want to see all versions (current and historical) of a specific record (identified by ID), you can run the following query:
SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 1  -- Replace with the specific ID you're interested in
ORDER BY EFFECTIVE_START_DATE DESC;


SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 3  -- Replace with the specific ID you're interested in
ORDER BY EFFECTIVE_START_DATE DESC;


SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 1 -- Replace with the specific ID you're interested in
  AND Is_Current = 'Y';
  
  SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 1 -- Replace with the specific ID you're interested in
  AND Is_Current = 'N'
ORDER BY EFFECTIVE_START_DATE DESC;




    SELECT 
    source.ID,
    REPLACE(REPLACE(source.Budget, '$', ''), ',', '')::NUMERIC AS Cleaned_Budget,
    REPLACE(REPLACE(target.Budget, '$', ''), ',', '')::NUMERIC AS Target_Cleaned_Budget
FROM movie_SCHEMA.MOREINFO AS source
LEFT JOIN CLEANED_SCHEMA.MOREINFO_CLEANED AS target
ON source.ID = target.ID
WHERE 
    target.Is_Current = 'Y'
    AND (
        REPLACE(REPLACE(source.Budget, '$', ''), ',', '')::NUMERIC <> REPLACE(REPLACE(target.Budget, '$', ''), ',', '')::NUMERIC
    );
    

    UPDATE CLEANED_SCHEMA.MOREINFO_CLEANED
SET Budget = REPLACE(REPLACE(Budget, '$', ''), ',', '');


INSERT INTO CLEANED_SCHEMA.MOREINFO_CLEANED (ID, Runtime, Budget, Film_ID, Is_Current)
SELECT
    ID,
    Runtime,
    REPLACE(REPLACE(Budget, '$', ''), ',', '') AS Cleaned_Budget,
    Film_ID,
    'Y' AS Is_Current
FROM movie_SCHEMA.MOREINFO;

ALTER TABLE CLEANED_SCHEMA.MOREINFO_CLEANED
MODIFY COLUMN Budget NUMERIC;
STREAM and TASK
-- Create a stream to track changes in the Movies table
CREATE OR REPLACE STREAM MOVIE_STREAM ON TABLE MOVIE_SCHEMA.MOVIES;


-- Create a stream to track changes in the filmdetails table
CREATE OR REPLACE STREAM FILM_DETAILS_STREAM ON TABLE MOVIE_SCHEMA.FILMDETAILS;

-- Create a stream to track changes in the MOREINFO table
CREATE OR REPLACE STREAM MOVIE_SCHEMA.MOREINFO_STREAM ON TABLE MOVIE_SCHEMA.MOREINFO;


-- create a task on movies_cleaned 
CREATE OR REPLACE TASK CLEANED_SCHEMA.CLEAN_MOVIES_TASK
WAREHOUSE = MOVIE_WH
SCHEDULE = '1 MINUTE'
AS
MERGE INTO CLEANED_SCHEMA.MOVIES_CLEANED AS target
USING (
    SELECT 
        id,
        title,
        genres,
        language,
        user_score,
        (runtime_hour * 60 + runtime_min) AS runtime_minutes,
        release_date,
        vote_count
    FROM movie_schema.MOVIE_STREAM
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
    title = source.title,
    genres = source.genres,
    runtime_minutes = source.runtime_minutes,
    vote_count = source.vote_count
WHEN NOT MATCHED THEN INSERT (
    id, title, genres, language, user_score, runtime_minutes, release_date, vote_count
) VALUES (
    source.id, source.title, source.genres, source.language, source.user_score, 
    source.runtime_minutes, source.release_date, source.vote_count
);




--scd type 1 on FILM_DETAILS_CLEANED

MERGE INTO CLEANED_SCHEMA.FILM_DETAILS_CLEANED AS target
USING (
    SELECT 
        id,
        director,
        top_billed,
        TRY_TO_NUMBER(REPLACE(budget_usd, '$', '')) AS budget_usd,
        TRY_TO_NUMBER(REPLACE(revenue_usd, '$', '')) AS revenue_usd
    FROM movie_recommendation.movie_schema.FILM_DETAILS_STREAM
) AS source
ON target.id = source.id
WHEN MATCHED THEN
    UPDATE SET 
        director = source.director,
        top_billed = source.top_billed,
        budget_usd = source.budget_usd,
        revenue_usd = source.revenue_usd
WHEN NOT MATCHED THEN
    INSERT (id, director, top_billed, budget_usd, revenue_usd)
    VALUES (source.id, source.director, source.top_billed, source.budget_usd, source.revenue_usd);



CREATE OR REPLACE TASK CLEANED_SCHEMA.CLEAN_FILM_DETAILS_TASK
WAREHOUSE = MOVIE_WH
SCHEDULE = '1 MINUTE'
AS
MERGE INTO CLEANED_SCHEMA.FILM_DETAILS_CLEANED AS target
USING (
    SELECT 
        id,
        director,
        top_billed,
        budget_usd,
        revenue_usd
    FROM movie_schema.FILM_DETAILS_STREAM
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET
    director = source.director,
    top_billed = source.top_billed,
    budget_usd = source.budget_usd,
    revenue_usd = source.revenue_usd
WHEN NOT MATCHED THEN INSERT (
    id, director, top_billed, budget_usd, revenue_usd
) VALUES (
    source.id, source.director, source.top_billed, source.budget_usd, source.revenue_usd
);



--task and SCD type2
-- Create a task to handle SCD Type 2 updates on MOREINFO_CLEANED
CREATE OR REPLACE TASK cleaned_schema.TASK_CLEAN_MOREINFO
WAREHOUSE = MOVIE_WH
SCHEDULE = '1 MINUTE'
AS
MERGE INTO CLEANED_SCHEMA.MOREINFO_CLEANED AS target
USING (
    SELECT 
        id,
        runtime,
        TRY_TO_NUMBER(REPLACE(budget, '$', '')) AS budget,
        TRY_TO_NUMBER(REPLACE(revenue, '$', '')) AS revenue,
        film_id
    FROM MOVIE_SCHEMA.MOREINFO_STREAM
) AS source
ON target.id = source.id AND target.is_current = TRUE
WHEN MATCHED AND (
    target.runtime != source.runtime OR
    target.budget != source.budget OR
    target.revenue != source.revenue OR
    target.film_id != source.film_id
) THEN
    -- Close the current record
    UPDATE SET 
        effective_end_date = CURRENT_TIMESTAMP,
        is_current = FALSE
WHEN NOT MATCHED THEN
    -- Insert new record with the updated information
    INSERT (
        id, runtime, budget, revenue, film_id, effective_start_date, effective_end_date, is_current
    ) VALUES (
        source.id, source.runtime, source.budget, source.revenue, source.film_id, 
        CURRENT_TIMESTAMP, NULL, TRUE
    );
    

    INSERT into  CLEANED_SCHEMA.MOREINFO_CLEANED  (
        id, runtime, budget, revenue, film_id, effective_start_date, effective_end_date, is_current
    ) VALUES (
        9720, 9720, 6789000,4567577, 1, 
        CURRENT_TIMESTAMP, NULL, TRUE
    );
    
    -- Query to view current records
 SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE Is_Current = 'y' and id = 2;


-- Query to view historical records
SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 2 and Is_Current = 'N'
ORDER BY Effective_Start_Date ;

---query both
SELECT *
FROM CLEANED_SCHEMA.MOREINFO_CLEANED
WHERE ID = 2 
ORDER BY Effective_Start_Date ;



ALTER TASK CLEAN_FILM_DETAILS_TASK RESUME;

ALTER TASK CLEAN_MOVIES_TASK RESUME;

ALTER TASK TASK_CLEAN_MOREINFO RESUME;
CLEANED
CREATE OR REPLACE TABLE CLEANED_SCHEMA.MOVIES_CLEANED AS 
SELECT 
    id,
    title,
    genres,
    language,
    user_score,
    (runtime_hour * 60 + runtime_min) AS runtime_minutes,
    release_date,
    vote_count
FROM MOVIE_SCHEMA.MOVIES;


select * from CLEANED_SCHEMA.MOVIES_CLEANED;
select * from movies;

SELECT *
FROM CLEANED_SCHEMA.MOVIES_CLEANED
ORDER BY EXTRACT(YEAR FROM release_date) DESC;

select * from movies_cleaned;


--table 2 
 CREATE OR REPLACE TABLE CLEANED_SCHEMA.FILM_DETAILS_CLEANED (
    id INT,
    director STRING,
    top_billed STRING,
    budget_usd NUMBER,
    revenue_usd NUMBER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO CLEANED_SCHEMA.FILM_DETAILS_CLEANED (id, director, top_billed, budget_usd, revenue_usd)
SELECT 
    id,
    director,
    top_billed,
    budget_usd,
    revenue_usd
FROM MOVIE_SCHEMA.FILMDETAILS;

SELECT 
    id, 
    director, 
    top_billed, 
    budget_usd, 
    revenue_usd
FROM CLEANED_SCHEMA.FILM_DETAILS_CLEANED
ORDER BY id;
 

SELECT * FROM CLEANED_SCHEMA.FILM_DETAILS_CLEANED;
SELECT * FROM FILMDETAILS;

SELECT * 
FROM CLEANED_SCHEMA.FILM_DETAILS_CLEANED
ORDER BY last_updated DESC;


----table 3

CREATE OR REPLACE TABLE CLEANED_SCHEMA.MOREINFO_CLEANED (
    id INT,
    runtime STRING,
    budget NUMBER,
    revenue NUMBER,
    film_id INT,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_current BOOLEAN
);


INSERT INTO CLEANED_SCHEMA.MOREINFO_CLEANED (
    id, runtime, budget, revenue, film_id, effective_start_date, effective_end_date, is_current
)
SELECT 
    id,
    runtime,
    TRY_TO_NUMBER(REPLACE(budget, '$', '')) AS budget,
    TRY_TO_NUMBER(REPLACE(revenue, '$', '')) AS revenue,
    film_id,
    CURRENT_TIMESTAMP AS effective_start_date,
    NULL AS effective_end_date,
    TRUE AS is_current
FROM MOVIE_SCHEMA.MOREINFO;

select * from CLEANED_SCHEMA.MOREINFO_CLEANED;
select * from MOVIE_SCHEMA.MOREINFO;

--table 4
create or replace TABLE MOVIE_RECOMMENDATION.CLEANED_SCHEMA.POSTERPATH_CLEANED (
	ID NUMBER(38,0) NOT NULL,
	POSTER_PATH VARCHAR(500),
	BACKDROP_PATH VARCHAR(500),
	primary key (ID)
);

insert into cleaned_schema.posterpath_cleaned(
id , poster_path , backdrop_path
)
select
    id ,
    poster_path , 
    backdrop_path
from movie_schema.posterpath;

select * from posterpath_cleaned;
RAW or MOVIE
CREATE OR REPLACE TABLE Movies (
    id INT PRIMARY KEY,
    title STRING,
    genres STRING,
    language STRING,
    user_score FLOAT,
    runtime_hour INT,
    runtime_min INT,
    release_date DATE,
    vote_count INT
);

CREATE OR REPLACE TABLE FilmDetails (
    id INT PRIMARY KEY,
    director STRING,
    top_billed STRING,
    budget_usd FLOAT,
    revenue_usd FLOAT
);

CREATE OR REPLACE TABLE MoreInfo (
    id INT PRIMARY KEY,
    runtime STRING,
    budget STRING,
    revenue STRING,
    film_id INT
);
CREATE OR REPLACE TABLE PosterPath (
    id INT PRIMARY KEY,
    poster_path STRING,
    backdrop_path STRING
);


---snowpipe for poster path
CREATE OR REPLACE PIPE poster_pipe
AUTO_INGEST = TRUE
AS
COPY INTO PosterPath
FROM @ext_movie_stage
FILE_FORMAT = CSV_FF;

ALTER PIPE poster_pipe REFRESH;
select * from PosterPath;



COPY INTO Movies
FROM @movie_stage/Movies.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

select * from movies;

COPY INTO FilmDetails
FROM @movie_stage/FilmDetails.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO MoreInfo
FROM @movie_stage/MoreInfo.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO PosterPath
FROM @movie_stage/PosterPath.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

SELECT * FROM Movies;
SELECT COUNT(*) FROM FilmDetails;
SELECT COUNT(*) FROM MoreInfo;
SELECT * FROM PosterPath;

truncate table MOVIES;

CREATE OR REPLACE PIPE movie_pipe
AUTO_INGEST = FALSE
AS
COPY INTO Movies
FROM @movie_stage/Movies.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


CREATE OR REPLACE PIPE MOVIE_PIPE
AUTO_INGEST = FALSE
AS
COPY INTO MOVIE_RECOMMENDATION.MOVIE_SCHEMA.MOVIES
FROM @MOVIE_STAGE
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
PATTERN = '.*Movies\.csv';


REMOVE @movie_stage/Movies.csv;
list @MOVIE_STAGE;

ALTER PIPE MOVIE_PIPE REFRESH;

SELECT * FROM MOVIE_RECOMMENDATION.MOVIE_SCHEMA.MOVIES;
INTERNAL STAGE
CREATE OR REPLACE STAGE movie_stage
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

EXTERNAL STAGE
CREATE STORAGE INTEGRATION movie_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::637423494475:role/myassignmentrole'
  STORAGE_AWS_EXTERNAL_ID = '123456'
  STORAGE_ALLOWED_LOCATIONS = ('s3://assign-kipi/PosterPath.csv');

  DESC INTEGRATION movie_integration;

  GRANT CREATE STAGE ON SCHEMA movie_SCHEMA TO ROLE MOVIE_ENGINEER ;

GRANT USAGE ON INTEGRATION movie_integration TO ROLE MOVIE_ENGINEER;
list @ext_movie_stage;

SHOW STORAGE INTEGRATIONS;
DROP STORAGE INTEGRATION movie_integration;

CREATE STORAGE INTEGRATION movie_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::637423494475:role/myassignmentrole'
STORAGE_ALLOWED_LOCATIONS = ('s3://assign-kipi/PosterPath.csv');

--ANALYSIS_USER
--VIEWS



CREATE OR REPLACE VIEW ANALYSIS_SCHEMA.MOVIE_POPULARITY_TRENDS AS
SELECT 
    GENRES,
    LANGUAGE,
    YEAR(TO_DATE(RELEASE_DATE)) AS RELEASE_YEAR, -- Removed the second argument
    AVG(USER_SCORE) AS AVG_SCORE,
    SUM(VOTE_COUNT) AS TOTAL_VOTES,
    COUNT(*) AS MOVIE_COUNT
FROM CLEANED_SCHEMA.MOVIES_CLEANED
GROUP BY GENRES, LANGUAGE, YEAR(TO_DATE(RELEASE_DATE))
ORDER BY RELEASE_YEAR, TOTAL_VOTES DESC;

select * from ANALYSIS_SCHEMA.MOVIE_POPULARITY_TRENDS;



--view 2

CREATE OR REPLACE VIEW ANALYSIS_SCHEMA.REVENUE_BUDGET_ANALYSIS AS
SELECT 
    F.DIRECTOR,
    F.TOP_BILLED,
    F.BUDGET_USD,
    F.REVENUE_USD,
    (F.REVENUE_USD - F.BUDGET_USD) AS PROFIT,
    CASE 
        WHEN F.REVENUE_USD > F.BUDGET_USD THEN 'PROFITABLE'
        ELSE 'NOT PROFITABLE'
    END AS PROFIT_STATUS
FROM CLEANED_SCHEMA.FILM_DETAILS_CLEANED F
WHERE F.BUDGET_USD IS NOT NULL AND F.REVENUE_USD IS NOT NULL
ORDER BY PROFIT DESC;

select * from ANALYSIS_SCHEMA.REVENUE_BUDGET_ANALYSIS;


--view 3




CREATE OR REPLACE VIEW ANALYSIS_SCHEMA.RUNTIME_RATINGS_ANALYSIS AS
SELECT 
    M.TITLE,
    CASE 
        WHEN MI.RUNTIME LIKE '%h%' AND MI.RUNTIME LIKE '%m%' THEN
            (CAST(SPLIT_PART(MI.RUNTIME, 'h', 1) AS INT) * 60 + 
             CAST(SPLIT_PART(SPLIT_PART(MI.RUNTIME, 'h ', 2), 'm', 1) AS INT))
        WHEN MI.RUNTIME LIKE '%h%' THEN
            CAST(SPLIT_PART(MI.RUNTIME, 'h', 1) AS INT) * 60
        WHEN MI.RUNTIME LIKE '%m%' THEN
            CAST(SPLIT_PART(MI.RUNTIME, 'm', 1) AS INT)
        ELSE NULL
    END AS TOTAL_RUNTIME_MIN,
    M.USER_SCORE,
    CASE 
        WHEN 
            CASE 
                WHEN MI.RUNTIME LIKE '%h%' AND MI.RUNTIME LIKE '%m%' THEN
                    (CAST(SPLIT_PART(MI.RUNTIME, 'h', 1) AS INT) * 60 + 
                     CAST(SPLIT_PART(SPLIT_PART(MI.RUNTIME, 'h ', 2), 'm', 1) AS INT))
                WHEN MI.RUNTIME LIKE '%h%' THEN
                    CAST(SPLIT_PART(MI.RUNTIME, 'h', 1) AS INT) * 60
                WHEN MI.RUNTIME LIKE '%m%' THEN
                    CAST(SPLIT_PART(MI.RUNTIME, 'm', 1) AS INT)
                ELSE NULL
            END < 90 THEN 'SHORT'
        WHEN 
            CASE 
                WHEN MI.RUNTIME LIKE '%h%' AND MI.RUNTIME LIKE '%m%' THEN
                    (CAST(SPLIT_PART(MI.RUNTIME, 'h', 1) AS INT) * 60 + 
                     CAST(SPLIT_PART(SPLIT_PART(MI.RUNTIME, 'h ', 2), 'm', 1) AS INT))
                WHEN MI.RUNTIME LIKE '%h%' THEN
                    CAST(SPLIT_PART(MI.RUNTIME, 'h', 1) AS INT) * 60
                WHEN MI.RUNTIME LIKE '%m%' THEN
                    CAST(SPLIT_PART(MI.RUNTIME, 'm', 1) AS INT)
                ELSE NULL
            END BETWEEN 90 AND 150 THEN 'MEDIUM'
        ELSE 'LONG'
    END AS RUNTIME_CATEGORY
FROM CLEANED_SCHEMA.MOVIES_CLEANED M
JOIN CLEANED_SCHEMA.MOREINFO_CLEANED MI ON M.ID = MI.ID
WHERE MI.RUNTIME IS NOT NULL
ORDER BY TOTAL_RUNTIME_MIN DESC;


SELECT 
    RUNTIME,
    CASE 
        WHEN RUNTIME LIKE '%h%' AND RUNTIME LIKE '%m%' THEN
            (CAST(SPLIT_PART(RUNTIME, 'h', 1) AS INT) * 60 + 
             CAST(SPLIT_PART(SPLIT_PART(RUNTIME, 'h ', 2), 'm', 1) AS INT))
        WHEN RUNTIME LIKE '%h%' THEN
            CAST(SPLIT_PART(RUNTIME, 'h', 1) AS INT) * 60
        WHEN RUNTIME LIKE '%m%' THEN
            CAST(SPLIT_PART(RUNTIME, 'm', 1) AS INT)
        ELSE NULL
    END AS TOTAL_RUNTIME_MIN
FROM CLEANED_SCHEMA.MOREINFO_CLEANED;

 

--view 4


CREATE OR REPLACE VIEW ANALYSIS_SCHEMA.ACTOR_DIRECTOR_CONTRIBUTIONS AS
WITH ACTOR_EXPANSION AS (
    SELECT 
        F.DIRECTOR,
        VALUE AS ACTOR,
        F.REVENUE_USD,
        F.BUDGET_USD
    FROM CLEANED_SCHEMA.FILM_DETAILS_CLEANED F,
    LATERAL FLATTEN(INPUT => SPLIT(F.TOP_BILLED, ',')) -- Splits the TOP_BILLED column into individual actors
)
SELECT 
    DIRECTOR,
    TRIM(ACTOR) AS ACTOR, -- Trims any leading/trailing spaces from actor names
    AVG(REVENUE_USD) AS AVG_REVENUE,
    AVG(BUDGET_USD) AS AVG_BUDGET,
    COUNT(*) AS MOVIE_COUNT
FROM ACTOR_EXPANSION
GROUP BY DIRECTOR, ACTOR
ORDER BY AVG_REVENUE DESC, MOVIE_COUNT DESC;


SELECT 
    F.DIRECTOR,
    VALUE AS ACTOR
FROM CLEANED_SCHEMA.FILM_DETAILS_CLEANED F,
LATERAL FLATTEN(INPUT => SPLIT(F.TOP_BILLED, ','));



---view 5

CREATE OR REPLACE VIEW ANALYSIS_SCHEMA.POSTER_BACKDROP_AVAILABILITY AS
SELECT 
    P.ID,
    CASE 
        WHEN P.POSTER_PATH = 'URL_NOT_AVAILABLE' THEN 'MISSING'
        ELSE 'AVAILABLE'
    END AS POSTER_STATUS,
    CASE 
        WHEN P.BACKDROP_PATH = 'URL_NOT_AVAILABLE' THEN 'MISSING'
        ELSE 'AVAILABLE'
    END AS BACKDROP_STATUS
FROM CLEANED_SCHEMA.POSTERPATH_CLEANED P; 


select * from  ANALYSIS_SCHEMA.POSTER_BACKDROP_AVAILABILITY;

--- secure view

CREATE OR REPLACE secure VIEW ANALYSIS_SCHEMA.MOVIE_POPULARITY_TRENDS1 AS
SELECT 
    GENRES,
    LANGUAGE,
    YEAR(TO_DATE(RELEASE_DATE)) AS RELEASE_YEAR, -- Removed the second argument
    AVG(USER_SCORE) AS AVG_SCORE,
    SUM(VOTE_COUNT) AS TOTAL_VOTES,
    COUNT(*) AS MOVIE_COUNT
FROM CLEANED_SCHEMA.MOVIES_CLEANED
GROUP BY GENRES, LANGUAGE, YEAR(TO_DATE(RELEASE_DATE))
ORDER BY RELEASE_YEAR, TOTAL_VOTES DESC;


----new view


    CREATE OR REPLACE VIEW VIEW_RELEASE_TITLE_DIRECTOR_REVENUE AS
SELECT 
    M.Release_Date,
    M.Title,
    F.Director,
    F.Revenue_USD AS Revenue
FROM CLEANED_SCHEMA.MOVIES_CLEANED M
LEFT JOIN CLEANED_SCHEMA.FILM_DETAILS_CLEANED F
    ON M.ID = F.ID;

    select * from VIEW_RELEASE_TITLE_DIRECTOR_REVENUE;


---- new view 2 
    CREATE OR REPLACE VIEW ANALYSIS_SCHEMA.VIEW_RELEASE_TITLE_DIRECTOR_REVENUE_BUDGET AS
SELECT 
    M.Release_Date,
    M.Title,
    F.Director,
    F.Revenue_USD AS Revenue,
    I.Runtime AS Detailed_Runtime,
FROM CLEANED_SCHEMA.MOVIES_CLEANED M
LEFT JOIN CLEANED_SCHEMA.FILM_DETAILS_CLEANED F
    ON M.ID = F.ID
LEFT JOIN CLEANED_SCHEMA.MOREINFO_CLEANED I
    ON M.ID = I.FILM_ID;

    select * from VIEW_RELEASE_TITLE_DIRECTOR_REVENUE_BUDGET;

--VIEWER_USER

SELECT RELEASE_YEAR, GENRES, AVG_SCORE 
FROM ANALYSIS_SCHEMA.MOVIE_POPULARITY_TRENDS 
WHERE GENRES = 'Action' 
ORDER BY RELEASE_YEAR;

SELECT RELEASE_YEAR, GENRES, AVG_SCORE 
FROM ANALYSIS_SCHEMA.MOVIE_POPULARITY_TRENDS 
WHERE GENRES = 'Drama' 
ORDER BY RELEASE_YEAR;

SELECT RELEASE_YEAR, GENRES, AVG_SCORE 
FROM ANALYSIS_SCHEMA.MOVIE_POPULARITY_TRENDS 
WHERE GENRES = 'Comedy' 
ORDER BY RELEASE_YEAR;

select * from analysis_schema.movie_popularity_trends;

--Most Profitable Directors
SELECT DIRECTOR, SUM(PROFIT) AS TOTAL_PROFIT 
FROM ANALYSIS_SCHEMA.REVENUE_BUDGET_ANALYSIS 
GROUP BY DIRECTOR 
ORDER BY TOTAL_PROFIT DESC;

--Compare Ratings by Runtime Category
SELECT RUNTIME_CATEGORY, AVG(USER_SCORE) AS AVG_SCORE
FROM ANALYSIS_SCHEMA.RUNTIME_RATINGS_ANALYSIS
GROUP BY RUNTIME_CATEGORY
ORDER BY AVG_SCORE DESC;



---new view

select * from ANALYSIS_SCHEMA.VIEW_RELEASE_TITLE_DIRECTOR_REVENUE;




    


    
