DELIMITER $$
CREATE PROCEDURE `generate_flat_otz_summary`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
                    set @primary_table := "flat_otz_summary";
                    set @total_rows_written = 0;
                    set @query_type = query_type;
                    
                    set @start = now();
                    set @table_version := "flat_otz_summary_v1.0";

                    set session sort_buffer_size=512000000;

                    set @last_date_created := (select max(max_date_created) from etl.flat_obs);
                    
SELECT 'Initializing variables successfull ...';


                    
                    
CREATE TABLE IF NOT EXISTS flat_otz_summary (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    uuid VARCHAR(100),
    gender TEXT,
    birth_date DATE,
    visit_id INT,
    visit_type INT,
    encounter_id INT,
    encounter_datetime DATETIME,
    encounter_type INT,
    date_enrolled_to_otz DATE,
    age_at_otz_enrollment INT, 
    previously_enrolled_to_otz VARCHAR,
    original_art_start_date DATE,
    original_art_regimen VARCHAR,
    vl_result_at_otz_enrollment VARCHAR,
    vl_result_date_at_otz_enrollment DATE,
    art_regimen_at_otz_enrollment VARCHAR,
    art_regimen_line_at_otz_enrollment VARCHAR,
    first_regimen_switch VARCHAR,
    first_regimen_switch_date DATE,
    first_regimen_switch_reason VARCHAR,
    second_regimen_switch VARCHAR,
    second_regimen_switch_date DATE,
    second_regimen_switch_reason VARCHAR
    third_regimen_switch VARCHAR,
    third_regimen_switch_date DATE,
    third_regimen_switch_reason VARCHAR
    fourth_regimen_switch VARCHAR,
    fourth_regimen_switch_date DATE,
    fourth_regimen_switch_reason VARCHAR,
    vl_result_post_otz_enrollment VARCHAR,
    vl__result_date_post_otz_enrollment DATE,
    otz_orientation tinyint,
    otz_treatment_literacy tinyint,
    otz_participation tinyint,
    otz_peer_mentorship tinyint,
    otz_leadership tinyint,
    otz_positive_health_dignity_prevention tinyint,
    otz_future_decison_making tinyint,
    otz_transition_adult_care tinyint,
    discontinue_otz_reason VARCHAR,
    discontinue_otz_date DATETIME,
    clinical_remarks VARCHAR,
    is_clinical_encounter INT,
    location_id INT,
    enrollment_location_id INT,
    vl_resulted INT,
    vl_resulted_date DATETIME,
    vl_1 INT,
    vl_1_date DATETIME,
    vl_2 INT,
    vl_2_date DATETIME,
    vl_1_date_within_6months INT,
    vl_order_date DATETIME,
    prev_arv_meds VARCHAR(500),
    cur_arv_meds VARCHAR(500),
    prev_clinical_location_id MEDIUMINT,
    next_clinical_location_id MEDIUMINT,
    prev_encounter_datetime_hiv DATETIME,
    next_encounter_datetime_hiv DATETIME,
    prev_clinical_datetime_hiv DATETIME,
    next_clinical_datetime_hiv DATETIME,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id , encounter_datetime),
    INDEX location_id_rtc_date (location_id),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
);

SELECT 'created table successfully ...';

                    -- drop temporary table if exists otz_patients;
					--  create temporary table otz_patients (person_id int NOT NULL) 
                    -- (
                    --      select distinct patient_id from amrs.encounter e
					-- 		where e.encounter_type in (284,288,285,283)
                    --         -- change encounter type to that of otz
                    -- );
                    
                    
                                        
                    if(@query_type="build") then
                            select 'BUILDING..........................................';
                            set @write_table = concat("flat_otz_summary_temp_",queue_number);
                            set @queue_table = concat("flat_otz_summary_build_queue_",queue_number);                                                                    

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_otz_summary_build_queue limit ', queue_size, ');'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            
                            SET @dyn_sql=CONCAT('delete t1 from flat_otz_summary_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            

                    end if;
                    
					if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = "flat_otz_summary";
                            set @queue_table = "flat_otz_summary_sync_queue";
                            CREATE TABLE IF NOT EXISTS flat_otz_summary_sync_queue (
                                person_id INT PRIMARY KEY
                            );                            
                                                        

                            set @last_update := null;
                            
                            SELECT 
                                MAX(date_updated)
                            INTO @last_update FROM
                                etl.flat_log
                            WHERE
                                table_name = @table_version;
 
							#set @last_update = '2022-12-12 10:00:00';
                                
							SELECT CONCAT('Last Update ..', @last_update);

                            replace into flat_otz_summary_sync_queue
                            (select distinct patient_id
                                from amrs.encounter e
                                join otz_patients o using (patient_id)
                                where e.date_changed > @last_update
                            );
                            -- adding flat_hiv_summary
                            -- replace into flat_otz_summary_sync_queue
                            -- (
                            --     select distinct person_id
                            --     from etl.flat_hiv_summary_v15b hv
                            --     join otz_patients o on (o.patient_id = hv.person_id)
                            --     where hv.encounter_datetime > @last_update
                            -- )

                            replace into flat_otz_summary_sync_queue
                            (select distinct ob.person_id
                                from etl.flat_obs ob
                                join otz_patients o on (o.patient_id = ob.person_id)
                                where ob.max_date_created > @last_update
                            );

                            replace into flat_otz_summary_sync_queue
                            (select distinct l.person_id
                                from etl.flat_lab_obs l
                                join otz_patients o on (o.patient_id = l.person_id)
                                where l.max_date_created > @last_update
                            );

                            replace into flat_otz_summary_sync_queue
                            (select distinct ord.person_id
                                from etl.flat_orders ord
								join otz_patients o on (o.patient_id = ord.person_id)
                                where ord.max_date_created > @last_update
                            );
                            
                            replace into flat_otz_summary_sync_queue
                            (select p.person_id from 
                                amrs.person p
                                join otz_patients o on (o.patient_id = p.person_id)
                                where p.date_voided > @last_update);


                            replace into flat_otz_summary_sync_queue
                            (select p2.person_id from 
                                amrs.person p2
                                join otz_patients o on (o.patient_id = p2.person_id)
                                where p2.date_changed > @last_update);
                                

                      end if;
    
					SELECT 'Removing test patients ...';
                    -- //confirm attribute type
                    SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1
                            join amrs.person_attribute t2 using (person_id)
                            where t2.person_attribute_type_id=28 and value="true" and voided=0');
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  

                    SET @person_ids_count = 0;
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;

                    SELECT @person_ids_count AS 'num patients to sync';

                    SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  

                    set @total_time=0;
                    set @cycle_number = 0;
                    

                    while @person_ids_count > 0 do

                        set @loop_start_time = now();
                        
                        drop temporary table if exists flat_otz_summary_build_queue__0;
                        
                        SET @dyn_sql=CONCAT('create temporary table flat_otz_summary_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                        
						SELECT 'creating  otz_summary_0a from flat_obs...';
                        drop temporary table if exists flat_otz_summary_0a;
                        create  temporary table flat_otz_summary_0a
                        (select
                            t1.person_id,
                            t1.visit_id,
                            v.visit_type_id as visit_type,
                            t1.encounter_id,
                            t1.encounter_datetime,
                            t1.encounter_type,
                            t1.location_id,
                            l.name as `clinic`,
                            t1.obs,
                            t1.obs_datetimes,
                            case
                                when t1.encounter_type in (1,2,105,106,137,186) then 1
                                else null
                            end as is_clinical_encounter,

                            case
                                when t1.encounter_type in (116) then 20
                                when t1.encounter_type in (9,114,284,288,285,283,154,158,186) then 10 
                                else 1
                            end as encounter_type_sort_index,

                            t2.orders
                            from etl.flat_obs t1
                                join otz_summary_build_queue__0 t0 using (person_id)
                                join otz_patients o on (t1.person_id = o.patient_id)
                                join amrs.location l using (location_id)
                                left join etl.flat_orders t2 using(encounter_id)
                                left join amrs.visit v on (v.visit_id = t1.visit_id)
								where t1.encounter_type in (21,67,110,111,114,116,121,123,154,158,168,186,116,284,288,285,283)
								
                        );
                        
                        SELECT 'creating  otz_summary_0a from flat_lab_obs...';

                        insert into flat_otz_summary_0a
                        (select
                            t1.person_id,
                            null,
                            null,
                            t1.encounter_id,
                            t1.test_datetime,
                            t1.encounter_type,
                            null,
                            null,
                            t1.obs,
                            null, 
                            
                            0 as is_clinical_encounter,
                            1 as encounter_type_sort_index,
                            null,
                            from etl.flat_lab_obs t1
                                join flat_otz_summary_build_queue__0 t0 using (person_id)
								join otz_patients o on (t1.person_id = o.patient_id)
                        );

                        drop temporary table if exists flat_otz_summary_0;
                        create temporary table if not exists flat_otz_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
                        (select * from flat_otz_summary_0a
                        order by person_id, date(encounter_datetime), encounter_type_sort_index
                        );


                        set @prev_id = -1;
                        set @cur_id = -1;
						set @prev_encounter_date = null;
                        set @cur_encounter_date = null;
						set @cur_rtc_date = null;
                        set @prev_rtc_date = null;
                        set @cur_location = null;
                        set @cur_clinic = null;
                        set @enrollment_location_id = null;

                        set @date_enrolled_to_otz = null;
                        set @previously_enrolled_to_otz = null;
                        set @age_at_otz_enrollment = null,
                        set @original_art_start_date = null,
                        set @original_art_regimen  = null,
                        set @vl_result_at_otz_enrollment = null,
                        set @vl_result_date_at_otz_enrollment  = null,
                        set @art_regimen_at_otz_enrollment = null,
                        set @art_regimen_line_at_otz_enrollment = null,

                        set @first_regimen_switch = null,
                        set @first_regimen_switch_date = null,
                        set @first_regimen_switch_reason = null,
                        set @second_regimen_switch = null,
                        set @second_regimen_switch_date = null,
                        set @second_regimen_switch_reason = null,
                        set @third_regimen_switch = null,
                        set @third_regimen_switch_date = null,
                        set @third_regimen_switch_reason = null,
                        set @fourth_regimen_switch = null,
                        set @fourth_regimen_switch_date = null,
                        set @fourth_regimen_switch_reason = null,

                        set @vl_result_post_otz_enrollment = null,
                        set @vl_date_post_otz_enrollment = null,

                        set @otz_orientation = null,
                        set @otz_treatment_literacy := null,
                        set @otz_participation = null,
                        set @otz_peer_mentorship = null,
                        set @otz_leadership = null,
                        set @otz_positive_health_dignity_prevention = null ,
                        set @otz_future_decison_making = null,
                        set @otz_transition_adult_care = null,

                        set @discontinue_otz_reason = null,
                        set @discontinue_otz_date = null,
                        set @clinical_remarks = null,
						set @vl_1=null;
                        set @vl_2=null;
                        set @vl_1_date=null;
                        set @vl_1_date_within_6months = null;
                        set @vl_2_date=null;
                        set @vl_resulted=null;
                        set @vl_resulted_date=null;
                        set @prev_arv_meds = null;
                        set @cur_arv_meds = null;
                        
                        drop temporary table if exists flat_otz_summary_1;
                        create temporary table flat_otz_summary_1 (index encounter_id (encounter_id))
                        (select
                            obs,
                            encounter_type_sort_index,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,
                            t1.person_id,
                            t1.visit_type,
                            p.uuid,
                            p.gender,
                            p.birth_date as birth_date
                            p.ccc_number
                            --added D.O.B and ccc number
                            -- added more indicators
                            
                             case
                                when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
                                else @prev_rtc_date := null
                            end as prev_rtc_date,
                            case
                                when obs regexp "!!5096=" then @cur_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
                                when @prev_id = @cur_id then if(@cur_rtc_date > encounter_datetime,@cur_rtc_date,null)
                                else @cur_rtc_date := null
                            end as cur_rtc_date,
                            t1.visit_id,
                            t1.encounter_id,
                            @prev_encounter_date := date(@cur_encounter_date) as prev_encounter_date,
                            @cur_encounter_date := date(encounter_datetime) as cur_encounter_date,
                            t1.encounter_datetime,                            
                            t1.encounter_type,
                            --date enrolled into otz** OTZ ENROLLMENT
                            case
                            -- 1, date 
                               when obs regexp "!!10793=1066!!" then @date_enrolled_to_otz:= date(encounter_datetime)
                               when obs regexp "!!10793=" AND obs regexp "!!10793=1065!!" then @date_enrolled_to_otz := GetValues(obs,'10747')
                               when NOT obs regexp "!!10793=" AND @date_enrolled_to_otz IS NULL then @date_enrolled_to_otz := date(encounter_datetime)
                               else @date_enrolled_to_otz
                            end as date_enrolled_to_otz,
                            
                            t1.is_clinical_encounter,                                                    
                            case
                                when location_id then @cur_location := location_id
                                when @prev_id = @cur_id then @cur_location
                                else null
                            end as location_id,
                            t1.clinic,



                            -- remove this logic below
                            --previously enrolled to otz and hence modules previosuly covered
                            case 
                            when obs regexp "10793=1065" and date_enrolled_to_otz is not null then 
                            when obs regexp "10793=1066" and date_enrolled_to_otz is null then 0
                            else previously_enrolled_to_otz
                            end as previously_enrolled_to_otz,




                            
                            -- calculate age at enrollment to otz
                            case
                            when date_enrolled_to_otz > birth_date then TIMESTAMPdIFF(YEAR, birth_date, date_enrolled_to_otz)
                            else NULL
                            end as age_at_otz_enrollment,

                            -- prev arv meds

                            case
						        when @prev_id=@cur_id then @prev_arv_meds := @cur_arv_meds
						        else @prev_arv_meds := null
							end as prev_arv_meds,

                            -- current arv meds
	
							case
								when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_meds := null
								when obs regexp "!!1250=" then @cur_arv_meds :=
									replace(replace((substring_index(substring(obs,locate("!!1250=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1250=", "") ) ) / LENGTH("!!1250=") ))),"!!1250=",""),"!!","")
								when obs regexp "!!1088=" then @cur_arv_meds :=
									replace(replace((substring_index(substring(obs,locate("!!1088=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!1088=", "") ) ) / LENGTH("!!1088=") ))),"!!1088=",""),"!!","")
								when obs regexp "!!2154=" then @cur_arv_meds :=
									replace(replace((substring_index(substring(obs,locate("!!2154=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!2154=", "") ) ) / LENGTH("!!2154=") ))),"!!2154=",""),"!!","")
								when @prev_id = @cur_id then @cur_arv_meds
								else @cur_arv_meds:= null
							end as cur_arv_meds,

                            -- original art start regimen, confirm if column is in flat hiv summary then replace as 
                            -- hv.arv_first_regimen
                            case
								when @original_art_regimen is null and obs regexp "!!2157=" then
										@original_art_regimen := replace(replace((substring_index(substring(obs,locate("!!2157=",obs)),@sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, "!!2157=", "") ) ) / LENGTH("!!2157=") ))),"!!2157=",""),"!!","")
								when obs regexp "!!7015=" and @original_art_regimen is null then @original_art_regimen := "unknown"
								when @original_art_regimen is null and @cur_arv_meds is not null then @original_art_regimen := @cur_arv_meds
								when @prev_id = @cur_id then @original_art_regimen
								when @prev_id != @cur_id then @original_art_regimen := null
								else "-1"
							end as original_art_regimen,

                            --original art regimen start date

                            	case
								when @original_art_regimen_start_date is null and obs regexp "!!1499=" then @original_art_regimen_start_date := replace(replace((substring_index(substring(obs,locate("!!1499=",obs)),@sep,1)),"!!1499=",""),"!!","")
								when obs regexp "!!7015=" and @original_art_regimen_start_date is null then @original_art_regimen_start_date := date(t1.encounter_datetime)
								when @original_art_regimen_start_date is null and @cur_arv_meds is not null then @original_art_regimen_start_date := date(t1.encounter_datetime)
								when @prev_id = @cur_id then @original_art_regimen_start_date
								when @prev_id != @cur_id then @original_art_regimen_start_date := null
								else @original_art_regimen_start_date
							end as original_art_regimen_start_date,
                                
                            -- current art regimen when getting enrolled to otz
                            when obs regexp "!!1255=(1107|1260)!!" then @original_art_regimen := null;
                            when obs regexp "1250=" then @original_art_regimen := normalize_arvs(obs, '1250')
                            when obs regexp "1088=" then @original_art_regimen := normalize_arvs(obs, '1088')
                            when obs regexp "2154=" then @original_art_regimen := normalize_arvs(obs, '2154')
                            when obs regexp "2157="  and not obs regexp "!!2157=1066" then @original_art_regimen := normalize_arvs(obs, '2157')
                            when @prev_id = @cur_id then @original_art_regimen
                            else @original_art_regimen := null
                            end as @original_art_regimen,

                            case 
                            -- most current art regimen
                            when obs regexp "!!2154=" and not obs regexp "!!2154=1066" then @most_current_art_regiment := normalize_arvs(obs, '2154')
                            when obs regexp "!!2157=" and @original_art_regimen is null then @most_current_art_regiment := normalize_arvs(obs, '2154')
                            else @most_current_art_regiment
                            end as @most_current_art_regiment

                            --date started current art regimen
                            case
                            when 


                            --regimen switches
                            case 
                            when prev_id:=cur_id and date_enrolled_to_otz is not null and prev_arv_meds != cur_arv_meds then first_regimen_switch := 1;


                            --otz modules completion tracker
                            -- otz orientation
                            case 
                            when obs regexp "!!11032=" and obs regexp "!!11032=1065" then @otz_orientation := 1
                            when obs regexp "!!11032=" and obs regexp "11032=1066" then @otz_orientation := 0
                            else @otz_orientation := null
                            end as otz_orientation,

                            --otz treatment literacy
                            case 
                            when obs regexp "!!11037=" and obs regexp "11037=1065" then @otz_treatment_literacy := 1
                             when obs regexp "!!11037=" and obs regexp "11037=1066" then @otz_treatment_literacy := 0
                             else @otz_treatment_literacy := null
                             end as otz_treatment_literacy,

                            --otz participation
                            case 
                            when obs regexp "!!11033=" and obs regexp "11033=1065" then @otz_participation := 1
                            when obs regexp "!!11033=" and obs regexp "11033=1066" then @otz_participation := 0
                            else @otz_participation := null
                            end as otz_participation,

                            -- otz peer mentorship
                             case 
                            when obs regexp "!!12300=" and obs regexp "12300=1065" then @otz_peer_mentorship := 1
                            when obs regexp "!!12300=" and obs regexp "12300=1066" then @otz_peer_mentorship := 0
                            else @otz_peer_mentorship := null
                            end as otz_peer_mentorship,

                            --otz leadership
                            case 
                            when obs regexp "!!11034=" and obs regexp "11034=1065" then @otz_leadership := 1
                            when obs regexp "!!11034=" and obs regexp "11034=1066" then @otz_leadership := 0
                            else @otz_leadership := null
                            end as otz_leadership,

                            --otz positive dignity prevention
                            case 
                            when obs regexp "!!12272=" and obs regexp "12272=1065" then @otz_positive_health_dignity_prevention := 1
                            when obs regexp "!!12272=" and obs regexp "12272=1066" then @otz_positive_health_dignity_prevention := 0
                            else @otz_positive_health_dignity_prevention := null 
                            end as otz_positive_health_dignity_prevention,

                            --otz future decision making
                            case 
                            when obs regexp "!!11035=" and obs regexp "11035=1065" then @otz_future_decison_making := 1
                            when obs regexp "!!11035=" and obs regexp "11035=1066" then @otz_future_decison_making := 0
                            else @otz_future_decison_making := null 
                            end as otz_future_decison_making,

                            --otz otz_transition_adult_care
                            case 
                            when obs regexp "!!9302=" and obs regexp "9302=1065" then @otz_transition_adult_care := 1
                            when obs regexp "!!9302=" and obs regexp "9302=1066" then @otz_transition_adult_care := 0
                            else @otz_transition_adult_care := null
                            end as otz_transition_adult_care,

                            --clinical remarks 9467
                            case when obs 


                            --discontinue otz(transition/attrition)
                            case 
                            when obs regexp "!!1596="  then @discontinue_otz := GetValues(obs, '1596')
                            else @discontinue_otz
                            end as discontinue_otz

                            
                            CASE
                             WHEN
                                 (@enrollment_location_id IS NULL
                                     || (@enrollment_location_id IS NOT NULL
                                     AND @prev_id != @cur_id))
                                     AND obs REGEXP '!!7030=5622'
                             THEN
                                 @enrollment_location_id:=9999
                             WHEN
                                 obs REGEXP '!!7015='
                                     AND (@enrollment_location_id IS NULL
                                     || (@enrollment_location_id IS NOT NULL
                                     AND @prev_id != @cur_id))
                             THEN
                                 @enrollment_location_id:=9999
                             WHEN
                                 encounter_type NOT IN (21 , @lab_encounter_type)
                                     AND (@enrollment_location_id IS NULL
                                     || (@enrollment_location_id IS NOT NULL
                                     AND @prev_id != @cur_id))
                             THEN
                                 @enrollment_location_id:= location_id
                             WHEN @prev_id = @cur_id THEN @enrollment_location_id
                             ELSE @enrollment_location_id:=NULL
                         END AS enrollment_location_id,
                             case
                                when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
                                else @prev_clinical_datetime := null
                            end as prev_clinical_datetime_hiv,
                            
                            case
                                when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
                                when @prev_id = @cur_id then @cur_clinical_datetime
                                else @cur_clinical_datetime := null
                            end as cur_clinical_datetime,
                            case
                                when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
                                else @prev_clinical_rtc_date := null
                            end as prev_clinical_rtc_date_hiv,

                            case
                                when is_clinical_encounter then @cur_clinical_rtc_date := @cur_rtc_date
                                when @prev_id = @cur_id then @cur_clinical_rtc_date
                                else @cur_clinical_rtc_date:= null
                            end as cur_clinic_rtc_date,

						         case
                                    when @prev_id=@cur_id then
                                        case
                                            when obs regexp "!!856=[0-9]" and @vl_1 >= 0
                                                and 
                                                    if(obs_datetimes is null,encounter_datetime, 
                                                        date(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""))) <> date(@vl_1_date) then @vl_2:= @vl_1
                                            else @vl_2
                                        end
                                    else @vl_2:=null
                            end as vl_2,

                            case
                                    when @prev_id=@cur_id then
                                        case
                                            when obs regexp "!!856=[0-9]" and @vl_1 >= 0
                                                and 
                                                    if(obs_datetimes is null,encounter_datetime,
                                                        date(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""))) <>date(@vl_1_date) then @vl_2_date:= @vl_1_date
                                            else @vl_2_date
                                        end
                                    else @vl_2_date:=null
                            end as vl_2_date,

                            case
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" then @vl_date_resulted := date(encounter_datetime)
                                when @prev_id = @cur_id and date(encounter_datetime) = @vl_date_resulted then @vl_date_resulted
                            end as vl_resulted_date,

                            case
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" then @vl_resulted := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
                                when @prev_id = @cur_id and date(encounter_datetime) = @vl_date_resulted then @vl_resulted
                            end as vl_resulted,
                            --most current vl
                            case
                                    when obs regexp "!!856=[0-9]" and t1.encounter_type = @lab_encounter_type then @vl_1:=cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
                                    when obs regexp "!!856=[0-9]"
                                            and (@vl_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""),@vl_1_date)) > 30)
                                            and (@vl_1_date is null or (replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")) > @vl_1_date)
                                        then @vl_1 := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
                                    when @prev_id=@cur_id then @vl_1
                                    else @vl_1:=null
                            end as vl_1,
                            -- date most current vl done
                            case
                                when obs regexp "!!856=[0-9]" and t1.encounter_type = @lab_encounter_type then @vl_1_date:= encounter_datetime
                                when obs regexp "!!856=[0-9]"
                                        and (@vl_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""),@vl_1_date)) > 30)
                                        and (@vl_1_date is null or (replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")) > @vl_1_date)
                                    then @vl_1_date := replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")
                                when @prev_id=@cur_id then @vl_1_date
                                else @vl_1_date:=null
                            end as vl_1_date,
                            -- vl done within 6 months?
                            case when DATEDIFF(@current_date, @vl_1_date) <= 6 then 1
                                else 0
                            end as vl_1_date_within_6months,

                            case
                                when obs regexp "!!1271=856!!" then @vl_order_date := date(encounter_datetime)
                                when orders regexp "856" then @vl_order_date := date(encounter_datetime)
                                when @prev_id=@cur_id and (@vl_1_date is null or @vl_1_date < @vl_order_date) then @vl_order_date
                                else @vl_order_date := null
                            end as vl_order_date,
                            
                             case
                                when @prev_id=@cur_id and @cur_arv_meds is not null then @prev_arv_meds := @cur_arv_meds
                                when @prev_id=@cur_id then @prev_arv_meds
                                else @prev_arv_meds := null
                            end as prev_arv_meds,

                            
                            
                            
                            #2154 : PATIENT REPORTED CURRENT ANTIRETROVIRAL TREATMENT
                            #2157 : PATIENT REPORTED PAST ANTIRETROVIRAL TREATMENT
                            --most current art regimen
                            case
                                when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_meds := null
                                when obs regexp "!!1250=" then @cur_arv_meds := normalize_arvs(obs,'1250')
                                    
                                    
                                when obs regexp "!!1088=" then @cur_arv_meds := normalize_arvs(obs,'1088')
                                                                        
                                when obs regexp "!!2154=" then @cur_arv_meds := normalize_arvs(obs,'2154')
								
                                when obs regexp "!!2157=" and not obs regexp "!!2157=1066" then @cur_arv_meds := normalize_arvs(obs,'2157')
                                    
                                when @prev_id = @cur_id then @cur_arv_meds
                                else @cur_arv_meds:= null
                            end as cur_arv_meds,
                   

                        from flat_otz_summary_0 t1
                            join amrs.person p using (person_id)
                            join etl.flat_hiv_summary_v15b hv where  t1.person_id = hv.person_id
                        );
                                     


                    SELECT 
                        COUNT(*)
                    INTO @new_encounter_rows FROM
                        flat_otz_summary_4;
                                        
                    SELECT @new_encounter_rows;                    
                                        set @total_rows_written = @total_rows_written + @new_encounter_rows;
                    SELECT @total_rows_written;
    
                    
                    
                    SET @dyn_sql=CONCAT('replace into ',@write_table,                                              
                        '(select
                        null,
                        person_id,
                        t1.uuid,
                        gender,
                        death_date,
                        patient_care_status,
						cur_rtc_date as rtc_date,
                        prev_rtc_date,
                        visit_id,
                        visit_type,
                        encounter_id,
                        encounter_datetime,
                        encounter_type,
                        date_enrolled,
                        age_at_enrollment,
                        date_started_current_art_regimen,
                        first_regimen_switch,
                        first_regimen_switch_date,
                        first_regimen_switch_reason,
                        second_regimen_switch,
                        second_regimen_switch_date,
                        second_regimen_switch_reason
                        third_regimen_switch,
                        third_regimen_switch_date,
                        third_regimen_switch_reason
                        fourth_regimen_switch,
                        fourth_regimen_switch_date,
                        fourth_regimen_switch_reason,
                        vl_result_post_otz_enrollment,
                        vl_date_post_otz_enrollment,
                        otz_orientation,
                        otz_treatment_literacy,
                        otz_participation,
                        otz_peer_mentorship,
                        otz_leadership,
                        otz_positive_health_dignity_prevention,
                        otz_future_decison_making 
                        otz_transition_adult_care,
                        discontinue_otz,
                        clinical_remarks ,
                        is_clinical_encounte,
                        location_id,
                        clinic,
                        enrollment_location_id,
						vl_resulted,
                        vl_resulted_date,
                        vl_1,
                        vl_1_date,
                        vl_2,
                        vl_2_date,
                        vl_order_date,
                        prev_arv_meds,
                        cur_arv_meds,
                        prev_clinical_location_id,
						next_clinical_location_id,
                        prev_encounter_datetime_hiv,
                        next_encounter_datetime_hiv,
                        prev_clinical_datetime_hiv,
	                    next_clinical_datetime_hiv,
                        from flat_otz_summary_4 t1)');

                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    

                    

                    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join otz_summary_build_queue__0; t2 using (person_id);'); 

                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    
                    
                    
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                                       
                    set @cycle_length = timestampdiff(second,@loop_start_time,now());
                    
                    set @total_time = @total_time + @cycle_length;
                    set @cycle_number = @cycle_number + 1;
                    
                    
                    set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
                    

            SELECT 
                @person_ids_count AS 'persons remaining',
                @cycle_length AS 'Cycle time (s)',
                CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
                @remaining_time AS 'Est time remaining (min)';

                 end while;
                 
                if(@query_type="build") then
                        SET @dyn_sql=CONCAT('drop table ',@queue_table,';'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;  
                        
                        SET @total_rows_to_write=0;
                        SET @dyn_sql=CONCAT("Select count(*) into @total_rows_to_write from ",@write_table);
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                                                
                        set @start_write = now();
                        
						SET @dyn_sql=CONCAT('describe ',@write_table,';');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
            SELECT 
                CONCAT(@start_write,
                        ' : Writing ',
                        @total_rows_to_write,
                        ' to ',
                        @primary_table);

                        SET @dyn_sql=CONCAT('replace into ', @primary_table,
                            '(select * from ',@write_table,');');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                        set @finish_write = now();
                        set @time_to_write = timestampdiff(second,@start_write,@finish_write);
        SELECT 
            CONCAT(@finish_write,
                    ' : Completed writing rows. Time to write to primary table: ',
                    @time_to_write,
                    ' seconds ');                        
                        
                        SET @dyn_sql=CONCAT('drop table ',@write_table,';'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;  
                        
                        
                end if;
                
                                    
                set @ave_cycle_length = ceil(@total_time/@cycle_number);
SELECT 
    CONCAT('Average Cycle Length: ',
            @ave_cycle_length,
            ' second(s)');
                
                 set @end = now();
                 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');



END$$
DELIMITER ;