DELIMITER $$
CREATE PROCEDURE `generate_otz_summary_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
                    set @primary_table := "otz_summary";
                    set @total_rows_written = 0;
                    set @query_type = query_type;
                    
                    set @start = now();
                    set @table_version := "otz_summary_v1.0";

                    set session sort_buffer_size=512000000;

                    set @sep = " ## ";
                    set @lab_encounter_type := 99999;
                    set @death_encounter_type := 31;
                    set @last_date_created := (select max(max_date_created) from etl.flat_obs);
                    
SELECT 'Initializing variables successfull ...';


                    
                    
CREATE TABLE IF NOT EXISTS otz_summary (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    uuid VARCHAR(100),
    gender TEXT,
    birth_date DATE,
    death_date DATE,
    patient_care_status INT,
    rtc_date DATETIME,
    prev_rtc_date DATETIME,
    visit_id INT,
    visit_type INT,
    encounter_id INT,
    encounter_datetime DATETIME,
    encounter_type INT,
    date_enrolled DATE,
    age_at_enrollment INT, 
    art_start_date DATE,
    original_art_regimen VARCHAR,
    most_current_vl_results VARCHAR,
    date_vl_done DATE,
    most_current_art_regiment VARCHAR,
    date_started_current_art_regimen VARCHAR,
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
    vl_date_post_otz_enrollment DATE,
    ltfu VARCHAR,
    ltfu_date DATE,
    opt_out VARCHAR,
    opt_out_date DATE,
    transition_adult_care VARCHAR,
    transition_adult_care DATE,
    otz_modules_completion_tracker BOOLEAN,
    remarks VARCHAR,
    is_clinical_encounter INT,
    location_id INT,
    clinic VARCHAR(100),
    enrollment_location_id INT,
    transfer_in TINYINT,
    transfer_in_location_id INT,
    transfer_in_date DATETIME,
    transfer_out TINYINT,
    transfer_out_location_id INT,
    transfer_out_date DATETIME,
    vl_resulted INT,
    vl_resulted_date DATETIME,
    vl_1 INT,
    vl_1_date DATETIME,
    vl_2 INT,
    vl_2_date DATETIME,
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

                    drop temporary table if exists otz_patients;
					 create temporary table otz_patients (person_id int NOT NULL) 
                    (
                         select distinct patient_id from amrs.encounter e
							where e.encounter_type in (115)
                            -- change encounter type to that of otz
                    );
                    
                    
                                        
                    if(@query_type="build") then
                            select 'BUILDING..........................................';
                            set @write_table = concat("otz_summary_temp_",queue_number);
                            set @queue_table = concat("otz_summary_build_queue_",queue_number);                                                                    

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from otz_summary_build_queue limit ', queue_size, ');'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            
                            SET @dyn_sql=CONCAT('delete t1 from otz_summary_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            

                    end if;
                    
					if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = "otz_summary";
                            set @queue_table = "otz_summary_sync_queue";
                            CREATE TABLE IF NOT EXISTS otz_summary_sync_queue (
                                person_id INT PRIMARY KEY
                            );                            
                                                        

                            set @last_update := null;
                            
                            SELECT 
                                MAX(date_updated)
                            INTO @last_update FROM
                                etl.flat_log
                            WHERE
                                table_name = @table_version;

                            --   confirm if you need to change the last update date  
							#set @last_update = '2020-11-13 10:00:00';
                                
							SELECT CONCAT('Last Update ..', @last_update);

                            replace into otz_summary_sync_queue
                            (select distinct patient_id
                                from amrs.encounter e
                                join otz_patients o using (patient_id)
                                where e.date_changed > @last_update
                            );

                            replace into otz_summary_sync_queue
                            (select distinct ob.person_id
                                from etl.flat_obs ob
                                join otz_patients o on (o.patient_id = ob.person_id)
                                where ob.max_date_created > @last_update
                            );

                            replace into otz_summary_sync_queue
                            (select distinct l.person_id
                                from etl.flat_lab_obs l
                                join otz_patients o on (o.patient_id = l.person_id)
                                where l.max_date_created > @last_update
                            );

                            replace into otz_summary_sync_queue
                            (select distinct ord.person_id
                                from etl.flat_orders ord
								join otz_patients o on (o.patient_id = ord.person_id)
                                where ord.max_date_created > @last_update
                            );
                            
                            replace into otz_summary_sync_queue
                            (select p.person_id from 
                                amrs.person p
                                join otz_patients o on (o.patient_id = p.person_id)
                                where p.date_voided > @last_update);


                            replace into otz_summary_sync_queue
                            (select p2.person_id from 
                                amrs.person p2
                                join otz_patients o on (o.patient_id = p2.person_id)
                                where p2.date_changed > @last_update);
                                

                      end if;
    
					SELECT 'Removing test patients ...';
                    
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
                        
                        
                        drop temporary table if exists otz_summary_build_queue__0;
                        

                        
                        SET @dyn_sql=CONCAT('create temporary table otz_summary_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                        
						SELECT 'creating  otz_summary_0a from flat_obs...';
                        drop temporary table if exists otz_summary_0a;
                        create  temporary table otz_summary_0a
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
                                when t1.encounter_type in (3,4,114,115,154,158,186) then 1
                                else null
                            end as is_clinical_encounter,

                            case
                                when t1.encounter_type in (116) then 20
                                when t1.encounter_type in (3,4,9,114,115,154,158,186,214,220) then 10
                                when t1.encounter_type in (129) then 5 
                                else 1
                            end as encounter_type_sort_index,

                            t2.orders
                            from etl.flat_obs t1
                                join otz_summary_build_queue__0 t0 using (person_id)
                                join otz_patients o on (t1.person_id = o.patient_id)
                                join amrs.location l using (location_id)
                                left join etl.flat_orders t2 using(encounter_id)
                                left join amrs.visit v on (v.visit_id = t1.visit_id)
								where t1.encounter_type in (21,67,110,111,114,115,116,121,123,154,158,168,186,116,212,214,220)
								AND NOT obs regexp "!!5303=703!!"
                        );
                        
                        SELECT 'creating  otz_summary_0a from flat_lab_obs...';

                        insert into otz_summary_0a
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
                            null
                            from etl.flat_lab_obs t1
                                join otz_summary_build_queue__0 t0 using (person_id)
								join otz_patients o on (t1.person_id = o.patient_id)
                        );

                        drop temporary table if exists otz_summary_0;
                        create temporary table if not exists otz_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
                        (select * from otz_summary_0a
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
                        set @initial_hiv_dna_pcr_order_date = null;
                        set @hiv_dna_pcr_resulted := null;
                        set @initial_pcr_8wks := null;
                        set @initial_pcr_8wks_12months := null;
                        set @date_enrolled := null;
                        set @death_date:= null;
                        set @patient_care_status := null;
                        set @age_at_enrollment := null,
                        set @art_start_date := null,
                        set @original_art_regimen := null,
                        set @most_current_vl_results := null,
                        set @date_vl_done := null,
                        set @most_current_art_regiment := null,
                        set @date_started_current_art_regimen := null,
                        set @first_regimen_switch := null,
                        set @first_regimen_switch_date := null,
                        set @first_regimen_switch_reason := null,
                        set @second_regimen_switch := null,
                        set @second_regimen_switch_date := null,
                        set @second_regimen_switch_reason := null,
                        set @third_regimen_switch := null,
                        set @third_regimen_switch_date := null,
                        set @third_regimen_switch_reason := null,
                        set @fourth_regimen_switch := null,
                        set @fourth_regimen_switch_date := null,
                        set @fourth_regimen_switch_reason := null,
                        set @vl_result_post_otz_enrollment := null,
                        set @vl_date_post_otz_enrollment := null,
                        set @ltfu := null,
                        set @ltfu_date := null,
                        set @opt_out := null,
                        set @opt_out_date := null,
                        set @transition_adult_care := null,
                        set @transition_adult_care := null,
                        set @otz_modules_completion_tracker := null,
                        set @remarks := null,
						set @vl_1=null;
                        set @vl_2=null;
                        set @vl_1_date=null;
                        set @vl_2_date=null;
                        set @vl_resulted=null;
                        set @vl_resulted_date=null;
                        set @prev_arv_meds = null;
                        set @cur_arv_meds = null;
                        
                        drop temporary table if exists otz_summary_1;
                        create temporary table otz_summary_1 (index encounter_id (encounter_id))
                        (select
                            obs,
                            encounter_type_sort_index,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,
                            t1.person_id,
                            t1.visit_type,
                            p.uuid,
                            p.gender,
							case
                                when p.dead or p.death_date then @death_date := p.death_date
                                when obs regexp "!!1570=" then @death_date := replace(replace((substring_index(substring(obs,locate("!!1570=",obs)),@sep,1)),"!!1570=",""),"!!","")
                                when @prev_id != @cur_id or @death_date is null then
                                    case
                                        when obs regexp "!!(1734|1573)=" then @death_date := encounter_datetime
                                        when obs regexp "!!(1733|9082|6206)=159!!" or t1.encounter_type=31 then @death_date := encounter_datetime
                                        else @death_date := null
                                    end
                                else @death_date
                            end as death_date,
                            p.birthDate as  birth_date,
                            case
                                when @death_date <= encounter_datetime then @patient_care_status := 159
                                when obs regexp "!!1946=1065!!" then @patient_care_status := 9036
                                when obs regexp "!!1285=" then @patient_care_status := replace(replace((substring_index(substring(obs,locate("!!1285=",obs)),@sep,1)),"!!1285=",""),"!!","")
                                when obs regexp "!!1596=" then @patient_care_status := replace(replace((substring_index(substring(obs,locate("!!1596=",obs)),@sep,1)),"!!1596=",""),"!!","")
                                when obs regexp "!!9082=" then @patient_care_status := replace(replace((substring_index(substring(obs,locate("!!9082=",obs)),@sep,1)),"!!9082=",""),"!!","")
                                
                                when t1.encounter_type = @lab_encounter_type and @cur_id != @prev_id then @patient_care_status := null
                                when t1.encounter_type = @lab_encounter_type and @cur_id = @prev_id then @patient_care_status
                                else @patient_care_status := 6101
                            end as patient_care_status,
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
                            case
                               when obs regexp "!!1839=" AND obs regexp "!!1839=7850!!" then @date_enrolled:= GetValues(obs,'7013')
                               when obs regexp "!!1839=" AND NOT obs regexp "!!1839=7850!!" AND @date_enrolled is NULL then @date_enrolled := date(encounter_datetime)
                               when NOT obs regexp "!!1839=" AND @date_enrolled IS NULL then @date_enrolled := date(encounter_datetime)
                               else @date_enrolled
                            end as date_enrolled,
                            t1.is_clinical_encounter,                                                    
                            case
                                when location_id then @cur_location := location_id
                                when @prev_id = @cur_id then @cur_location
                                else null
                            end as location_id,
                            t1.clinic,
                            -- age at enrollment
                            -- art start date
                            case
                            -- art regimen
                            when obs regexp "10804" then @original_art_regimen := GetValues(obs, )

                            --
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

                            case
                                    when obs regexp "!!856=[0-9]" and t1.encounter_type = @lab_encounter_type then @vl_1:=cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
                                    when obs regexp "!!856=[0-9]"
                                            and (@vl_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""),@vl_1_date)) > 30)
                                            and (@vl_1_date is null or (replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")) > @vl_1_date)
                                        then @vl_1 := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
                                    when @prev_id=@cur_id then @vl_1
                                    else @vl_1:=null
                            end as vl_1,

                            case
                                when obs regexp "!!856=[0-9]" and t1.encounter_type = @lab_encounter_type then @vl_1_date:= encounter_datetime
                                when obs regexp "!!856=[0-9]"
                                        and (@vl_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""),@vl_1_date)) > 30)
                                        and (@vl_1_date is null or (replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")) > @vl_1_date)
                                    then @vl_1_date := replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")
                                when @prev_id=@cur_id then @vl_1_date
                                else @vl_1_date:=null
                            end as vl_1_date,



                            
                            
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
                            case
                                when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_meds := null
                                when obs regexp "!!1250=" then @cur_arv_meds := normalize_arvs(obs,'1250')
                                    
                                    
                                when obs regexp "!!1088=" then @cur_arv_meds := normalize_arvs(obs,'1088')
                                                                        
                                when obs regexp "!!2154=" then @cur_arv_meds := normalize_arvs(obs,'2154')
								
                                when obs regexp "!!2157=" and not obs regexp "!!2157=1066" then @cur_arv_meds := normalize_arvs(obs,'2157')
                                    
                                when @prev_id = @cur_id then @cur_arv_meds
                                else @cur_arv_meds:= null
                            end as cur_arv_meds,
                   

                        from flat_hei_summary_0 t1
                            join amrs.person p using (person_id)
                            LEFT JOIN amrs.relationship `r` on (r.person_b = p.person_id AND r.relationship = 2)
                            LEFT JOIN amrs.person `mother` on (mother.person_id = r.person_a AND mother.gender = 'F')
                        );
                        
                

                        set @prev_id = null;
                        set @cur_id = null;
                        set @prev_encounter_datetime = null;
                        set @cur_encounter_datetime = null;

                        set @prev_clinical_location_id = null;
                        set @cur_clinical_location_id = null;


                        alter table flat_hei_summary_1 drop prev_id, drop cur_id, drop cur_clinical_datetime, drop cur_clinic_rtc_date;

                        drop temporary table if exists flat_hei_summary_2;
                        create temporary table flat_hei_summary_2
                        (select *,
                                   @prev_id := @cur_id as prev_id,
                            @cur_id := person_id as cur_id,

                            case
                                when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                                else @prev_encounter_datetime := null
                            end as next_encounter_datetime_hiv,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

                            case
                                when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
                                else @next_encounter_type := null
                            end as next_encounter_type_hiv,

                            @cur_encounter_type := encounter_type as cur_encounter_type,

                            case
                                when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
                                else @prev_clinical_datetime := null
                            end as next_clinical_datetime_hiv,

                            case
                                when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                                else @prev_clinical_location_id := null
                            end as next_clinical_location_id,

                            case
                                when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
                                when @prev_id = @cur_id then @cur_clinical_datetime
                                else @cur_clinical_datetime := null
                            end as cur_clinic_datetime,

                            case
                                when is_clinical_encounter then @cur_clinical_location_id := location_id
                                when @prev_id = @cur_id then @cur_clinical_location_id
                                else @cur_clinical_location_id := null
                            end as cur_clinic_location_id,

                            case
                                when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
                                else @prev_clinical_rtc_date := null
                            end as next_clinical_rtc_date_hiv,

                            case
                                when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
                                when @prev_id = @cur_id then @cur_clinical_rtc_date
                                else @cur_clinical_rtc_date:= null
                            end as cur_clinical_rtc_date

                            from flat_hei_summary_1
                            order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
                        );

                        alter table flat_hei_summary_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


                        set @prev_id = null;
                        set @cur_id = null;
                        set @prev_encounter_type = null;
                        set @cur_encounter_type = null;
                        set @next_encounter_type = null;
                        set @prev_clinical_location_id = null;
                        set @cur_clinical_location_id = null;

                        drop  temporary table if exists flat_hei_summary_3;
                        create  temporary table flat_hei_summary_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
                        (select
                            *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,
                            
                             case
                                when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                                else @prev_encounter_datetime := null
                            end as prev_encounter_datetime_hiv,
                            
                            case
                                when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                                else @prev_clinical_location_id := null
                            end as prev_clinical_location_id,


                            case
                                when is_clinical_encounter then @cur_clinical_location_id := location_id
                                when @prev_id = @cur_id then @cur_clinical_location_id
                                else @cur_clinical_location_id := null
                            end as cur_clinical_location_id

                            from flat_hei_summary_2 t1
                            order by person_id, date(encounter_datetime), encounter_type_sort_index
                        );
                                        
                        alter table flat_hei_summary_3 drop prev_id, drop cur_id;

                        set @prev_id = null;
                        set @cur_id = null;
                        set @transfer_in = null;
                        set @transfer_in_date = null;
                        set @transfer_in_location_id = null;
                        set @transfer_out = null;
                        set @transfer_out_date = null;
                        set @transfer_out_location_id = null;
                        
                        #Handle transfers
                
                        drop temporary table if exists flat_hei_summary_4;

                        create temporary table flat_hei_summary_4 ( index person_enc (person_id, encounter_datetime))
                        (select
                            *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,
                            case
                                when obs regexp "!!7015=" then @transfer_in := 1
                                when prev_clinical_location_id != location_id then @transfer_in := 1
                                else @transfer_in := null
                            end as transfer_in,

                            case 
                                when obs regexp "!!7015=" then @transfer_in_date := date(encounter_datetime)
                                when prev_clinical_location_id != location_id then @transfer_in_date := date(encounter_datetime)
                                when @cur_id = @prev_id then @transfer_in_date
                                else @transfer_in_date := null
                            end transfer_in_date,
                            
                            case 
                                when obs regexp "!!7015=1287" then @transfer_in_location_id := 9999
                                when prev_clinical_location_id != location_id then @transfer_in_location_id := prev_clinical_location_id
                                when @cur_id = @prev_id then @transfer_in_location_id
                                else @transfer_in_location_id := null
                            end transfer_in_location_id,
                            
							case
                                    when obs regexp "!!1285=" then @transfer_out := 1
                                    when obs regexp "!!1596=1594!!" then @transfer_out := 1
                                    when obs regexp "!!9082=(1287|1594|9068|9504|1285)!!" then @transfer_out := 1
                                    when obs regexp "!!10000=" then @transfer_out := 1
                                    when next_clinical_location_id != location_id then @transfer_out := 1
                                    else @transfer_out := null
                            end as transfer_out,

                            case 
                                when obs regexp "!!1285=(1287|9068|2050)!!" and next_clinical_datetime_hiv is null then @transfer_out_location_id := 9999
                                when obs regexp "!!1285=1286!!" and next_clinical_datetime_hiv is null then @transfer_out_location_id := 9998
                                when obs regexp "!!10000=" then @transfer_out_location_id := 9999
                                when next_clinical_location_id != location_id then @transfer_out_location_id := next_clinical_location_id
                                else @transfer_out_location_id := null
                            end transfer_out_location_id,

                            
                            case 
                                when @transfer_out and next_clinical_datetime_hiv is null then @transfer_out_date := date(IF(cur_rtc_date IS NOT NULL,cur_rtc_date,encounter_datetime))
                                when next_clinical_location_id != location_id then @transfer_out_date := date(next_clinical_datetime_hiv)
                                else @transfer_out_date := null
                            end transfer_out_date
                                                    
                            
                          
                            from flat_hei_summary_3 t1
                            order by person_id, date(encounter_datetime), encounter_type_sort_index
                        );


SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    flat_hei_summary_4;
                    
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
                        is_clinical_encounter,
                        location_id,
                        clinic,
                        enrollment_location_id,
                        transfer_in,
                        transfer_in_location_id,
                        transfer_in_date,
                        transfer_out,
                        transfer_out_location_id,
                        transfer_out_date,
                        person_bringing_patient,
                        hei_outcome,
						vl_resulted,
                        vl_resulted_date,
                        vl_1,
                        vl_1_date,
                        vl_2,
                        vl_2_date,
                        vl_order_date,
                        prev_arv_meds,
                        cur_arv_meds,
                        newborn_arv_meds,
                        prev_clinical_location_id,
						next_clinical_location_id,
                        prev_encounter_datetime_hiv,
                        next_encounter_datetime_hiv,
                        prev_clinical_datetime_hiv,
	                    next_clinical_datetime_hiv,
                        from flat_hei_summary_4 t1)');

                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    

                    

                    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_hei_summary_build_queue__0 t2 using (person_id);'); 

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