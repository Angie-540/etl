CREATE DEFINER=`fkamau`@`%` PROCEDURE `build_flat_hiv_summary_extended`(IN query_type varchar(50), IN queue_number int,
                                                                        IN queue_size int, IN cycle_size int,
                                                                        IN log tinyint(1))
BEGIN
                     SET @primary_table := "flat_hiv_summary_ext_v1";
                    set @query_type = query_type;
                    set @queue_table = "";
                    set @total_rows_written = 0;
                    
                    set @start = now();
                    set @table_version = "flat_hiv_summary_ext_v1.0";

                    set session sort_buffer_size=512000000           ;      
                    set @last_date_created = (select max(max_date_created) from etl.flat_obs);

                    
                    
CREATE TABLE IF NOT EXISTS etl.flat_hiv_summary_ext_v1 (
    person_id INT,
    encounter_id INT,
    location_id INT,
    encounter_datetime DATETIME,
    encounter_type INT,
    cm_test INT,
    cm_result INT,
    cm_result_date DATE,
    cm_treatment_start_date DATETIME DEFAULT NULL,
    on_cm_treatment INT(2) DEFAULT NULL,
    cm_treatment_end_date DATETIME DEFAULT NULL,
    cm_treatment_phase INT(5) DEFAULT NULL,
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    pcp_tx_stop_date DATETIME,
    pcp_tx_end_date DATETIME,
    
    ks_tx_start_date DATETIME,
    ks_tx_end_date DATETIME,
    
    toxo_tx_start_date DATETIME,
    toxo_tx_end_date DATETIME,

    cd4_justification INT,

    reason_for_arv_init_delay INT,

    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id , encounter_datetime),
    INDEX person_location (person_id , location_id),
    INDEX location_id (person_id , location_id),
    INDEX location_date (location_id , encounter_datetime),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
);
                    
                    
                   if(@query_type="build") then
                            select 'BUILDING..........................................';
                            set @write_table = concat("flat_hiv_summary_ext_temp_",queue_number);
                            set @queue_table = concat("flat_hiv_summary_ext_build_queue_",queue_number);  

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_hiv_summary_ext_build_queue limit ', queue_size, ');'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            
                            SET @dyn_sql=CONCAT('delete t1 from flat_hiv_summary_ext_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                    end if;
    
                                        if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = "flat_hiv_summary_ext";
                            set @queue_table = "flat_hiv_summary_ext_sync_queue";
							CREATE TABLE IF NOT EXISTS flat_hiv_summary_ext_sync_queue (
    person_id INT PRIMARY KEY
);
                            set @last_update = null;
							SELECT 
    MAX(date_updated)
INTO @last_update FROM
    etl.flat_log
WHERE
    table_name = @table_version;

                            replace into flat_hiv_summary_ext_sync_queue
                            (select distinct patient_id
                                from amrs.encounter
                                where date_changed > @last_update
                            );

                            replace into flat_hiv_summary_ext_sync_queue
                            (select distinct person_id
                                from etl.flat_obs
                                where max_date_created > @last_update
                            );

                            replace into flat_hiv_summary_ext_sync_queue
                            (select distinct person_id
                                from etl.flat_lab_obs
                                where max_date_created > @last_update
                            );

                            replace into flat_hiv_summary_ext_sync_queue
                            (select distinct person_id
                                from etl.flat_orders
                                where max_date_created > @last_update
                            );

                            replace into flat_hiv_summary_ext_sync_queue
                            (select person_id from
                                amrs.person
                                where date_voided > @last_update);


                            replace into flat_hiv_summary_ext_sync_queue
                            (select person_id from
                                amrs.person
                                where date_changed > @last_update);


                      end if;
                      

                    
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
                    
SELECT CONCAT('Done deleting from primary table');

                    set @total_time=0;
                    set @cycle_number = 0;
                    

                    while @person_ids_count > 0 do

                        set @loop_start_time = now();
                        
                        
                        drop temporary table if exists flat_hiv_summary_ext_build_queue__0;
                        

                        
                        SET @dyn_sql=CONCAT('create temporary table flat_hiv_summary_ext_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;

						SELECT CONCAT('Start flat_hiv_summary_ext_0a');
                        drop temporary table if exists flat_hiv_summary_ext_0a;
                        create temporary table flat_hiv_summary_ext_0a
                        (select
                            t1.person_id,
                            t1.encounter_id,
                            t1.encounter_datetime,
                            t1.encounter_type,
                            t1.location_id,
                            t1.obs,
                            t1.obs_datetimes
                            from etl.flat_obs t1
                                join flat_hiv_summary_ext_build_queue__0 t0 using (person_id)
							    where t1.encounter_type in (1,2,4,186)
                        );
                        
                        
SELECT CONCAT('Start flat_hiv_summary_ext_0');

                        drop temporary table if exists flat_hiv_summary_ext_0;
                        create temporary table flat_hiv_summary_ext_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
                        (select * from flat_hiv_summary_ext_0a
                        order by person_id, date(encounter_datetime)
                        );


                        set @prev_id = -1;
                        set @cur_id = -1;
                        set @cur_location = null;
					SET @sep := '##';
                       
                        set @cm_test = null;
                        set @cm_result = null;
                        set @cm_result_date = null;
                        set @on_cm_treatment = null;
						set @cm_treatment_phase = null;
                        set @cm_treatment_start_date= null;
                        set @cm_treatment_end_date = null;

                        set @ks_tx_start_date = null;
                        set @ks_tx_end_date = null;
                        set @toxoplasmosis_start_date = null;
                        set @toxoplasmosis_end_date = null;
                       
                        

                        
                        
SELECT CONCAT('Start flat_hiv_summary_ext_summary_1');

                        drop temporary table if exists flat_hiv_summary_ext_summary_1;
                        create temporary table flat_hiv_summary_ext_summary_1 (index encounter_id (encounter_id))
                        (select
                            obs,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,
                            t1.person_id,
                            t1.encounter_id,
                            t1.encounter_datetime,                            
                            t1.encounter_type,
                            case
                                when location_id then @cur_location := location_id
                                when @prev_id = @cur_id then @cur_location
                                else null
                            end as location_id,
                            case
                                when obs regexp "!!9812=" then @cm_test := 1 
                                else null
                            end as cm_test,
                            case
                                    when obs regexp "!!9812=664" then @cm_result := 1 -- negative
									when obs regexp "!!9812=703" then @cm_result := 2 -- positive
									when obs regexp "!!9812=1138" then @cm_result := 3 -- indeterminate
									when obs regexp "!!9812=1304" then @cm_result := 4 -- poor sample quality
                                   
                                when @prev_id != @cur_id then @cm_result := null
                                else @cm_result 
                            end as cm_result,
                            case
                                when obs regexp "!!9812=" AND obs_datetimes regexp "!!9812=" then @cm_result_date :=  replace(replace((substring_index(substring(obs_datetimes,locate("!!9812=",obs_datetimes)),@sep,1)),"!!9812=",""),"!!","")
                                when obs regexp "!!9812=" AND NOT obs_datetimes regexp "!!9812=" then @cm_result_date := encounter_datetime 
                                when @cur_id = @prev_id then @cm_result_date
                                else @cm_result_date := null
                            end as cm_result_date,
                            case
                                when obs regexp "!!1277=(1256|1257|1406|1259|12126)!!" then @on_cm_treatment := 1
								when obs regexp "!!1277=(1107|1260|1407)!!" then @on_cm_treatment := 0
								when @cur_id != @prev_id then @on_cm_treatment := 0
                                else @on_cm_treatment := 0
                            end as on_cm_treatment,
                            case
                                when obs regexp "!!12042=(8395)!!" then @cm_treatment_phase := 1 -- induction
								when obs regexp "!!12042=(12043)!!" then @cm_treatment_phase := 2 -- consolidation
								when obs regexp "!!12042=(12044)!!" then @cm_treatment_phase := 3 -- maintenance
								when @cur_id != @prev_id then @cm_treatment_phase := null
                                else @cm_treatment_phase:=0
                            end as cm_treatment_phase,

                            case
                              when obs regexp "!!12048=" then @cm_result_date :=  replace(replace((substring_index(substring(obs,locate("!!12048=",obs)),@sep,1)),"!!12048=",""),"!!","")
								when obs regexp "!!1277=1256!!" and @cm_treatment_start_date is null then @cm_treatment_start_date:= encounter_datetime
								when obs regexp "!!1277=(1107|1260)" then @cm_treatment_start_date:= null
                                when obs regexp "!!1277=1257!!" and @cm_treatment_start_date is null then @cm_treatment_start_date:= encounter_datetime
                                when @cur_id != @prev_id then @cm_treatment_start_date:= null
                                else @cm_treatment_start_date
                            end as cm_treatment_start_date,
                            
                            case
                                when obs regexp "!!1277=1260" then @cm_treatment_end_date :=  encounter_datetime
								-- when obs regexp "!!1277=1107" then @cm_treatment_end_date :=  encounter_datetime
                                when @cur_id = @prev_id then @cm_treatment_end_date
                                when @cur_id != @prev_id then @cm_treatment_end_date := null
                                else @cm_treatment_end_date
                            end as cm_treatment_end_date,
                            
                            case
								when obs regexp "!!12098=1260!!"  then @pcp_tx_stop_date:= encounter_datetime
								when obs regexp "!!1261=1260!!" then @pcp_tx_stop_date:= encounter_datetime
                                when obs regexp "!!12098=1260!!" then @pcp_tx_stop_date:= encounter_datetime
                                when @cur_id != @prev_id then @pcp_tx_stop_date:= null
                                else @pcp_tx_stop_date
                            end as pcp_tx_stop_date,

                            case
                                when (obs regexp "!!12100=1256!!") then @ks_tx_start_date :=  encounter_datetime
                                when @cur_id = @prev_id then @ks_tx_start_date
                                when @cur_id != @prev_id then @ks_tx_start_date := null
                                else @cm_treatment_end_date
                            end as ks_tx_start_date,

                            case
                                when (obs regexp "!!12100=1260!!") then @ks_tx_end_date :=  encounter_datetime
                                when @cur_id = @prev_id then @ks_tx_end_date
                                when @cur_id != @prev_id then @ks_tx_end_date := null
                                else @ks_tx_end_date
                            end as ks_tx_end_date,

                            case
                                when (obs regexp "!!12099=1256!!") then @toxoplasmosis_start_date :=  encounter_datetime
                                when @cur_id = @jprev_id then @toxoplasmosis_start_date
                                when @cur_id != @prev_id then @toxoplasmosis_start_date := null
                                else @toxoplasmosis_start_date
                            end as toxoplasmosis_start_date,

                            case
                                when (obs regexp "!!12100=1260!!") then @toxoplasmosis_end_date :=  encounter_datetime
                                when @cur_id = @prev_id then @toxoplasmosis_end_date
                                when @cur_id != @prev_id then @toxoplasmosis_end_date := null
                                else @toxoplasmosis_end_date
                            end as toxoplasmosis_end_date,

                            etl.GetValues(obs, 12100) as cd4_justification,
                            etl.GetValues(obs, 1505) as reason_for_arv_init_delay,
                            
                           
                           
                        from flat_hiv_summary_ext_0 t1
                        order by t1.person_id, t1.encounter_datetime asc
                        );
                        
                        drop temporary table if exists flat_hiv_summary_ext_summary_2;
                        create temporary table flat_hiv_summary_ext_summary_2 (index encounter_id (encounter_id))
                        (
                            select
                            t1.person_id,
                            t1.encounter_id,
                            t1.location_id,
                            t1.encounter_datetime,
                            t1.encounter_type,


                            t1.cm_test,
                            t1.cm_result,
                            t1.cm_result_date,
                            t1.cm_treatment_start_date,
                            t1.on_cm_treatment,
                            t1.cm_treatment_end_date,
                            t1.cm_treatment_phase ,
                            t1.date_created,

                            t1.ks_tx_start_date,
                            t1.ks_tx_end_date,

                            t1.toxoplasmosis_start_date,
                            t1.toxoplasmosis_end_date

                            from flat_hiv_summary_ext_summary_1 t1
                        );



SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    flat_hiv_summary_ext_summary_1;
                    
SELECT @new_encounter_rows;                    
                    set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;
    
                   -- SHOW COLUMNS FROM @write_table;
                    
                    SET @dyn_sql=CONCAT('replace into ',@write_table,                                              
                        '(select
                            t1.person_id,
                            t1.encounter_id,
                            t1.location_id,
                            t1.encounter_datetime,
                            t1.encounter_type,
                            
                            t1.cm_test,
                            t1.cm_result,
                            t1.cm_result_date,
                            t1.cm_treatment_start_date,
                            t1.on_cm_treatment,
                            t1.cm_treatment_end_date,
                            t1.cm_treatment_phase ,
                            t1.date_created,

                            t1.ks_tx_start_date,
                            t1.ks_tx_end_date,
                            t1.toxoplasmosis_start_date,
                            t1.toxoplasmosis_end_date
                        from flat_hiv_summary_ext_summary_2 t1
                        )');

                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    

                    

                    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_hiv_summary_ext_build_queue__0 t2 using (person_id);'); 

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
                 if (log) then
                 	insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
                    end if;
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

        END