CREATE DEFINER=`replication`@`%` PROCEDURE `generate_ahd_monthly_report_dataset`(IN query_type varchar(50),
                                                                                 IN queue_number int, IN queue_size int,
                                                                                 IN cycle_size int,
                                                                                 IN start_date varchar(50))
BEGIN

			set @start = now();
			set @table_version = "ahd_monthly_report_dataset_v1.0";
			set @last_date_created = (select max(date_created) from etl.flat_hiv_summary_ext_v1);

			set @sep = " ## ";
			set @lab_encounter_type = 99999;
			set @death_encounter_type = 31;
            

            #drop table if exists ahd_monthly_report_dataset;
           create table if not exists ahd_monthly_report_dataset (
				date_created timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
                elastic_id bigint,
				endDate date,
                encounter_id int,
				person_id int,
                person_uuid varchar(100),
				birthdate date,
				age double,
				gender varchar(1),
				encounter_date date,
				location_id int,

				last_cd4_count tinyint,

				cur_who_stage int,

				reason_for_arv_init_delay int,

				last_cd4_date datetime,
				last_cd4_percentage double,
				cd4_resulted double,
				cd4_resulted_date datetime,

				on_cm_tx tinyint,

				started_cm_tx_this_month tinyint,
				stopped_cm_tx_this_month tinyint,
				ended_cm_tx_this_month tinyint,

				started_tb_tx_this_month tinyint,
				stopped_tb_tx_this_month tinyint,
				ended_tb_tx_this_month tinyint,

				on_pcp_tx tinyint,
				started_pcp_tx_this_month tinyint,
				stopped_pcp_tx_this_month tinyint,
				ended_pcp_tx_this_month tinyint,
                
				on_toxo_tx tinyint,
				started_toxo_tx_this_month tinyint,
				stopped_toxo_tx_this_month tinyint,
				ended_toxo_tx_this_month tinyint,

				on_ks_tx tinyint,
				started_ks_tx_this_month tinyint,
				stopped_ks_tx_this_month tinyint,
				ended_ks_tx_this_month tinyint,
				
				reason_for_cd4_test tinyint,
				tb_screened tinyint,
				tb_status tinyint,
				is_art_init_delayed tinyint,

				crag_tested tinyint,
				cm_test_status tinyint,
				on_cm_tx_for_6mon_plus tinyint,
				cd4_test_done_atmon6 tinyint,
				on_cm_tx_for_12mon_plus tinyint,
				cd4_test_done_atmon12 tinyint,
				
				toxo_dx tinyint,
				toxo_tx tinyint,

				pcp_dx tinyint,
				pcp_tx tinyint,

				ks_dx tinyint,
				ks_tx tinyint,

                primary key elastic_id (elastic_id),
				index person_enc_date (person_id, encounter_date),
                index person_report_date (person_id, endDate),
                index endDate_location_id (endDate, location_id),
                index date_created (date_created),
                index status_change (location_id, endDate, status, prev_status)
                
            );
            
            #create table if not exists ahd_monthly_report_dataset_build_queue (person_id int primary key);
			#replace into ahd_monthly_report_dataset_build_queue
			#(select distinct person_id from flat_hiv_summary_v15);

			if (query_type = "build") then
					select "BUILDING.......................";
					set @queue_table = concat("ahd_monthly_report_dataset_build_queue_",queue_number);                    
#set @queue_table = concat("ahd_monthly_report_dataset_build_queue_1");                    				
					#create  table if not exists @queue_table (person_id int, primary key (person_id));
					#SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from ahd_monthly_report_dataset_build_queue limit 1000);'); 
					SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from ahd_monthly_report_dataset_build_queue limit ', queue_size, ');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
                    
                    			#delete t1 from ahd_monthly_report_dataset_build_queue t1 join @queue_table t2 using (person_id)
					SET @dyn_sql=CONCAT('delete t1 from ahd_monthly_report_dataset_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			end if;

			
            if (query_type = "sync") then
					set @queue_table = "ahd_monthly_report_dataset_sync_queue";                                
                    create table if not exists ahd_monthly_report_dataset_sync_queue (person_id int primary key);
                    
					select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);

					replace into ahd_monthly_report_dataset_sync_queue
                    (select distinct person_id from flat_hiv_summary_v15b where date_created >= @last_update);
            end if;
                        

			SET @num_ids := 0;
			SET @dyn_sql=CONCAT('select count(*) into @num_ids from ',@queue_table,';'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;          
            
            
            SET @person_ids_count = 0;
			SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
            
            
SET @dyn_sql=CONCAT('delete t1 from ahd_monthly_report_dataset_v1_2 t1 join ',@queue_table,' t2 using (person_id);'); 
#            SET @dyn_sql=CONCAT('delete t1 from ahd_monthly_report_dataset t1 join ',@queue_table,' t2 using (person_id);'); 
			PREPARE s1 from @dyn_sql; 
			EXECUTE s1; 
			DEALLOCATE PREPARE s1;  
            
            set @total_time=0;
			set @cycle_number = 0;
                    
			while @person_ids_count > 0 do
			
				set @loop_start_time = now();                        
				#create temporary table ahd_monthly_report_dataset_build_queue_0 (select * from ahd_monthly_report_dataset_build_queue_2 limit 5000); #TODO - change this when data_fetch_size changes

				drop temporary table if exists ahd_monthly_report_dataset_build_queue__0;
                create temporary table ahd_monthly_report_dataset_build_queue__0 (person_id int primary key);                

#SET @dyn_sql=CONCAT('insert into ahd_monthly_report_dataset_build_queue__0 (select * from ahd_monthly_report_dataset_build_queue_1 limit 100);'); 
                SET @dyn_sql=CONCAT('insert into ahd_monthly_report_dataset_build_queue__0 (select * from ',@queue_table,' limit ',cycle_size,');'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;
                
                
                set @age =null;
                set @status = null;
                

				--  1). Get the field wanted from the summaries
                drop temporary table if exists ahd_monthly_report_dataset_0;
				create temporary table ahd_monthly_report_dataset_0
				(
					select
						t1.encounter_id,
						t1.person_id,
						t1.person_uuid,
						t1.birthdate,
						t1.age,
						t1.gender,
						t1.encounter_date,
						t1.location_id,

						t2.cur_who_stage,

						t1.cm_test as crag_tested,
						t1.cm_result as cm_test_status,
						t1.cm_result_date,
						t1.cm_treatment_start_date,
						t1.on_cm_treatment,
						t1.cm_treatment_end_date,
						t1.cm_treatment_phase ,
						t1.date_created,

						t1.kaposis_sarcoma_start_date,
						t1.kaposis_sarcoma_end_date,
						t1.toxoplasmosis_start_date,
						t1.toxoplasmosis_end_date,

						t2.cd4_resulted ,
						t2.cd4_resulted_date,
						t2.cd4_1,
						t2.cd4_1_date,
						t2.cd4_2,
						t2.cd4_2_date,
						t2.cd4_percent_1,
						t2.cd4_percent_1_date,
						t2.cd4_percent_2,
						t2.cd4_percent_2_date,
						t1.cd4_justification,
						
						t2.tb_screen as tb_screened,
						t2.tb_screening_result as tb_status,
						t2.tb_screening_datetime,
						t2.on_ipt,
						t2.ipt_start_date,
						t2.ipt_stop_date,
						t2.ipt_completion_date,
						t2.on_tb_tx,
						t2.tb_tx_start_date,
						t2.tb_tx_end_date,
						t2.tb_tx_stop_date,
						t2.pcp_prophylaxis_start_date,
						t2.tb_tx_stop_reason
					from etl.flat_hiv_summary_ext_v1 t1
					join etl.flat_hiv_summary_v15b t2 using(person_id)
				);

				--  2). add report end dates
				drop temporary table if exists ahd_monthly_report_dataset_1;
				create temporary table ahd_monthly_report_dataset_1
				(
					select
						concat(date_format(d.endDate,"%Y%m"),t1.person_id) as elastic_id,
						*
					from etl.dates d
					join ahd_monthly_report_dataset_0 t1
					order by person_id, endDate
				);

				--  3). set up report logic
				set @prev_id = null;
				set @cur_id = null;
				set @cur_status = null;
				set @prev_status = null;
                set @prev_location_id = null;
                set @cur_location_id = null;

				drop temporary table if exists ahd_monthly_report_dataset_2;
				create temporary table ahd_monthly_report_dataset_2
				(select
					*,
					@prev_id := @cur_id as prev_id,
					@cur_id := person_id as cur_id,
					
					if(cm_treatment_start_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as started_cm_tx_this_month,
					if(cm_treatment_end_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as ended_cm_tx_this_month,
					null as stopped_cm_tx_this_month,

					if(tb_tx_start_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as started_tb_tx_this_month,
					if(tb_tx_end_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as ended_tb_tx_this_month,
                    if(tb_tx_stop_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as as stopped_tb_tx_this_month,
					
					if(pcp_prophylaxis_start_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as started_pcp_tx_this_month,
					if(pcp_tx_end_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as ended_pcp_tx_this_month,
                    if(pcp_tx_stop_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as as stopped_pcp_tx_this_month,
					
					if(ks_tx_start_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as started_ks_tx_this_month,
					if(ks_tx_end_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as ended_ks_tx_this_month,
                    null as stopped_ks_tx_this_month,


					if(toxo_tx_start_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as started_toxo_tx_this_month,
					if(toxo_tx_end_date between date_format(endDate,"%Y-%m-01")  and endDate,1,0) as ended_toxo_tx_this_month,
                    null as stopped_toxo_tx_this_month,
					
					if((cd4_resulted is not null), 1, 0) as has_cd4_result,

					case
						when ((on_cm_tx = 1) and (ABS(TIMESTAMPDIFF(MONTH, NOW(), cm_treatment_start_date))) = 6) then  1
						else 0
					end as on_cm_tx_for_6mon_plus,

					case
						when ((cd4_resulted is not null) and (ABS(TIMESTAMPDIFF(MONTH, NOW(), cm_treatment_start_date))) = 6) then  1
						else 0
					end as cd4_test_done_atmon6,

					case
						when ((on_cm_tx = 1) and (ABS(TIMESTAMPDIFF(MONTH, NOW(), cm_treatment_start_date))) =12) then  1
						else 0
					end as on_cm_tx_for_12mon_plus,

					case
						when ((cd4_resulted is not null) and (ABS(TIMESTAMPDIFF(MONTH, NOW(), cm_treatment_start_date))) = 12) then  1
						else 0
					end as cd4_test_done_atmon12,

					from ahd_monthly_report_dataset_1
					order by person_id, endDate desc
				);

				alter table ahd_monthly_report_dataset_2 drop column prev_id, cur_id;


				#delete from @queue_table where person_id in (select person_id from ahd_monthly_report_dataset_build_queue__0);
				SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join ahd_monthly_report_dataset_build_queue__0 t2 using (person_id);'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  
				
				#select @person_ids_count := (select count(*) from @queue_table);                        
				SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;  
                
                #select @person_ids_count as num_remaining;
                
				set @cycle_length = timestampdiff(second,@loop_start_time,now());
				#select concat('Cycle time: ',@cycle_length,' seconds');                    
				set @total_time = @total_time + @cycle_length;
				set @cycle_number = @cycle_number + 1;
				
				#select ceil(@person_ids_count / cycle_size) as remaining_cycles;
				set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
				#select concat("Estimated time remaining: ", @remaining_time,' minutes');
                
#                select count(*) into @num_in_hmrd from ahd_monthly_report_dataset_v1_2;
                
                select @num_in_hmrd as num_in_hmrd,
					@person_ids_count as num_remaining, 
					@cycle_length as 'Cycle time (s)', 
                    ceil(@person_ids_count / cycle_size) as remaining_cycles, 
                    @remaining_time as 'Est time remaining (min)';


			end while;

			if(query_type = "build") then
					SET @dyn_sql=CONCAT('drop table ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;  
			end if;            

			set @end = now();
            -- not sure why we need last date_created, I've replaced this with @start
  			insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
			select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");

        END