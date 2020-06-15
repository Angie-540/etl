DELIMITER $$
CREATE  PROCEDURE `generate_flat_orders_v_1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
                    set @primary_table := "flat_orders";
                    set @query_type = query_type;
                    set @queue_table = "";
                    set @total_rows_written = 0;
                    
                    set @start = now();
                    set @table_version = "flat_orders_v1.8";

                    set session sort_buffer_size=512000000;


                    
                    
					CREATE TABLE IF NOT EXISTS flat_orders (
    person_id INT,
    encounter_id INT,
    order_id INT,
    encounter_datetime DATETIME,
    encounter_type INT,
    date_activated DATETIME,
    orders TEXT,
    order_datetimes TEXT,
    max_date_created DATETIME,
    INDEX encounter_id (encounter_id),
    INDEX person_enc_id (person_id , encounter_id),
    INDEX date_created (max_date_created),
    PRIMARY KEY (encounter_id)
);

					SELECT CONCAT('Created flat_orders_table');

                    if(@query_type="build") then
                            select 'BUILDING..........................................';
                            set @write_table = concat("flat_orders_test_temp_",queue_number);
                            set @queue_table = concat("flat_orders_test_build_queue_",queue_number);                                                                    

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select person_id from flat_orders_test_build_queue limit ', queue_size, ');'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            SET @dyn_sql=CONCAT('delete t1 from flat_orders_test_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                    end if;


                    SET @person_ids_count = 0;
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;

SELECT @person_ids_count AS 'num patients to sync/build';
                    
                    SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  


                    set @total_time=0;
                    set @cycle_number = 0;
                    

                    while @person_ids_count > 0 do

                        set @loop_start_time = now();
                        
						drop temporary table if exists flat_orders_test_build_queue__0;
                        

                        SET @dyn_sql=CONCAT('create temporary table IF NOT EXISTS flat_orders_test_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                        

						CREATE temporary  TABLE IF NOT EXISTS flat_person_encounters__0 (
							person_id INT,
							visit_id INT,
							encounter_id INT,
							encounter_datetime DATETIME,
							encounter_type INT,
							location_id INT
						);
                        
SELECT CONCAT('Created flat person encounters table');
                       
                       
					SELECT CONCAT('replace into flat_person_encounters__0');

                        replace into flat_person_encounters__0(
                        SELECT 
                            p.person_id,
                            e.visit_id,
                            e.encounter_id,
                            e.encounter_datetime,
                            e.encounter_type,
                            e.location_id
                        FROM
                            etl.flat_orders_test_build_queue__0 `p`
                        INNER JOIN amrs.encounter `e` on (e.patient_id = p.person_id)  
                        );
                        
                        
						drop temporary table if exists flat_orders_test__0;

                        create temporary table flat_orders_test__0
                            (select
                                o.patient_id,
                                o.encounter_id,
                                o.order_id,
                                e.encounter_datetime,
                                e.encounter_type,
                                o.date_activated,
                                e.location_id,
                                group_concat(concept_id order by o.concept_id separator ' ## ') as orders,
                                group_concat(concat(@boundary,o.concept_id,'=',date(o.date_activated),@boundary) order by o.concept_id separator ' ## ') as order_datetimes,
                                max(o.date_created) as max_date_created

                                from amrs.orders o
                                    join flat_person_encounters__0 `e` using (encounter_id)
                                where o.encounter_id > 0
                                    and o.voided=0
                                group by o.encounter_id
                            );

                   
                    drop temporary table if exists flat_person_encounters__0;


                    SET @dyn_sql=CONCAT('replace into ',@write_table,                                              
                        '(select 
			             patient_id,
	                     encounter_id,
                         order_id,
	                     encounter_datetime,
	                     encounter_type,
                         date_activated,
	                     orders,
	                     order_datetimes,
	                     max_date_created
                        from flat_orders_test__0 t1)');
                        
					PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    
                    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_orders_test_build_queue__0 t2 using (person_id);'); 
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
                      
                        SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_orders_test_build_queue__0 t2 using (person_id);'); 

							PREPARE s1 from @dyn_sql; 
							EXECUTE s1; 
							DEALLOCATE PREPARE s1;  
                      
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
                        
                        
                        
                        
                        
                      end if;

                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    
                    
                     SET @dyn_sql=CONCAT('drop table ',@write_table,';'); 
					 PREPARE s1 from @dyn_sql; 
					 EXECUTE s1; 
					 DEALLOCATE PREPARE s1;  


  END$$
DELIMITER ;
