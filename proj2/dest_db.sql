-- Destination Database
create schema if not exists project_2;

drop table if exists postgres.project_2.employees;


CREATE TABLE if not exists postgres.project_2.employees( 
	emp_id SERIAL primary key,  
	first_name VARCHAR(100), 
	last_name VARCHAR(100), 
	dob DATE, 
	city VARCHAR(100) 
	);


select * from postgres.project_2.employees;