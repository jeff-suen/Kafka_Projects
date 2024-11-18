-- Source Database
create schema if not exists project_2;

drop table if exists project_2.employees;
drop table if exists project_2.employees_cdc; 
drop table if exists project_2.employees_perm_cdc;

CREATE TABLE project_2.employees( 
	emp_id SERIAL primary key, 
	first_name VARCHAR(100), 
	last_name VARCHAR(100), 
	dob DATE, 
	city VARCHAR(100) 
	);

CREATE TABLE project_2.employees_cdc(
	action_id SERIAL primary key, 
	emp_id SERIAL, 
	first_name VARCHAR(100), 
	last_name VARCHAR(100), 
	dob DATE,
	city VARCHAR(100), 
	action VARCHAR(100) 
	);

CREATE TABLE project_2.employees_perm_cdc(
	action_id SERIAL primary key, 
	emp_id SERIAL, 
	first_name VARCHAR(100), 
	last_name VARCHAR(100), 
	dob DATE,
	city VARCHAR(100), 
	action VARCHAR(100) 
	);

-- Trigger Function
CREATE OR REPLACE FUNCTION trigger_emp_cdc()
	RETURNS TRIGGER AS $$
BEGIN

  IF TG_OP = 'INSERT' THEN
    INSERT INTO project_2.employees_cdc (emp_id, first_name, last_name, dob, city, action)
    VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, 'INSERT');
    RETURN NEW;

  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO project_2.employees_cdc (emp_id, first_name, last_name, dob, city, action)
    VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, 'UPDATE');
    RETURN NEW;

  ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO project_2.employees_cdc (emp_id, first_name, last_name, dob, city, action)
    VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, 'DELETE');
    RETURN OLD;

  END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS emp_trigger ON employees;

CREATE TRIGGER emp_trigger
AFTER INSERT OR UPDATE OR DELETE ON project_2.employees
FOR EACH ROW
EXECUTE FUNCTION trigger_emp_cdc();

-- The 1st round of modifications

INSERT INTO project_2.employees (first_name, last_name, dob, city)
VALUES 
    ('Max', 'Smith', '1990-01-01', 'New York'),
    ('Jane', 'Smith', '1985-05-15', 'Los Angeles'),
    ('Robert', 'Brown', '1992-10-20', 'Chicago'),
    ('Emily', 'Johnson', '1988-07-30', 'Houston');
  
UPDATE project_2.employees SET city = 'Seattle' WHERE emp_id = 3;

DELETE FROM project_2.employees WHERE emp_id = 1;

-- Checking tables
select * from project_2.employees_cdc;

select * from project_2.employees_perm_cdc;

select * from project_2.employees;

-- The 2nd round of modification

INSERT INTO project_2.employees (first_name, last_name, dob, city)
VALUES 
    ('Jeff', 'Sung', '1990-01-01', 'Boston'),
    ('Will', 'Luk', '1985-05-15', 'Dallas');

UPDATE project_2.employees SET city = 'Dallas' WHERE emp_id = 3;


-- Checking tables
select * from project_2.employees_cdc;

select * from project_2.employees_perm_cdc;

select * from project_2.employees;