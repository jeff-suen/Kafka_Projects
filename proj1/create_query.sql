DROP TABLE IF EXISTS project_1.department_employee;
DROP TABLE IF EXISTS project_1.department_employee_salary;

CREATE TABLE project_1.department_employee(
department VARCHAR(100),
department_division VARCHAR(100),
position_title VARCHAR(100),
hire_date DATE,
salary decimal
);

CREATE TABLE project_1.department_employee_salary(
department varchar(100) NOT NULL,
total_salary int4 NULL,
CONSTRAINT department_employee_salary_pk PRIMARY KEY (department)
);

select *
from project_1.department_employee_salary;

select *
from project_1.department_employee;