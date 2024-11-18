import json

class Employee_Agg:
    def __init__(self,  emp_dept: str = '', emp_salary: int = 0):
        self.emp_dept = emp_dept
        self.emp_salary = emp_salary
        
    @staticmethod
    def from_csv_line(line):
        return Employee_Agg(line[0], line[1])

    def to_json(self):
        return json.dumps(self.__dict__)

class Employee_Salary:
    def __init__(self,  emp_dept: str = '', dept_div: str='', position_title: str='', hire_date: str='', salary: int = 0):
        self.emp_dept = emp_dept
        self.dept_div = dept_div
        self.position_title = position_title
        self.hire_date = hire_date
        self.salary = salary
        
    @staticmethod
    def from_csv_line(line):
        return Employee_Salary(line[0], line[1], line[2], line[3], line[4])

    def to_json(self):
        return json.dumps(self.__dict__)