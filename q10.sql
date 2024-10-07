SELECT employees.employee_id FROM employees LEFT JOIN dependents ON dependents.employee_id = employees.employee_id WHERE dependents.dependent_id IS NULL;
