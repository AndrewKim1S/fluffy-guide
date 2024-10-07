SELECT department_name, COUNT(employee_id) FROM departments LEFT JOIN employees ON employees.department_id = departments.department_id GROUP BY department_name ORDER BY COUNT(employee_id);
