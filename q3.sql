SELECT e.first_name, j.max_salary FROM employees e, departments d, jobs j WHERE e.department_id = d.department_id and e.job_id = j.job_id GROUP BY d.department_id HAVING AVG(j.max_salary) > 8000;
