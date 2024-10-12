SELECT e.manager_id, e.salary FROM employees e WHERE e.salary = (SELECT MIN(salary) FROM employees WHERE manager_id IS NOT NULL);
