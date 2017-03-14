JOIN PUSH DOWN
==============

Here are some examples:

```sql
-- Evaluate the JOIN condition (a.dept_id=b.dept_id) on the Hive server.
SELECT a.id,a.name,a.doj,b.DEPT
    FROM TEST_SCHEMA.COMPANY a JOIN TEST_SCHEMA.DEPARTMENT b
        ON (a.dept_id=b.dept_id);
-- Assume TAB3 is not a Hive (foreign) table. The JOIN of TAB1 with
-- TAB2 is evaluated on the Hive server and the JOIN with TAB3 is
-- evaluated on the PostgreSQL server.
SELECT t1.b, t3.e FROM TEST_SCHEMA.TAB1 t1
    LEFT OUTER JOIN TAB3 t3
        ON t1.b = t3.e
    JOIN TEST_SCHEMA.TAB2 t2
        ON t1.a = t2.c;
```
