IMPORT FOREIGN SCHEMA
=====================

Here are some examples:

```sql
-- IMPORT hadoop test_schema to the local SCHEMA.
IMPORT FOREIGN SCHEMA test_schema
    FROM SERVER hadoop_server INTO test_schema;

-- IMPORT only test_tab1, test_tab2 from hadoop test_schema to the local
-- SCHEMA.
IMPORT FOREIGN SCHEMA test_schema
    LIMIT TO (test_tab1, test_tab2)
    FROM SERVER hadoop_server INTO test_schema;

-- IMPORT all other objects from the hadoop test_schema SCHEMA EXCEPT
-- test_tab1 and test_tab2.
IMPORT FOREIGN SCHEMA test_schema
    EXCEPT (test_tab1, test_tab2)
    FROM SERVER hadoop_server INTO test_schema;
```
