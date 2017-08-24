# MiniSql-Engine
Mini​ sql engine which will run a subset of SQL Queries using ​ command line interface​.

## Type of Queries:​​
1. Select all records :
```sql
	> select * from table_name;
```
2. Aggregate functions: Simple aggregate functions on a single column. Sum, average, max and min. They will be very trivial given that the data is only numbers.
```sql
	Select max(col1) from table1;
```
3. Project Columns(could be any number of columns) from one or more tables :
```sql
	Select col1, col2 from table_name;
```
4. Select/project with distinct from one table : Select distinct(col1), distinct(col2) from table_name;

5. Select with where from one or more tables :
```sql
	Select col1,col2 from table1,table2 where col1 = 10 AND col2 = 20;
```
	a. In the where queries, there would be a maximum of one AND/OR operator with no NOT operators.
6. Projection of one or more(including all the columns) from two tables with one join:
condition :
```sql
> Select * from table1, table2 where table1.col1=table2.col2;
> Select col1,col2 from table1,table2 where table1.col1=table2.col2;
```
