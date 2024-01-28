# SQL Cache

Experiment with loading data from a database into a in-memory database with some goals:

- Use SQL (SQLAlchemy) everywhere
- Fast in-memory queries with "SQL-like" guarantees (FK, joins, groupby)
- Good perf? reduce serde/network overhead? (kinda rules out redis?)


The main use case is reducing load on main DB and by executing repeated analytics queries against the in-memory cached data.