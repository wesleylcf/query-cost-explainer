import logging
from explain import Explainer
import psycopg2;

conn = psycopg2.connect(
    dbname="tpc",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)

print('Initializing Explainer...')
explainer = Explainer(conn, True)
print('Initialized Explainer.')

tableToPKey = { 'region': 'r_regionkey', 'nation': 'n_nationkey', 'part': 'p_partkey', 'supplier': 's_suppkey', 'partsupp': 'ps_partkey, ps_suppkey', 'customer': 'c_custkey', 'orders': 'o_orderkey', 'lineitem': 'l_orderkey, l_partkey, l_suppkey, l_linenumber'}
seq_scan_queries = [f"SELECT * FROM public.{name};" for name in explainer.tableSet]
index_only_queries = [f"SELECT {tableToPKey[name]} FROM public.{name};" for name in explainer.tableSet]

print('Testing Seq Scan...')
for qry in seq_scan_queries:
  print(qry)
  out = explainer.run_explain(qry)
  explainer.analyze_execution_plan(out)
  print(f"OK")

print('Testing Index Only Scan...')
for qry in index_only_queries:
  print(qry)
  out = explainer.run_explain(qry)
  explainer.analyze_execution_plan(out)
  print(f"OK")

print('Testing Nested Loop...')
for tableName in explainer.tableSet:
  qry = f"SELECT * FROM {tableName} CROSS JOIN public.nation;"
  if tableName == 'nation':
    qry = f"SELECT * FROM {tableName} CROSS JOIN public.region;"
  print(qry)
  out = explainer.run_explain(qry)
  explainer.analyze_execution_plan(out)
  print(f"OK")