import sys

from dotenv import load_dotenv
from neo4j import GraphDatabase
import os

Q1 = """
MATCH (o:Order)-[l:LINE_ITEM]->(ps:PartSup)
WHERE l.shipdate <= date("2024-05-01")
WITH l.returnflag AS l_returnflag, l.linestatus AS l_linestatus,
     sum(l.quantity) AS sum_qty,
     sum(l.extendedprice) AS sum_base_price,
     sum(l.extendedprice * (1 - l.discount)) AS sum_disc_price,
     sum(l.extendedprice * (1 - l.discount) * (1 + l.tax)) AS sum_charge,
     avg(l.quantity) AS avg_qty,
     avg(l.extendedprice) AS avg_price,
     avg(l.discount) AS avg_disc,
     count(*) AS count_order
RETURN l_returnflag, l_linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order
ORDER BY l_returnflag, l_linestatus;
"""

Q2 = """ 
MATCH (p:Part)-[:GIVES_PART]->(ps:PartSup)<-[:SUPPLIES_PART]-(s:Sup),
      (s)<-[:HAS_SUPPLIER]-(n:Nation)<-[:HAS_NATION]-(r:Region {name: 'Europe'})
WITH MIN(ps.supplycost) AS min_supplycost

MATCH (p:Part {size: 100})-[:GIVES_PART]->(ps:PartSup {supplycost: min_supplycost})<-[:SUPPLIES_PART]-(s:Sup),
    (s)<-[:HAS_SUPPLIER]-(n:Nation)<-[:HAS_NATION]-(r:Region {name: 'Europe'})
WHERE p.type CONTAINS 'TypeA'
RETURN s.acctbal AS s_acctbal,
    s.name AS s_name,
    n.name AS n_name,
    p.partkey AS p_partkey,
    p.mfgr AS p_mfgr,
    s.address AS s_address,
    s.phone AS s_phone,
    s.comment AS s_comment
ORDER BY s.acctbal DESC, n.name, s.name, p.partkey
"""

Q3 = """
MATCH (c:Customer)-[:PLACED]->(o:Order)-[l:LINE_ITEM]->(ps:PartSup)
WHERE c.mktsegment = 'SegmentB' // Segmento de mercado
    AND o.orderdate < date('2021-03-02') // Fecha en formato 'YYYY-MM-DD'
    AND l.shipdate > date('2024-04-01') // Fecha en formato 'YYYY-MM-DD'
WITH o.orderkey AS l_orderkey, 
     o.orderdate AS o_orderdate,
     o.shippriority AS o_shippriority,
     SUM(l.extendedprice * (1 - l.discount)) AS revenue
RETURN l_orderkey, revenue, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate;
"""

Q4 = """
MATCH (r:Region)-[:HAS_NATION]->(n:Nation)-[:HAS_CUSTOMER]->(c:Customer)-[:PLACED]->(o:Order)-[l:LINE_ITEM]->(ps:PartSup)<-[:SUPPLIES_PART]-(s:Sup),
      (s)-[:HAS_SUPPLIER]->(n)  // Asegura que el proveedor está en la misma nación
WHERE r.name = 'Asia'
  AND o.orderdate >= date('2021-01-01')
  AND o.orderdate < date({ year: date('2021-01-01').year + 1, month: date('2021-01-01').month, day: date('2021-01-01').day })
WITH n.name AS n_name,
     SUM(l.extendedprice * (1 - l.discount)) AS revenue
RETURN n_name, revenue
ORDER BY revenue DESC;
"""



def drop_all_constraints_and_indexes(session):
    constraints = session.run("SHOW CONSTRAINTS")
    for record in constraints:
        constraint_name = record["name"]
        session.run(f"DROP CONSTRAINT {constraint_name}")
    
    indexes = session.run("SHOW INDEXES")
    for record in indexes:
        index_name = record["name"]
        session.run(f"DROP INDEX {index_name}")

def clear_database(session):
    session.run("MATCH (n) DETACH DELETE n")

def create_indices_and_constraints(session):

    session.run("CREATE CONSTRAINT FOR (p:Part) REQUIRE p.partkey IS UNIQUE")
    session.run("CREATE CONSTRAINT FOR (s:Sup) REQUIRE s.suppkey IS UNIQUE")
    session.run("CREATE CONSTRAINT FOR (c:Customer) REQUIRE c.custkey IS UNIQUE")
    session.run("CREATE CONSTRAINT FOR (o:Order) REQUIRE o.orderkey IS UNIQUE")
    session.run("CREATE CONSTRAINT FOR (n:Nation) REQUIRE n.nationkey IS UNIQUE")
    session.run("CREATE CONSTRAINT FOR (r:Region) REQUIRE r.regionkey IS UNIQUE")

    session.run("CREATE INDEX shipdate FOR ()-[l:LINE_ITEM]->() ON (l.shipdate)")

    session.run("CREATE INDEX size_type FOR (p:Part) ON (p.size, p.type)")
    session.run("CREATE INDEX name FOR (r:Region) ON (r.name)")

    session.run("CREATE INDEX c_mktsegment FOR (c:Customer) ON (c.mktsegment)")
    session.run("CREATE INDEX o_orderdate FOR (o:Order) ON (o.orderdate)")
    print("Índices y restricciones creados con éxito.")

def create_data(session):
    session.run("""
        CREATE 
            (p:Part {partkey: "P12345", mfgr: "ManufacturerX", type: "TypeA", size: 100}),
            (p2:Part {partkey: "P54321", mfgr: "ManufacturerY", type: "TypeB", size: 200}),
                
            (s:Sup {suppkey: "S001", name: "SupplierA", address: "1234 Elm Street, Cityville", phone: "555-123-4567", acctbal: 25000.75, comment: "Proveedor confiable y eficiente."}),
            (s2:Sup {suppkey: "S002", name: "SupplierB", address: "5678 Oak Street, Townville", phone: "555-987-6543", acctbal: 15000.50, comment: "Proveedor de alta calidad."}),
                
            (ps: PartSup {supplycost: 100.50}),
            (ps2: PartSup {supplycost: 200.75}),
                
            //Relaciones Part i Sup con PartSup
            (p)-[:GIVES_PART]->(ps),
            (s)-[:SUPPLIES_PART]->(ps),
                
            (p2)-[:GIVES_PART]->(ps2),
            (s2)-[:SUPPLIES_PART]->(ps2),
                
            (c:Customer {custkey: "C98765", mktsegment: "SegmentA"}),
            (c2:Customer {custkey: "C56789", mktsegment: "SegmentB"}),

            (n: Nation {nationkey: "N001", name: "Spain"}),
            (n2: Nation {nationkey: "N002", name: "Japan"}),

            (r: Region {regionkey: "R001", name: "Europe"}),  
            (r2: Region {regionkey: "R002", name: "Asia"}),

            //Relaciones Region i Nation con Customer y Supplier    
            (r)-[:HAS_NATION]->(n),
            (r2)-[:HAS_NATION]->(n2),
                
            (n)-[:HAS_CUSTOMER]->(c),
            (n2)-[:HAS_CUSTOMER]->(c2),
                
            (n)-[:HAS_SUPPLIER]->(s),
            (n2)-[:HAS_SUPPLIER]->(s2),

            (o: Order {orderkey: "O12345", orderdate: date("2021-01-01"), shippriority: 1}),
            (o2: Order {orderkey: "O54321", orderdate: date("2021-02-02"), shippriority: 2}),

            //Relaciones Customer i Order    
            (c)-[:PLACED]->(o),
            (c2)-[:PLACED]->(o2),
                
            //Relaciones Order i PartSup (lineItem)
            (o)-[:LINE_ITEM {returnflag: "Y", linestatus: 1, quantity: 10.5, tax: 2.5, shipdate: date("2024-05-01"), extendedprice: 1050.0, discount: 0.05}]->(ps),
            (o2)-[:LINE_ITEM {returnflag: "N", linestatus: 0, quantity: 20.5, tax: 3.5, shipdate: date("2024-06-01"), extendedprice: 2050.0, discount: 0.10}]->(ps2)              
            """)

def print_plan(step, indent=0):
    prefix = "  " * indent
    operator_type = step.get('operatorType', 'UnknownOperator')
    arguments = step.get('arguments', {})
    print(f"{prefix}- {operator_type}@neo4j: {arguments}")
    children = step.get('children', [])
    for child in children:
        print_plan(child, indent+1)

def run_explain_and_query(session, query):

    explain_query = "EXPLAIN " + query
    result = session.run(explain_query)
    summary = result.consume()
    plan = summary.plan
    if plan:
        print("Plan de Ejecución:")
        print_plan(plan)
    else:
        print("No se obtuvo un plan de ejecución.")

    result = session.run(query)
    records = list(result)
    if records:
        print("Resultados:")
        for record in records:
            print(record)
    else:
        print("La consulta no devolvió resultados.")


if __name__ == "__main__":
    load_dotenv()
    NEO4J_URI = os.environ["NEO4J_URI"]
    NEO4J_USER = os.environ["NEO4J_USER"]
    NEO4J_PASSWORD = os.environ["NEO4J_PASSWORD"]

    if not NEO4J_URI or not NEO4J_USER or not NEO4J_PASSWORD:
        print("Faltan variables de entorno. Asegúrate de que .env contenga NEO4J_URI, NEO4J_USER y NEO4J_PASSWORD.")
        sys.exit(1)

    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
    except Exception as e:
        print("Error al conectar al servidor de Neo4j:", e)
        sys.exit(1)

    QUERIES = { "Q1": Q1, "Q2": Q2, "Q3": Q3, "Q4": Q4 }
    
    with driver.session() as session:

        drop_all_constraints_and_indexes(session)

        clear_database(session)

        create_indices_and_constraints(session)
        create_data(session)
        print("Nodos, relaciones e índices creados con éxito.")

        while True:
            print("Ingrese la consulta a ejecutar (o 'exit' para salir):")
            print("[Q1, Q2, Q3, Q4]")
            query = input()
            if query == "exit":
                break
            if query not in QUERIES:
                print("Consulta no válida.")
                continue
            else:
                run_explain_and_query(session, QUERIES[query])

    driver.close()