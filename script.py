import sys
from neo4j import GraphDatabase
import os

def drop_all_constraints_and_indexes(session):
    # Eliminar todos los constraints
    constraints = session.run("SHOW CONSTRAINTS")
    for record in constraints:
        constraint_name = record["name"]
        session.run(f"DROP CONSTRAINT {constraint_name}")
    
    # Eliminar todos los índices
    indexes = session.run("SHOW INDEXES")
    for record in indexes:
        index_name = record["name"]
        session.run(f"DROP INDEX {index_name}")

def clear_database(session):
    session.run("MATCH (n) DETACH DELETE n")

def create_indices_and_constraints(session):
    #Constraints
    session.run("CREATE CONSTRAINT FOR (p:Part) REQUIRE p.partkey IS UNIQUE")
    session.run("CREATE CONSTRAINT FOR (s:Sup) REQUIRE s.suppkey IS UNIQUE")
    session.run("CREATE CONSTRAINT FOR (c:Customer) REQUIRE c.custkey IS UNIQUE")
    session.run("CREATE CONSTRAINT FOR (o:Order) REQUIRE o.orderkey IS UNIQUE")
    session.run("CREATE CONSTRAINT FOR (n:Nation) REQUIRE n.nationkey IS UNIQUE")
    session.run("CREATE CONSTRAINT FOR (r:Region) REQUIRE r.regionkey IS UNIQUE")

    #Indices querys WHERE
    #Q1
    session.run("CREATE INDEX shipdate FOR ()-[l:LINE_ITEM]->() ON (l.shipdate)")
    #Q2
    session.run("CREATE INDEX size_type FOR (p:Part) ON (p.size, p.type)")
    session.run("CREATE INDEX name FOR (r:Region) ON (r.name)")
    #Q3
    session.run("CREATE INDEX c_mktsegment FOR (c:Customer) ON (c.mktsegment)")
    session.run("CREATE INDEX o_orderdate FOR (o:Order) ON (o.orderdate)")
    #Indice shipdate ya creado en Q1
    #Q4
    #Indices para name de region y orderdate de order ya creados en Q2 y Q3
    print("Índices y restricciones creados con éxito.")

def create_data(session):
    #Hay que revisar la dirección de las relaciones para que sea lo mas optimas o si hace falta duplicar relacion. Esto se debe porque son dirigidas.
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

            (o: Order {orderkey: "O12345", orderdate: "2021-01-01", shippriority: 1}),
            (o2: Order {orderkey: "O54321", orderdate: "2021-02-02", shippriority: 2}),

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

    QUERIES = []
    
    with driver.session() as session:

        drop_all_constraints_and_indexes(session)

        clear_database(session)

        create_indices_and_constraints(session)
        create_data(session)
        print("Nodos, relaciones e índices creados con éxito.")

        #for i, q in enumerate(QUERIES, start=1):
            #print(f"\n--- Ejecución de consulta {i} ---")
            #run_explain_and_query(session, q)

    driver.close()