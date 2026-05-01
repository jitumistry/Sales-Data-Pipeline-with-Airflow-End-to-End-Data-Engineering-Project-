from sqlalchemy import create_engine

# Source DB connection
def get_source_engine():
    return create_engine(
        "postgresql://admin:admin@localhost:5433/sales_source"
    )

# Warehouse DB connection
def get_warehouse_engine():
    return create_engine(
        "postgresql://admin:admin@localhost:5434/sales_wh"
    )
