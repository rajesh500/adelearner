from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# ============================================================
# STEP 1: Read CDC source incrementally
# ============================================================

@dp.view(name="customer_changes")
def customer_changes():

    return (
        spark.readStream.option("ignoreDeletes", "true")
             .table("rajeswariade.auto_cdc_demo.customer_cdc_source")
    )


# ============================================================
# STEP 2: Create target streaming table
# ============================================================

dp.create_streaming_table(
    name="customers_history"
)


# ============================================================
# STEP 3: Apply AUTO CDC
# ============================================================

dp.create_auto_cdc_flow(

    target="customers_history",

    source="customer_changes",

    keys=["customer_id"],

    sequence_by=col("sequence_num"),

    apply_as_deletes=expr(
        "operation = 'DELETE'"
    ),

    except_column_list=[
        "operation",
        "sequence_num"
    ],

    stored_as_scd_type=2
)