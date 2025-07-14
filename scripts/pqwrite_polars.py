from pprint import pprint
import pyarrow
from pyarrow import parquet
import polars

file_name_pq = r"C:\dev\data\1217\out\policy_anchor.ael.20240930.1734354272.8351038.parquet"
file_name_csv = r"C:\dev\data\1217\policy_anchor.ael.20240930.1734354272.8351038.csv"


# "policy_id","effective_date","policy_number","block","admin_system","as400_company","cm_policy_id"

policy_anchor_schema = {
    "policy_id": polars.Int32,
    "effective_date": polars.Date,
    "policy_number": polars.String,
    "block": polars.String,
    "admin_system": polars.String,
    "as400_company": polars.String,
    "cm_policy_id": polars.Int64,
}

pprint(policy_anchor_schema)


df = polars.read_csv(
    file_name, has_header=True, schema=policy_anchor_schema, separator=","
)

rc = 0
for r in df.iter_rows(named=True):
    #    print(r)
    rc += 1

print(rc)

print(df.shape, df.schema)

file_name_out = r"C:\dev\data\anchors_stage.csv.parquet"

df.write_parquet(file_name_out)

pq_file = pyarrow.parquet.ParquetFile(file_name_out)
print(pq_file)
print(pq_file.schema)
pa_table = pq_file.read()

print(pa_table)
