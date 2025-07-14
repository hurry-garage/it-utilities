import boto3

glue_client = boto3.client("glue", "us-east-1")


database_name = "aade_ingestion"
# table_names = ["policy", "strategy", "income_rider", "death_benefit_rider"]

table_names = [
    "iicanf02",
    "iicanf06",
    "iicanf08",
    "iicanf09",
    "iicanf10",
    "iicanf15",
    "iicanf24",
    "iicanf26",
    "unfanf00a",
    "unfanf01a",
    "unfanf02",
    "unfanf06",
    "unfanf08",
    "unfanf09",
    "unfanf10",
    "unfanf15",
    "unfanf24",
]
block = "2025"
val_date = "4"


for table_name in table_names:
    response = glue_client.delete_partition(
        # CatalogId='string',
        DatabaseName=database_name,
        TableName=table_name,
        PartitionValues=[block, val_date],
    )
    print(response)
