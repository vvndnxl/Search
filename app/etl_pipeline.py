import csv
import ast
import asyncio
from datetime import datetime
from elasticsearch.helpers import async_bulk
import pathlib

from app.database import AsyncSessionLocal, create_elastic_client
from app.models import DocumentModel

CHUNK_SIZE = 1000  # Safeguard boundary preventing Out-Of-Memory crashes


async def populate_search_engine_batched(csv_path: str):
    """
    Parses a CSV dataset and stream-injects elements in strict batch blocks
    to optimize memory footprints across Postgres and Elasticsearch engines.
    """
    print(f"[ETL] Initializing pipeline ingestion for source: {csv_path}")

    # Instantiate isolated client dedicated exclusively to this runtime thread
    es_client = create_elastic_client()

    current_batch_docs = []
    documents_to_index = []
    total_counter = 0

    with open(csv_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file, delimiter=',', quotechar='"')

        for row in reader:
            if not row or len(row) < 3:
                continue

            raw_text, raw_date, raw_rubrics = row[0], row[1], row[2]

            try:
                parsed_rubrics = ast.literal_eval(raw_rubrics.strip())
            except (ValueError, SyntaxError):
                parsed_rubrics = [r.strip() for r in raw_rubrics.strip("[]").split(",")]

            try:
                parsed_date = datetime.strptime(raw_date.strip(), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                parsed_date = datetime.utcnow()

            # Stage memory allocation configuration structures inside a local array
            current_batch_docs.append({
                "model": DocumentModel(rubrics=parsed_rubrics, text=raw_text, created_date=parsed_date),
                "text": raw_text
            })

            # When chunk capacity thresholds are hit, flush data down to databases
            if len(current_batch_docs) >= CHUNK_SIZE:
                total_counter += await flush_batch(current_batch_docs, documents_to_index, es_client)
                current_batch_docs.clear()
                documents_to_index.clear()

        # Handle trailing remaining offsets after completing primary loop
        if current_batch_docs:
            total_counter += await flush_batch(current_batch_docs, documents_to_index, es_client)

    await es_client.close()
    print(f"[ETL] Ingestion process complete. Successfully pipeline-routed {total_counter} total entries.")


async def flush_batch(batch_list: list, index_list: list, es_client) -> int:
    """Helper routine performing execution commits across databases inside a single block."""
    async with AsyncSessionLocal() as session:
        async with session.begin():
            for item in batch_list:
                model_inst = item["model"]
                session.add(model_inst)
                # Flush assigns sequential database primary IDs immediately
                await session.flush()

                index_list.append({
                    "_index": "documents",
                    "_id": str(model_inst.id),
                    "text": item["text"]
                })

    # Perform asynchronous parallel index update streaming inside Elasticsearch
    success, errors = await async_bulk(client=es_client, actions=index_list)
    if errors:
        print(f"[ETL] Alert: Encountered {len(errors)} payload errors during bulk index transfer processing.")

    return len(batch_list)



# async def populate_search_engine(csv_path: str):
#     """
#     Parses the provided CSV file, inserts records into PostgreSQL using ORM models
#     to ensure explicit type validation, and batch-indexes them into Elasticsearch.
#     """
#     print(f"Starting aligned ETL pipeline from {csv_path}...")
#     await init_infrastructure()
#
#     es_client = await get_elastic_client()
#     documents_to_index = []
#
#     async with AsyncSessionLocal() as session:
#         # Use transaction context explicitly
#         async with session.begin():
#             with open(csv_path, mode='r', encoding='utf-8') as file:
#                 reader = csv.reader(file, delimiter=',', quotechar='"')
#
#                 for row in reader:
#                     if not row or len(row) < 3:
#                         continue
#
#                     raw_text, raw_date, raw_rubrics = row[0], row[1], row[2]
#
#                     try:
#                         parsed_rubrics = ast.literal_eval(raw_rubrics.strip())
#                     except (ValueError, SyntaxError):
#                         parsed_rubrics = [r.strip() for r in raw_rubrics.strip("[]").split(",")]
#
#                     parsed_date = datetime.strptime(raw_date.strip(), "%Y-%m-%d %H:%M:%S")
#
#                     # Fix 1 & 2: Use ORM mapping. SQLAlchemy automatically converts
#                     # the Python list into a valid JSONB format for PostgreSQL.
#                     db_doc = DocumentModel(
#                         rubrics=parsed_rubrics,
#                         text=raw_text,
#                         created_date=parsed_date
#                     )
#                     session.add(db_doc)
#
#                     # Flush pushes changes to DB and populates db_doc.id as a pure int
#                     await session.flush()
#
#                     # Fix 3: Staging accurate, pure integer IDs for Elasticsearch indexation
#                     documents_to_index.append({
#                         "_index": settings.ELASTIC_INDEX,
#                         "_id": str(db_doc.id),
#                         "text": raw_text
#                     })
#
#             # Commit occurs automatically when exiting the session.begin() context manager
#         print("Successfully committed unified records to PostgreSQL.")
#
#     # 4. Stream data to Elasticsearch
#     if documents_to_index:
#         success, errors = await async_bulk(client=es_client, actions=documents_to_index)
#         print(f"Elasticsearch indexing completed: {success} items indexed. Errors: {len(errors)}")
#
#     await close_infrastructure()

# ... [Previous imports and populate_search_engine_batched() function remain unchanged] ...

if __name__ == "__main__":
    # 1. Get the absolute path of the current script: Search/app/etl_pipeline.py
    current_script_path = pathlib.Path(__file__).resolve()

    # 2. Go up exactly one directory level to find the project root workspace: Search/
    project_root_directory = current_script_path.parent.parent

    # 3. Securely pin the absolute location of data.csv relative to the root directory
    absolute_csv_path = project_root_directory / "data.csv"

    print(f"[ETL Runtime] Resolved absolute data target pathway to: '{absolute_csv_path}'")

    # Verify the target file actually exists before starting database transactions
    if not absolute_csv_path.exists():
        print(f"❌ CRITICAL ERROR: Ingestion stopped. Target file missing at: '{absolute_csv_path}'")
    else:
        # Pass the verified absolute path string to the bulk ingestion engine
        asyncio.run(populate_search_engine_batched(csv_path=str(absolute_csv_path)))

