# backend/scripts/generate_and_copy.py
import csv
import os
import random
import uuid
import tempfile
from decimal import Decimal
from faker import Faker
from dotenv import load_dotenv
from urllib.parse import urlparse, unquote
import psycopg2
from tqdm import tqdm

load_dotenv()  # read DATABASE_URL from backend/.env

faker = Faker("en_US")

STATUS_CHOICES = ["open", "paid", "denied", "pending", "rejected"]
TYPE_CHOICES = ["hospitalization", "pharmacy", "consultation", "lab", "therapy", "dental", "vision"]

def make_row(file_source: str):
    claim_amount = round(random.uniform(50.0, 20000.0), 2)
    settlement_amount = round(claim_amount * random.uniform(0.3, 1.0), 2)
    return [
        str(uuid.uuid4()),
        f"PN-{faker.bothify(text='????-#####')}",
        faker.date_between(start_date='-2y', end_date='today').isoformat(),
        f"{claim_amount:.2f}",
        random.choice(STATUS_CHOICES),
        random.choice(TYPE_CHOICES),
        f"{settlement_amount:.2f}",
        str(random.randint(0, 120)),
        faker.bothify(text='D??###'),
        f"P-{faker.bothify(text='#####')}",
        file_source
    ]

def get_db_conn_from_database_url():
    durl = os.getenv("DATABASE_URL")
    if not durl:
        raise RuntimeError("DATABASE_URL not set in environment (.env)")
    # normalize if using SQLAlchemy style prefix
    if durl.startswith("postgresql+psycopg2://"):
        durl = durl.replace("postgresql+psycopg2://", "postgresql://", 1)
    parsed = urlparse(durl)
    user = parsed.username
    pwd = unquote(parsed.password) if parsed.password else None
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    dbname = parsed.path.lstrip("/")
    conn = psycopg2.connect(dbname=dbname, user=user, password=pwd, host=host, port=port)
    return conn

def generate_csv(path: str, rows: int, file_source: str):
    header = ["claim_id","policy_number","claim_date","claim_amount","claim_status",
              "claim_type","settlement_amount","processing_days","diagnosis_code",
              "provider_id","file_source"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for _ in tqdm(range(rows), desc="Generating CSV"):
            writer.writerow(make_row(file_source))

def copy_csv_to_db(csv_path: str):
    conn = get_db_conn_from_database_url()
    cur = conn.cursor()
    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            "COPY claims (claim_id, policy_number, claim_date, claim_amount, claim_status, claim_type, settlement_amount, processing_days, diagnosis_code, provider_id, file_source) FROM STDIN WITH CSV HEADER",
            f
        )
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=100000, help="Number of fake rows to generate")
    p.add_argument("--file-source", type=str, default="faker_copy", help="file_source column value")
    args = p.parse_args()

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    tmp.close()
    csv_path = tmp.name
    print("Generating CSV at:", csv_path)
    generate_csv(csv_path, args.rows, args.file_source)
    print("CSV generated — starting COPY into DB (this may take a while)...")
    copy_csv_to_db(csv_path)
    print("COPY complete. Removing temp CSV.")
    os.remove(csv_path)
    print("Done.")

