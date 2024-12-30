import sqlite3
import csv

DOMAINS_CSV = "top10milliondomains.csv"

def PopulateDomainsDBfromCSV(db_filename):
    conn = sqlite3.connect(db_filename)
    cursor = conn.cursor()

    with open(DOMAINS_CSV, "r") as file:
        reader = csv.reader(file)
        next(reader) 
        for row in reader:
            cursor.execute("INSERT INTO domains (id, domain, open_page_rank) VALUES (?, ?, ?)", (row[0], row[1], row[2]))

    conn.commit()
    conn.close()

