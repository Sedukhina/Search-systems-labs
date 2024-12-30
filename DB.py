import os
import sqlite3
import threading
from Domains import *
from LinkProcessing import GetDomainAndRelativePath

from enum import Enum

DB_FILENAME = "main.db"

db_lock = threading.Lock()

class LOCATION_TYPES(Enum):
    COUNTRY = 1
    STATE = 2
    SETTLEMENT = 3


def CreateMainDB():
    if not os.path.exists(DB_FILENAME):
        conn = sqlite3.connect(DB_FILENAME)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS domains (
            id INTEGER PRIMARY KEY,
            domain TEXT NOT NULL,
            open_page_rank REAL NOT NULL
        )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                domain_ID INTEGER NOT NULL,
                title     VARCHAR,
                description     VARCHAR,   
                relative_link VARCHAR NOT NULL,
                publication_date TIMESTAMP,
                FOREIGN KEY (domain_ID) REFERENCES domain (id) ON DELETE CASCADE,
                UNIQUE (domain_ID, relative_link) ON CONFLICT IGNORE
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword VARCHAR NOT NULL UNIQUE
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS keyword_link (
                link_ID INTEGER NOT NULL,
                keyword_ID INTEGER NOT NULL,
                FOREIGN KEY (link_ID) REFERENCES links (id) ON DELETE CASCADE,
                FOREIGN KEY (keyword_ID) REFERENCES keyword (id) ON DELETE CASCADE
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS countries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL,
                country_id INT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                FOREIGN KEY (country_id) REFERENCES countries(id)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settlements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(255) NOT NULL,
                state_id INT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                FOREIGN KEY (state_id) REFERENCES states(id)
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS links_locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                link_id INT NOT NULL,
                location_id INT NOT NULL,
                location_type INT NOT NULL,
                FOREIGN KEY (link_id) REFERENCES links(id) ON DELETE CASCADE
            );
        """)

        conn.commit()
        conn.close()

        PopulateDomainsDBfromCSV(DB_FILENAME)


def InsertLinkKeyword(link_id, keyword_id):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM links WHERE id = ? ", (link_id,))
        link = cursor.fetchone()
        if link == None:
            return None

        cursor.execute("SELECT * FROM keywords WHERE id = ? ", (keyword_id,))
        keyword = cursor.fetchone()
        if keyword == None:
            return None

        cursor.execute(" INSERT INTO keyword_link (link_id, keyword_id) VALUES (?, ?)", (link_id, keyword_id))
    
        conn.commit()
        conn.close()

# Inserts link in db and returns link_id
def InsertLink(domain, relative_link, title, description):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM domains WHERE domain = ? ", (domain,))
        domain_id = cursor.fetchone()
        if domain_id == None:
            return None

        cursor.execute(" INSERT INTO links (domain_id, title, description, relative_link) VALUES (?, ?, ?, ?)", (domain_id[0], title, description, relative_link))
        link_id = cursor.lastrowid
    
        conn.commit()
        conn.close()
        return link_id
    
# Inserts link in db and returns link_id
def InsertKeyword(keyword):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute(" INSERT INTO keywords (keyword) VALUES (?) ON CONFLICT DO NOTHING", ((keyword,)))
        keyword_id = cursor.lastrowid
    
        conn.commit()
        conn.close()
        return keyword_id


def GetLinkID(url):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        domain, rel = GetDomainAndRelativePath(url)

        cursor.execute("SELECT id FROM domains WHERE domain = ? ", (domain,))
        domain_id = cursor.fetchone()

        if domain_id == None:
            return None

        cursor.execute("SELECT id FROM links WHERE domain_id = ? AND relative_link = ?", (domain_id[0],rel))
        url_id = cursor.fetchone()
        conn.close()

        if url_id == None:
            return None

        return url_id[0]

def GetLinkTitleAndDescription(link_id):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT title, description FROM links WHERE id = ? ", (link_id,))
        res = cursor.fetchone()
        conn.close()

        if res == None:
            return None, None

        return res[0], res[1]

def GetKeywordID(keyword):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM keywords WHERE keyword = ? ", (keyword,))
        keyword_id = cursor.fetchone()

        conn.close()

        if keyword_id == None:
            return None

        return keyword_id[0]


def GetDomainByLinkID(link_id):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT domain_ID FROM links WHERE id = ? ", (link_id,))
        domain_id = cursor.fetchone()
        if domain_id == None:
            conn.close()
            return None

        cursor.execute("SELECT domain FROM domains WHERE id = ? ", (domain_id[0],))
        domain = cursor.fetchone()

        conn.close()
        if domain == None:
            return None

        return domain[0]


def GetDomainOpenPageRank(domain_name):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT open_page_rank FROM domains WHERE domain = ? ", (domain_name,))
        domain_authority = cursor.fetchone()

        conn.close()
        if domain_authority == None:
            return None
        return domain_authority[0]


def LinkKeywordConnectionExists(link_id, keyword_id):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM keyword_link WHERE link_id = ? AND keyword_id = ? ", (link_id, keyword_id))
        result = cursor.fetchone()

        conn.close()

        if result == None:
            return False

        return True


def GetLinkByID(link_id):
    with db_lock:
        domain = GetDomainByLinkID(link_id)
        if domain == None:
            return None

        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT relative_link FROM links WHERE id = ? ", (link_id,))
        link = cursor.fetchone()

        conn.close()
    
        if link == None:
            return None

    return domain, link[0]

def AddPublicationDate(date, link_id):
    conn = sqlite3.connect("main.db")
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM links WHERE id = ? ", (link_id,))
    link = cursor.fetchone()
    if link == None:
        conn.close()
        return None

    cursor.execute("UPDATE links SET publication_date = ? WHERE id = ?", (date, link_id))
    conn.commit()
    conn.close()


def GetCountryID(country):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM countries WHERE name = ? ", (country,))
        country_id = cursor.fetchone()

        conn.close()

        if country_id == None:
            return None

        return country_id[0]


def InsertCountry(country, latitude, longitude):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("INSERT INTO countries (name, latitude, longitude) VALUES (?, ?, ?) ON CONFLICT DO NOTHING", (country, latitude, longitude))
        country_id = cursor.lastrowid
    
        conn.commit()
        conn.close()
        return country_id


def GetStateID(state, country_id):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM states WHERE name = ? AND country_id = ?", (state, country_id))
        state_id = cursor.fetchone()

        conn.close()

        if state_id == None:
            return None

        return state_id[0]


def InsertState(state, country_id, latitude, longitude):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM countries WHERE id = ? ", (country_id,))
        country_existence_test = cursor.fetchone()
        if country_existence_test == None:
            return None

        cursor.execute("INSERT INTO states (name, country_id, latitude, longitude) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING", (state, country_id, latitude, longitude))
        state_id = cursor.lastrowid
    
        conn.commit()
        conn.close()
        return state_id


def GetSettlementID(state, state_id):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM states WHERE name = ? AND id = ?", (state, state_id))
        state_id = cursor.fetchone()

        conn.close()

        if state_id == None:
            return None

        return state_id[0]


def InsertSettlement(settlement, state_id, latitude, longitude):
    with db_lock:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM states WHERE id = ? ", (state_id,))
        country_existence_test = cursor.fetchone()
        if country_existence_test == None:
            return None

        cursor.execute("INSERT INTO settlements (name, state_id, latitude, longitude) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING", (settlement, state_id, latitude, longitude))
        settlement_id = cursor.lastrowid
    
        conn.commit()
        conn.close()
        return settlement_id


def AddLocationConnection(link_id, settlement, state, country, settlement_coords, state_coords, country_coords):
    if country:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        # Checking if link id is valid
        cursor.execute("SELECT * FROM links WHERE id = ? ", (link_id,))
        link = cursor.fetchone()
        if link == None:
            conn.close()
            return None

        country_id = GetCountryID(country)
        if not country_id:
            country_id = InsertCountry(country, country_coords[0], country_coords[1])

        if state:
            state_id = GetStateID(state, country_id)
            if not state_id:
                state_id = InsertState(state, country_id, state_coords[0], state_coords[1])

            if settlement:
                settlement_id = GetSettlementID(settlement, state_id)
                if not settlement_id:
                    settlement_id = InsertSettlement(settlement, state_id, settlement_coords[0], settlement_coords[1])
                cursor.execute("INSERT INTO links_locations (link_id, location_id, location_type) VALUES (?, ?, ?) ON CONFLICT DO NOTHING", (link_id, settlement_id, LOCATION_TYPES.SETTLEMENT.value))
            else:
                cursor.execute("INSERT INTO links_locations (link_id, location_id, location_type) VALUES (?, ?, ?) ON CONFLICT DO NOTHING", (link_id, state_id, LOCATION_TYPES.STATE.value))
        else:
            cursor.execute("INSERT INTO links_locations (link_id, location_id, location_type) VALUES (?, ?, ?) ON CONFLICT DO NOTHING", (link_id, country_id, LOCATION_TYPES.COUNTRY.value))

        conn.commit()
        conn.close()


def GetURLCoords(link_id):
    if link_id:
        conn = sqlite3.connect("main.db")
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM links WHERE id = ? ", (link_id,))
        link = cursor.fetchone()
        if link == None:
            conn.close()
            return None

        cursor.execute("SELECT location_id, location_type FROM links_locations WHERE link_id = ? ", (link_id,))
        locations = cursor.fetchall()
        
        conn.close()
        
        result = []
        for location_id, location_type in locations:
            if location_type == LOCATION_TYPES.SETTLEMENT.value:
                cursor.execute("SELECT latitude, longitude FROM settlement WHERE id = ? ", (location_id,))
            elif location_type == LOCATION_TYPES.STATE.value:
                cursor.execute("SELECT latitude, longitude FROM state WHERE id = ? ", (location_id,))
            else:
                cursor.execute("SELECT latitude, longitude FROM countr WHERE id = ? ", (location_id,))
            res = cursor.fetchone()
            if res:
                result.append(res)
                
        return result
    return None
