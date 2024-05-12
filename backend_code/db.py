import sqlite3
from threading import Lock

class ConnectionPool:
    def __init__(self, db_file, max_connections=30):
        self.db_file = db_file
        self.max_connections = max_connections
        self.pool = []
        self.lock = Lock()

    def get_connection(self):
        with self.lock:
            if self.pool:
                return self.pool.pop()
            elif len(self.pool) < self.max_connections:
                conn = sqlite3.connect(self.db_file, check_same_thread=True)
                return conn
            else:
                raise RuntimeError("Maximum number of connections reached")

    def release_connection(self, conn):
        with self.lock:
            self.pool.append(conn)

    def close_all_connections(self):
        with self.lock:
            for conn in self.pool:
                conn.close()
            self.pool.clear()

# Create a connection pool
pool = ConnectionPool('web_crawler.db', max_connections=30)

def init_connection():
    conn = pool.get_connection()
    cursor = conn.cursor()
    return conn, cursor

def close_connection(conn):
    pool.release_connection(conn)


def create_tables(conn, cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS page_mapping (
            page_id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stemmed_mapping (
            word_id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS non_stemmed_mapping (
            word_id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS page_info (
            page_id INTEGER PRIMARY KEY,
            page_size INTEGER,
            last_modified TEXT,
            title TEXT,
            body TEXT,
            child_links TEXT,
            FOREIGN KEY(page_id) REFERENCES page_mapping(page_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS forwardIndex_body (
            page_id INTEGER,
            word_id INTEGER,
            frequency INTEGER,
            positions TEXT,
            PRIMARY KEY(page_id, word_id),
            FOREIGN KEY(page_id) REFERENCES page_mapping(page_id),
            FOREIGN KEY(word_id) REFERENCES stemmed_mapping(word_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS forwardIndex_title (
            page_id INTEGER,
            word_id INTEGER,
            frequency INTEGER,
            positions TEXT,
            PRIMARY KEY(page_id, word_id),
            FOREIGN KEY(page_id) REFERENCES page_mapping(page_id),
            FOREIGN KEY(word_id) REFERENCES stemmed_mapping(word_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS invertedIndex_body (
            word_id INTEGER PRIMARY KEY,
            pages_freq TEXT,
            FOREIGN KEY(word_id) REFERENCES stemmed_mapping(word_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS invertedIndex_title (
            word_id INTEGER PRIMARY KEY,
            pages_freq TEXT,
            FOREIGN KEY(word_id) REFERENCES stemmed_mapping(word_id)
        )
    """)

def populate_mapping(conn, cursor, attributes, table):
    if table == 'page_mapping':
        attribute_set = set(attributes)
        cursor.execute(f"SELECT url FROM {table} WHERE url IN ({','.join(['?'] * len(attribute_set))})", tuple(attribute_set))
        existing_urls = set(row[0] for row in cursor.fetchall())
        new_urls = attribute_set - existing_urls
        if new_urls:
            cursor.executemany(f"INSERT or REPLACE INTO {table} (url) VALUES (?)", [(url,) for url in new_urls])
            conn.commit()
    elif table in ['stemmed_mapping', 'non_stemmed_mapping']:
        attribute_set = set(attributes)
        cursor.execute(f"SELECT word FROM {table} WHERE word IN ({','.join(['?'] * len(attribute_set))})", tuple(attribute_set))
        existing_words = set(row[0] for row in cursor.fetchall())
        new_words = attribute_set - existing_words
        if new_words:
            cursor.executemany(f"INSERT or REPLACE INTO {table} (word) VALUES (?)", [(word,) for word in new_words])
            conn.commit()
    else:
        print("Invalid table name. Please choose from page_mapping, stemmed_mapping or non_stemmed_mapping.")

def get_id(conn, cursor, table, data, attribute):
    cursor.execute(f"SELECT rowid FROM {table} WHERE {attribute} = ?", (data,))
    data = cursor.fetchone()
    if data is None:
        cursor.execute(f"INSERT or REPLACE INTO {table} ({attribute}) VALUES (?)", (data,))
        conn.commit()
        cursor.execute(f"SELECT last_insert_rowid() FROM {table}")
        data = cursor.fetchone()

    return data[0]

def populate_pageinfo(conn, cursor, pages):
    for url, info in pages.items():
        page_id = get_id(conn, cursor, 'page_mapping', url, 'url')
        cursor.execute("""
            INSERT or REPLACE INTO page_info (page_id, page_size, last_modified, title, body, child_links)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (page_id, info['page_size'], info['last_modified'], info['title'], info['body'], ','.join(info['child_links'])))

def populate_forward_index(conn, cursor, url, word, frequency, positions, table):
    if table in ['forwardIndex_body', 'forwardIndex_title']:
        page_id = get_id(conn, cursor, 'page_mapping', url, 'url')
        word_id = get_id(conn, cursor, 'stemmed_mapping', word, 'word')
        cursor.execute(f"""
            INSERT or REPLACE INTO {table} (page_id, word_id, frequency, positions)
            VALUES (?, ?, ?, ?)
        """, (page_id, word_id, frequency, ','.join(map(str, positions))))
        conn.commit()
    else:
        print("Invalid table name. Please choose from forwardIndex_body or forwardIndex_title.")

def populate_inverted_index(conn, cursor, word, page_freq, table):
    if table in ['invertedIndex_body', 'invertedIndex_title']:
        word_id = get_id(conn, cursor, 'stemmed_mapping', word, 'word')
        cursor.execute(f"""
            INSERT or REPLACE INTO {table} (word_id, pages_freq)
            VALUES (?, ?)
        """, (word_id, page_freq))
        conn.commit()
    else:
        print("Invalid table name. Please choose from invertedIndex_body or invertedIndex_title.")
