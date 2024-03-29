import sqlite3

conn = sqlite3.connect('web_crawler.db')
cursor = conn.cursor()

def create_tables():
    cursor.execute("""
        CREATE TABLE page_mapping (
            page_id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE stemmed_mapping (
            word_id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE non_stemmed_mapping (
            word_id INTEGER PRIMARY KEY AUTOINCREMENT,
            word TEXT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE page_info (
            page_id INTEGER PRIMARY KEY,
            last_modified TEXT,
            title TEXT,
            body TEXT,
            child_links TEXT,
            FOREIGN KEY(page_id) REFERENCES page_mapping(page_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE forwardIndex_body (
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
        CREATE TABLE forwardIndex_title (
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
        CREATE TABLE invertedIndex_body (
            word_id INTEGER PRIMARY KEY,
            pages_body_freq TEXT,
            FOREIGN KEY(word_id) REFERENCES stemmed_mapping(word_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE invertedIndex_title (
            word_id INTEGER PRIMARY KEY,
            pages_title_freq TEXT,
            FOREIGN KEY(word_id) REFERENCES stemmed_mapping(word_id)
        )
    """)

    conn.commit()

def populate_mapping(attribute, table):
    if table == 'page_mapping':
        cursor.execute(f"SELECT * FROM {table} WHERE url = ?", (attribute,))
        data = cursor.fetchone()
        if data is None:
            cursor.execute(f"INSERT INTO {table} (url) VALUES (?)", (attribute,))
            conn.commit()
    elif table in ['stemmed_mapping', 'non_stemmed_mapping']:
        cursor.execute(f"SELECT * FROM {table} WHERE word = ?", (attribute,))
        data = cursor.fetchone()
        if data is None:
            cursor.execute(f"INSERT INTO {table} (word) VALUES (?)", (attribute,))
            conn.commit()
    else:
        print("Invalid table name. Please choose from page_mapping, stemmed_mapping or non_stemmed_mapping.")

def populate_pageinfo(pages):
    for page_id, info in pages.items():
        cursor.execute("""
            INSERT INTO page_info (page_id, last_modified, title, body, child_links)
            VALUES (?, ?, ?, ?, ?)
        """, (page_id, info['last_modified'], info['title'], info['body'], ','.join(info['child_links'])))
    conn.commit()

def populate_forward_index(page_id, word_id, frequency, positions, table):
    if table in ['forwardIndex_body', 'forwardIndex_title']:
        cursor.execute(f"""
            INSERT INTO {table} (page_id, word_id, frequency, positions)
            VALUES (?, ?, ?, ?)
        """, (page_id, word_id, frequency, ','.join(map(str, positions))))
        conn.commit()
    else:
        print("Invalid table name. Please choose from forwardIndex_body or forwardIndex_title.")

def populate_inverted_index(word_id, page_body_freq, table):
    if table in ['invertedIndex_body', 'invertedIndex_title']:
        cursor.execute(f"""
            INSERT INTO {table} (word_id, pages_body_freq)
            VALUES (?, ?)
        """, (word_id, page_body_freq))
        conn.commit()
    else:
        print("Invalid table name. Please choose from invertedIndex_body or invertedIndex_title.")

create_tables()
conn.close()
