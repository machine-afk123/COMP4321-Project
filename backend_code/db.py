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
            page_size INTEGER,
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
            pages_freq TEXT,
            FOREIGN KEY(word_id) REFERENCES stemmed_mapping(word_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE invertedIndex_title (
            word_id INTEGER PRIMARY KEY,
            pages_freq TEXT,
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

def get_id(table, data, attribute):
    cursor.execute(f"SELECT rowid FROM {table} WHERE {attribute} = ?", (data,))
    data = cursor.fetchone()
    if data is None:
        cursor.execute(f"INSERT INTO {table} ({attribute}) VALUES (?)", (data,))
        conn.commit()
        cursor.execute(f"SELECT last_insert_rowid() FROM {table}")
        data = cursor.fetchone()
    return data[0]

def populate_pageinfo(pages):
    for url, info in pages.items():
        page_id = get_id('page_mapping', url, 'url')
        cursor.execute("""
            INSERT INTO page_info (page_id, page_size, last_modified, title, body, child_links)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (page_id, info['page_size'], info['last_modified'], info['title'], info['body'], ','.join(info['child_links'])))
    conn.commit()

def populate_forward_index(url, word, frequency, positions, table):
    if table in ['forwardIndex_body', 'forwardIndex_title']:
        page_id = get_id('page_mapping', url, 'url')
        word_id = get_id('stemmed_mapping', word, 'word')
        cursor.execute(f"""
            INSERT INTO {table} (page_id, word_id, frequency, positions)
            VALUES (?, ?, ?, ?)
        """, (page_id, word_id, frequency, ','.join(map(str, positions))))
        conn.commit()
    else:
        print("Invalid table name. Please choose from forwardIndex_body or forwardIndex_title.")

def populate_inverted_index(word, page_freq, table):
    if table in ['invertedIndex_body', 'invertedIndex_title']:
        word_id = get_id('stemmed_mapping', word, 'word')
        cursor.execute(f"""
            INSERT INTO {table} (word_id, pages_freq)
            VALUES (?, ?)
        """, (word_id, page_freq))
        conn.commit()
    else:
        print("Invalid table name. Please choose from invertedIndex_body or invertedIndex_title.")

# create_tables()
conn.close()
