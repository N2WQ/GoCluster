import sqlite3
conn = sqlite3.connect('data/records/spots.db')
cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM spot_records")
total = cur.fetchone()[0]
fields = [
    ('report', 'report'),
    ('comment', 'comment'),
    ('source_type', 'source_type'),
    ('source_node', 'source_node'),
    ('dx_continent', 'dx_continent'),
    ('dx_country', 'dx_country'),
    ('dx_cq_zone', 'dx_cq_zone'),
    ('dx_itu_zone', 'dx_itu_zone'),
    ('dx_grid', 'dx_grid'),
    ('de_continent', 'de_continent'),
    ('de_country', 'de_country'),
    ('de_cq_zone', 'de_cq_zone'),
    ('de_itu_zone', 'de_itu_zone'),
    ('de_grid', 'de_grid')
]
print(f"Total spots: {total}")
for field, label in fields:
    cur.execute(f"SELECT COUNT(*) FROM spot_records WHERE {field} IS NULL OR {field} = ''")
    count = cur.fetchone()[0]
    print(f"Missing {label}: {count}")
conn.close()
