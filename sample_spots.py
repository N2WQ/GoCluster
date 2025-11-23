import sqlite3, json
conn = sqlite3.connect('data/records/spots.db')
cur = conn.cursor()
modes = [row[0] for row in cur.execute("SELECT DISTINCT mode FROM spot_records ORDER BY mode").fetchall()]
for mode in modes:
    row = cur.execute("SELECT id, mode, dx_call, de_call, frequency, dx_continent, dx_cq_zone, dx_grid FROM spot_records WHERE mode=? ORDER BY id DESC LIMIT 1", (mode,)).fetchone()
    if row:
        print(json.dumps({"mode": mode, "id": row[0], "dx_call": row[2], "de_call": row[3], "freq": row[4], "dx_meta": f"{row[5]} CQ {row[6]} {row[7]}"}))
conn.close()
