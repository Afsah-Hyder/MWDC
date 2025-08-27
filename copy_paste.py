#!/usr/bin/env python3
"""
clone_mission.py

Clone all rows belonging to a given mission.id from a source MariaDB into a target DB,
generating new IDs (uuid4.hex) and remapping all intra-project foreign keys.

Usage:
    python clone_mission.py --mission-id <OLD_MISSION_ID>

Requirements:
    pip install pymysql pandas
"""

import argparse
import pymysql
import pymysql.cursors
import uuid
import datetime
from collections import defaultdict

# ---------- CONFIG ----------
# Edit these connection dicts for your environment.
SOURCE_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root123",
    "password": "admin",
    "database": "shipdb_pk",
    "cursorclass": pymysql.cursors.DictCursor,
    "charset": "utf8mb4",
    "autocommit": False,
}

# You can set TARGET_CONFIG identical to SOURCE_CONFIG to insert back into same DB (careful).
TARGET_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root123",
    "password": "admin",
    "database": "shipdb_pk",   # can be a different DB name
    "cursorclass": pymysql.cursors.DictCursor,
    "charset": "utf8mb4",
    "autocommit": False,
}

# Order of processing (unique, depth-preserving)
TABLE_ORDER = [
    "missions",
    "areas",
    "bottomidentification",
    "environmentaldata",
    "navigationmark",
    "operatornotes",
    "specialpoint",
    "tracks",
    "tasks",
    "areacells",
    "areapoints",
    "paths",
    "qroutes",
    "trackpoints",
    "taskexecutions",
    "underwaterobject",
]

# Which FK columns in each table refer to other tables within TABLE_ORDER
# format: { table: { fk_column: parent_table, ... }, ... }
FK_MAP = {
    "missions" : {},
    "areas": {"missions_id": "missions"},
    "bottomidentification": {"missions_id": "missions", "bottomtype_id": None, "bottomclutter_id": None},
    "environmentaldata": {"missions_id": "missions"},
    "navigationmark": {"missions_id": "missions", "marktype_id": None},
    "operatornotes": {"missions_id": "missions"},
    "specialpoint": {"missions_id": "missions"},
    "tracks": {"missions_id": "missions", "areas_id": "areas"},
    "tasks": {"missions_id": "missions", "tracks_id": "tracks", "specialpoint_id": "specialpoint"},
    "areacells": {"areas_id": "areas"},
    "areapoints": {"areas_id": "areas"},
    "paths": {"tracks_id": "tracks", "pathgenerator_id": None, "colors_id": None},
    "qroutes": {"tracks_id": "tracks", "qroutegenerator_id": None},
    "trackpoints": {"tracks_id": "tracks", "navigationmode_id": None},
    "taskexecutions": {"tasks_id": "tasks"},
    "underwaterobject": {"missions_id": "missions", "taskexecutions_id": "taskexecutions", 
                          # many technical lookup columns are left as-is
                          "contacttype_id": None, "minebodytype_id": None, "minetype_id": None,
                          "nonmilectype_id": None, "explosivetype_id": None, "materialtype_id": None,
                          "objectshapes_id": None, "techidenttype_id": None, "buriedmethod_id": None,
                          "colors_id": None},
    # missions has no parent fks
}

# Columns that contain zero-dates that must be converted to None
ZERO_DATE_STRS = {"0000-00-00 00:00:00", "0000-00-00"}

# safety flags
DRY_RUN = False   # if True: do everything except INSERTs (useful to preview)
COMMIT = True     # if False: rollback at the end (useful for testing)

# ---------- HELPERS ----------
def new_id():
    """Generate a new 32-char hex id (fits char(32) UUID usage)."""
    return uuid.uuid4().hex

def normalize_value(val):
    """Convert MySQL zero timestamps to None, keep others."""
    if isinstance(val, (str,)):
        if val in ZERO_DATE_STRS:
            return None
    return val

# ---------- CORE LOGIC ----------
def fetch_rows_for_table(src_conn, table, old_mission_id, mission_old_ids_by_table):
    with src_conn.cursor() as cur:
        rows = []

        # Case 1: Direct missions_id FK exists in FK_MAP
        if table == "missions":
            sql = f"SELECT * FROM `{table}` WHERE id = %s"
            cur.execute(sql, (old_mission_id,))
            rows = cur.fetchall()

        elif table in FK_MAP and "missions_id" in FK_MAP[table]:
            sql = f"SELECT * FROM `{table}` WHERE missions_id = %s"
            cur.execute(sql, (old_mission_id,))
            rows = cur.fetchall()

        # Case 2: Parent relationship exists (e.g., areas_id -> areas)
        if not rows:
            found_parent = None
            for fk_col, parent_tbl in FK_MAP.get(table, {}).items():
                if parent_tbl and parent_tbl in mission_old_ids_by_table and mission_old_ids_by_table[parent_tbl]:
                    found_parent = (fk_col, parent_tbl)
                    break

            if found_parent:
                fk_col, parent_tbl = found_parent
                ids = tuple(mission_old_ids_by_table[parent_tbl])
                if ids:
                    if len(ids) == 1:
                        cur.execute(f"SELECT * FROM `{table}` WHERE {fk_col} = %s", (ids[0],))
                    else:
                        placeholders = ",".join(["%s"] * len(ids))
                        cur.execute(f"SELECT * FROM `{table}` WHERE {fk_col} IN ({placeholders})", ids)
                    rows = cur.fetchall()

        # Case 3: Fallback — check if table has missions_id column
        if not rows:
            cur.execute("SHOW COLUMNS FROM `{}`".format(table))
            cols = [r["Field"] for r in cur.fetchall()]
            if "missions_id" in cols and "missions" in mission_old_ids_by_table:
                cur.execute(
                    f"SELECT * FROM `{table}` WHERE missions_id = %s",
                    (list(mission_old_ids_by_table["missions"])[0],),
                )
                rows = cur.fetchall()

        return rows

def insert_row_target(tgt_conn, table, row, id_map_for_table):
    """
    Insert a row dict into target DB for table.
    - Generates new id (uuid) and returns it.
    - Expects row to be a dict with column names -> values.
    - Does not remap FKs here (remapping done by calling code).
    """
    # copy the row to avoid mutating caller's copy
    r = row.copy()
    # remove old id; we'll set our own
    old_id = r.pop("id", None)
    # normalize zero dates
    for k, v in list(r.items()):
        r[k] = normalize_value(v)


    if table == "missions":
        with tgt_conn.cursor() as cur:
            cur.execute(f"Select id from `missions` where name = %s", (row["name"],))
            existing = cur.fetchone()
            if existing:
                r["name"] = r["name"] + "_copy"

    # Generate new id
    newid = new_id()
    r["id"] = newid

    # build columns and placeholders
    cols = list(r.keys())
    vals = list(r.values())
    placeholders = ", ".join(["%s"] * len(vals))
    cols_sql = ", ".join([f"`{c}`" for c in cols])
    
    sql = f"INSERT INTO `{table}` ({cols_sql}) VALUES ({placeholders})"
    with tgt_conn.cursor() as cur:
        cur.execute(sql, vals)

    return old_id, newid

def gather_and_insert(src_conn, tgt_conn, mission_old_id):
    """
    Main routine:
    - Walk TABLE_ORDER
    - For each table select rows that belong to mission_old_id (directly or via parent old ids)
    - Insert rows into target generating new IDs and remapping FKs using id_map
    """
    # mission_old_ids_by_table: holds sets of old IDs we selected for each table (used to find children)
    mission_old_ids_by_table = defaultdict(set)
    id_map = {t: {} for t in TABLE_ORDER}  # old_id -> new_id per table

    # initialize missions set
    mission_old_ids_by_table["missions"].add(mission_old_id)

    for table in TABLE_ORDER:
        print(f"\n=== Processing table: {table} ===")

        # fetch candidate rows from source
        rows = fetch_rows_for_table(src_conn, table, mission_old_id, mission_old_ids_by_table)
        print(f"Found {len(rows)} rows in source.{table} (relevant to mission).")

        # store the old ids we will process for children detection
        this_table_old_ids = []
        for row in rows:
            oldid = row.get("id")
            if oldid:
                this_table_old_ids.append(oldid)

        # track them
        for oid in this_table_old_ids:
            mission_old_ids_by_table[table].add(oid)

        # Insert rows into target, remapping parent FKs to new ids where applicable
        for row in rows:
            # Create a copy and remap FK columns that point to tables we are copying
            rcopy = dict(row)  # shallow copy
            # For each fk column in this table defined in FK_MAP, remap if parent in id_map
            for fk_col, parent_tbl in FK_MAP.get(table, {}).items():
                if parent_tbl and fk_col in rcopy:
                    old_fk = rcopy.get(fk_col)
                    if old_fk is None:
                        continue
                    # If we have a mapping for that parent old id -> new id, use it.
                    # If not yet mapped, leave as NULL (we may process in later step), but better to handle parents earlier in order.
                    mapped = id_map.get(parent_tbl, {}).get(old_fk)
                    if mapped:
                        rcopy[fk_col] = mapped
                    else:
                        # if parent is 'missions' and we're currently handling missions table, special-case:
                        if parent_tbl == "missions" and table == "missions":
                            # We'll set id below to a new uuid; do not overwrite rcopy['id'] here.
                            pass
                        else:
                            # leave as-is (old FK) for now; it might get remapped if inserted later with references.
                            # To be safe, set to None if parent id is not found: child shouldn't reference old parent id in target.
                            rcopy[fk_col] = None

            # convert zero-date strings to None
            for k in list(rcopy.keys()):
                if isinstance(rcopy[k], str) and rcopy[k] in ZERO_DATE_STRS:
                    rcopy[k] = None

            if DRY_RUN:
                print(f"DRY RUN - would insert into {table}: {rcopy}")
                # still record mapping of old->fake new id for remapping children
                fake_new = "dryrun_" + (rcopy.get("id") or uuid.uuid4().hex)
                old = row.get("id")
                if old:
                    id_map[table][old] = fake_new
                continue

            # insert into target
            oldid, newid = insert_row_target(tgt_conn, table, rcopy, id_map[table])
            # store mapping
            if oldid:
                id_map[table][oldid] = newid
            else:
                # if no old id (unlikely), store by generated newid -> itself
                id_map[table][newid] = newid

        # commit per-table to keep progress saved
        tgt_conn.commit()
        print(f"Inserted {len(rows)} rows into target.{table}. New IDs mapped for {len(id_map[table])} rows.")

    return id_map

# ---------- CLI ----------
def main():
    parser = argparse.ArgumentParser(description="Clone mission rows across schema with remapped IDs.")
    parser.add_argument("--mission-name", required=True, help="Mission name to clone (exact match)")
    parser.add_argument("--dry-run", action="store_true", help="Do not perform INSERTs, just simulate")
    parser.add_argument("--commit", action="store_true", help="Commit changes to target DB (default: commit). Use --no-commit to rollback.")
    parser.add_argument("--no-commit", action="store_true", help="Do not commit changes to target DB (rollback at end).")
    args = parser.parse_args()

    global DRY_RUN, COMMIT
    DRY_RUN = args.dry_run
    if args.no_commit:
        COMMIT = False
    else:
        COMMIT = True

    mission_name = args.mission_name

    print("Connecting to source DB...")
    src_conn = pymysql.connect(**SOURCE_CONFIG)
    print("Connecting to target DB...")
    tgt_conn = pymysql.connect(**TARGET_CONFIG)

    try:
        # sanity: verify mission exists in source by name
        with src_conn.cursor() as cur:
            cur.execute("SELECT id, name FROM missions WHERE name = %s", (mission_name,))
            mrow = cur.fetchone()
            if not mrow:
                print("Mission name not found in source DB:", mission_name)
                return

            old_mission_id = mrow["id"]
            print(f"Mission found: {mrow['name']} (id={old_mission_id})")

        # run gather & insert
        id_map = gather_and_insert(src_conn, tgt_conn, old_mission_id)

        if DRY_RUN:
            print("\nDRY RUN complete. No changes were written.")
        else:
            if COMMIT:
                tgt_conn.commit()
                print("\nAll changes committed to target DB.")
            else:
                tgt_conn.rollback()
                print("\nNO-COMMIT mode: rolled back changes in target DB.")

        # print mapping summary
        print("\nID mapping summary (sample):")
        for tbl, m in id_map.items():
            if not m:
                continue
            print(f"  {tbl}: {len(m)} rows mapped. Sample: {list(m.items())[:3]}")

    finally:
        src_conn.close()
        tgt_conn.close()



if __name__ == "__main__":
    main()
