#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json, os, re, shutil, sqlite3, sys, requests

MAX_PERFORMERS = 3

def sanitize_filename_linux(name: str) -> str:
    s = name.replace("/", "-").strip().strip(". ")
    s = re.sub(r"\s+", " ", s)
    return s

def build_new_basename(scene) -> str:
    performers = [p["name"].strip() for p in (scene.get("performers") or []) if p.get("name")]
    performers_str = ", ".join(performers[:MAX_PERFORMERS]) if performers else "Unknown"
    date = scene.get("date") or ""
    year = date[:4] if len(date) >= 4 else "0000"
    title = scene.get("title") or "Unknown"
    studio = (scene.get("studio") or {}).get("name") or "Unknown"
    base = f"{performers_str} - {year} - {title} [{studio}]"
    return sanitize_filename_linux(base)

def gql_endpoint(server):
    scheme = server.get("Scheme", "http")
    host = server.get("Host", "localhost")
    if host == "0.0.0.0":
        host = "localhost"
    port = str(server.get("Port", 9999))
    return f"{scheme}://{host}:{port}/graphql"

def gql_headers():
    return {"Content-Type": "application/json", "Accept": "application/json"}

def gql_cookies(server):
    sess = server.get("SessionCookie", {}).get("Value", "")
    return {"session": sess} if sess else {}

def callGraphQL(server, query, variables=None):
    res = requests.post(
        gql_endpoint(server),
        json={"query": query, "variables": variables or {}},
        headers=gql_headers(),
        cookies=gql_cookies(server),
        timeout=60,
    )
    res.raise_for_status()
    data = res.json()
    if "errors" in data:
        raise RuntimeError(data["errors"])
    return data.get("data", {})

def graphql_get_build(server):
    q = """{ systemStatus { databaseSchema } }"""
    d = callGraphQL(server, q)
    return int(d["systemStatus"]["databaseSchema"])

def graphql_get_config(server):
    q = """query { configuration { general { databasePath } } }"""
    d = callGraphQL(server, q)
    return d["configuration"]

def graphql_find_scenes(server, per_page=1000):
    q = """
    query FindScenes($filter: FindFilterType) {
      findScenes(filter: $filter) {
        scenes {
          id
          title
          date
          organized
          studio { name }
          performers { name }
          files { path }
        }
      }
    }
    """
    variables = {"filter": {"page": 1, "per_page": per_page}}
    d = callGraphQL(server, q, variables)
    return d.get("findScenes", {})

def connect_db(path):
    return sqlite3.connect(path, timeout=10)

def db_rename_refactor(stash_db, scene_id, new_dir, new_basename):
    cur = stash_db.cursor()
    cur.execute("SELECT file_id FROM scenes_files WHERE scene_id=?", [scene_id])
    file_ids = cur.fetchall()
    if not file_ids:
        raise RuntimeError("No file_id linked to scene")
    target_file_id = file_ids[0][0]
    cur.execute("SELECT id FROM folders WHERE path=?", [new_dir])
    r = cur.fetchall()
    if not r:
        raise RuntimeError(f"Folder not found in DB: {new_dir}")
    new_folder_id = r[0][0]
    cur.execute(
        "UPDATE files SET basename=?, parent_folder_id=? WHERE id=?;",
        [new_basename, new_folder_id, target_file_id],
    )
    stash_db.commit()
    cur.close()

def db_rename_legacy(stash_db, scene_id, new_full_path):
    cur = stash_db.cursor()
    cur.execute("UPDATE scenes SET path=? WHERE id=?;", [new_full_path, scene_id])
    stash_db.commit()
    cur.close()

def process_scene(scene, stash_db, db_version, dry_run):
    sid = scene["id"]
    if not bool(scene.get("organized")):
        return 0, 0, 0

    files = scene.get("files") or []
    renamed = missing = errors = 0

    for f in files:
        src_path = f["path"]
        if not os.path.exists(src_path):
            missing += 1
            continue

        ext = os.path.splitext(src_path)[1] or ".mp4"
        new_basename = build_new_basename(scene) + ext
        new_dir = os.path.dirname(src_path)
        dst_path = os.path.join(new_dir, new_basename)

        if os.path.normpath(dst_path) == os.path.normpath(src_path):
            continue

        if dry_run:
            print(f"[DRY RUN] {src_path} -> {dst_path}")
            renamed += 1
            continue

        try:
            shutil.move(src_path, dst_path)
            if db_version >= 32:
                db_rename_refactor(stash_db, int(sid), new_dir, os.path.basename(dst_path))
            else:
                db_rename_legacy(stash_db, int(sid), dst_path)
            renamed += 1
        except Exception as e:
            print(f"[ERROR] {sid}: {e}")
            errors += 1

    return renamed, missing, errors

def main():
    fragment = json.loads(sys.stdin.read())
    server = fragment.get("server_connection", {})
    args = fragment.get("args", {}) or {}
    dry_run = bool(args.get("dry_run", True))

    db_version = graphql_get_build(server)
    stash_db_path = graphql_get_config(server)["general"]["databasePath"]
    stash_db = connect_db(stash_db_path)

    payload = graphql_find_scenes(server, per_page=1000)
    scenes = payload.get("scenes", []) or []

    total_r = total_m = total_e = 0
    for scene in scenes:
        r, m, e = process_scene(scene, stash_db, db_version, dry_run)
        total_r += r
        total_m += m
        total_e += e

    print(json.dumps({
        "output": f"Renamed: {total_r} | Missing: {total_m} | Errors: {total_e}",
        "error": None
    }))

if __name__ == "__main__":
    main()