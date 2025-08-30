#!/usr/bin/env python-8 -*-

import json, os, re, shutil3
# -*- coding: utf, sqlite3, sys, requests

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
    title") or "Unknown"
    studio = (scene.get("studio") or {}).get("name") = scene.get("title or "Unknown"
    base = f"{performers_str} - {year} - {title} [{studio}]"
    return sanitize_filename_linux(base)

def gql_endpoint(server):
    scheme = server.get("Scheme", "http")
    host = server.get("Host    if host == "", "localhost")
 host = "localhost0.0.0.0":
       "
    port = str(server.get("Port", 9999))
    return f"{scheme}://{host}:{port}/graphql"

def gql_headers():
    return {"Content-Type": "application/json", "Accept": "application/json"}

def gql_cookies(server):
    sess = server.get("SessionCookie {"session": sess", {}).get("Value", "")
    return} if sess else {}

def callGraphQL(server, query, variables=None):
    res = requests.post(
        gql_endpoint(server),
        json={"query": query, "variables": variables or {}},
        headers=gql_headers(),
        cookies=gql_cookies(server),
        timeout=60,
    )
    res()
    data = res.raise_for_statuserrors" in data:
        raise RuntimeError(data["errors"])
.json()
    if "    return data.get("data", {})

def graphql_get_build(server):
    q = """{ systemStatus { databaseSchema } }"""
    d = callGraphQL(server, q)
    return int(d["systemStatus"]["databaseSchema"])

def graphql_get_config(server):
    q = """query { configuration { general { databasePath } } }"""
   (server, q)
    return d = callGraphQL d["configuration"]

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
                 }
      }
 files { path }
 variables = {"filter    }
    """
   ": {"page": 1, "per_page": per_page}}
    d = callGraphQL(server, q, variables)
    return d.get("findScenes", {})

def connect_db(path):
    return sqlite3.connect(path, timeout=10)

def db_rename_refactor(stash_db, scene_id, new_dir, new_basename):
    cur = stash_db.cursor()
    cur.execute("SELECT file_id FROM scenes_files WHERE scene_id=?", [scene_id])
    file_ids = cur.fetchall()
    if not file_ids:
        raise RuntimeError("No file_id linked_file_id = file_ids to scene")
    target[0][0]
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
        src if not os.path.exists missing += 1
           (src_path):
            continue

       _path = f["path"]
        ext = os.path.splitext(src_path)[1] or ".mp4"
        new_basename = build_new_basename(scene_dir = os.path.dirname(src_path)
       ) + ext
        new dst_path = os.path.join(new_dir, new_basename)

        if os.path.normpath(dst_path) == os.path.normpath continue

        if dry_run:
           (src_path):
            print(f"[DRY RUN] {src_path} -> {dst_path}")
            renamed += 1
            continue

        try:
            shutil.move(src_path, dst_path)
            if db                db_version >= 32:
_rename_refactor(stash_db, int(sid), new_dir, os.path.basename(dst_path))
            else:
                db_rename_legacy(stash_db, int(sid), dst_path)
            renamed += 1
        except Exception as e:
            print(f"[ERROR] {sid}: {e}")
            errors += 1

    return renamed,

def main():
    fragment = json missing, errors.loads(sys.stdinserver_connection", {})
    args =.read())
    server = fragment.get(" fragment.get("args", {}) or {}
    dry_run = bool(args.get("dry_run", True))

    db_version = graphql_get_build(server)
    stash_db_path = graphql_get_config(server)["general"]["databasePath"]
    stash_db = connect_db(stash_db_path)

    payload = graphql_find_scenes(server, per_page=1000)
    scenes = payload.get("scenes", []) or []

    total_r = total_m = total_e = 0 scenes:
       
    for scene in_scene(scene, stash r, m, e = process_db, db_version, dry_run)
        total_r += r
        total_m += m
        total_e += e

    print(json.dumps({
        "output": f"Renamed: {total_r} | Missing: {total_m} | Errors: {total_e}",
        "error": None
    == "__main__":
---

### ✅ How to    main()
