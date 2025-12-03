from fastapi import FastAPI, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import parse_data as p
import polars as pl
from maplib import Model
import tempfile
import os

pl.Config.set_fmt_str_lengths(150)

app = FastAPI(title="GEDCOM to TTL + Graph API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://veleda.github.io"],  # allow all origins for simplicity
    allow_methods=["*"],
    allow_headers=["*"]
)

TPL_FILE = "../ttl/tpl.ttl"
ONT_FILE = "../ttl/ont.ttl"

@app.get("/")
def read_root():
    return {"message": "GEDCOM Parser API running"}

def gedcom_to_graph_json(df_persons, df_families):
    nodes = []
    links = []

    # Persons
    for row in df_persons.to_dicts():
        nodes.append({
            "id": row["subject_uri"],
            "label": row.get("NAME", row["subject_uri"]),
            "type": "person"
        })

    # Families
    for row in df_families.to_dicts():
        fam_uri = row["family_uri"]
        nodes.append({"id": fam_uri, "label": fam_uri, "type": "family"})

        if row.get("husband_uri"):
            links.append({"source": row["husband_uri"], "target": fam_uri, "label": "husband"})
        if row.get("wife_uri"):
            links.append({"source": row["wife_uri"], "target": fam_uri, "label": "wife"})
        for c in row.get("child_uris", []):
            links.append({"source": c, "target": fam_uri, "label": "child"})

    return {"nodes": nodes, "links": links}

@app.post("/parse")
async def parse_gedcom(file: UploadFile = File(...)):
    tmp_dir = tempfile.mkdtemp()
    gedcom_path = os.path.join(tmp_dir, file.filename)
    ttl_path = os.path.join(tmp_dir, "output.ttl")

    with open(gedcom_path, "wb") as f:
        f.write(file.file.read())

    try:
        # Parse GEDCOM
        out = p.parse_gedcom_to_polars(gedcom_path)
        df_persons = out["persons"]
        df_family = out["families"]

        # Maplib for TTL
        m = Model()
        with open(TPL_FILE, "r") as tpl_file:
            tpl = tpl_file.read()
        m.add_template(tpl)
        m.map("urn:maplib_default:default_template_0", df_persons)
        m.map("urn:maplib_default:default_template_1", df_family)
        m.read(ONT_FILE)
        m.write(ttl_path)

        count_info = {
            "persons": df_persons.height,
            "families": df_family.height
        }

        graph_json = gedcom_to_graph_json(df_persons, df_family)

        return JSONResponse({
            "count": count_info,
            "graph": graph_json,
            "ttl_file": ttl_path
        })

    finally:
        pass  # keep files for download

@app.get("/download")
def download_ttl(file_path: str):
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="text/turtle", filename="output.ttl")
    return JSONResponse({"error": "File not found"}, status_code=404)
