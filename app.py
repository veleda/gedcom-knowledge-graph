# app.py
import os
import tempfile
import shutil
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

import parse_data as p   
from maplib import Model   

# --------------------------
# CONFIG
# --------------------------
# Set this to your frontend origin while in production, e.g. "https://veleda.github.io"
FRONTEND_ORIGIN = "https://veleda.github.io"

app = FastAPI(title="GEDCOM to TTL + Graph API")

# CORS middleware must be added BEFORE route definitions
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN],  # during testing you can use ["*"]
    allow_methods=["*"],
    allow_headers=["*"],
)

TPL_FILE = "tpl.ttl" # OTTR template
ONT_FILE = "ont.ttl" # Ontology

def gedcom_to_graph_json(df_persons, df_families):
    nodes = []
    links = []
    # persons
    for row in df_persons.to_dicts():
        nodes.append({
            "id": row["subject_uri"],
            "label": row.get("NAME") or row.get("GIVN") or row["subject_uri"],
            "type": "person",
        })
    # families
    # Ensure families have a subject_uri; if not, create one from family_id
    for row in df_families.to_dicts():
        fam_uri = row.get("subject_uri") or f"urn:family:{row.get('family_id')}"
        nodes.append({
            "id": fam_uri,
            "label": row.get("family_id") or fam_uri,
            "type": "family",
        })
        # husband/wife -> family
        if row.get("husband_uri"):
            links.append({"source": row["husband_uri"], "target": fam_uri, "label": "husband"})
        if row.get("wife_uri"):
            links.append({"source": row["wife_uri"], "target": fam_uri, "label": "wife"})
        # children -> family
        for c in row.get("child_uris") or []:
            links.append({"source": c, "target": fam_uri, "label": "child"})
    return {"nodes": nodes, "links": links}


@app.get("/")
def root():
    return {"message": "GEDCOM Parser API running"}


@app.post("/parse")
async def parse_gedcom(file: UploadFile = File(...)):
    # save upload in a temp dir for processing
    tmp_dir = tempfile.mkdtemp(prefix="gedcom_")
    try:
        gedcom_path = os.path.join(tmp_dir, file.filename)
        with open(gedcom_path, "wb") as fh:
            fh.write(await file.read())

        ### Parse GEDCOM into Polars Data Frame
        out = p.parse_gedcom_to_polars(gedcom_path)
        df_persons = out["persons"]
        df_families = out["families"]

        ### Init maplib RDF Model
        m = Model()
        
        if not os.path.exists(TPL_FILE):
            raise HTTPException(status_code=500, detail=f"Missing template file: {TPL_FILE}")
        if not os.path.exists(ONT_FILE):
            raise HTTPException(status_code=500, detail=f"Missing ontology file: {ONT_FILE}")

        ### Add template
        with open(TPL_FILE, "r", encoding="utf-8") as tplfh:
            tpl = tplfh.read()
        m.add_template(tpl)

        ### Serialise data frames into RDF
        m.map("urn:maplib_default:default_template_0", df_persons)
        m.map("urn:maplib_default:default_template_1", df_families)
        m.read(ONT_FILE) # merge in ontology

        ttl_path = os.path.join(tmp_dir, "output.ttl")
        m.write(ttl_path)

        ### rdflib for pretty turtle. This is on the roadmap for maplib!
        from rdflib import Graph
        g = Graph()
        g.bind("gen", "http://gen.example.com/")
        g.parse(ttl_path, format="turtle")

        g.serialize(ttl_path, format="turtle")

        # read TTL content to return to frontend (no local file paths returned)
        with open(ttl_path, "r", encoding="utf-8") as tf:
            ttl_text = tf.read()

        # counts
        count_info = {"persons": df_persons.height, "families": df_families.height}

        # graph JSON for D3
        graph_json = gedcom_to_graph_json(df_persons, df_families)

        return JSONResponse({"count": count_info, "graph": graph_json, "ttl": ttl_text})

    except Exception as e:
        # provide error info (don't leak sensitive info in production)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # cleanup temporary folder
        try:
            shutil.rmtree(tmp_dir)
        except Exception:
            pass
