# parse_gedcom_fully_fixed.py
import re
from typing import List
import polars as pl

# ----------------------------
# CONFIG
# ----------------------------
BASE_URI = "http://ged.example.com/"
BASE_URI_FAM = "http://ged.example.com/family/"
GEDCOM_PATH = "../data/data.ged"   # adjust to your file path

# ----------------------------
# READ GEDCOM LINES
# ----------------------------
LINE_RE = re.compile(
    r"^(?P<level>\d+)\s+(?:(?P<xref>@[^@]+@)\s+)?(?P<tag>[A-Z0-9_]+)(?:\s+(?P<value>.*))?$"
)

def read_gedcom_lines(path: str) -> pl.DataFrame:
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            line = raw.rstrip("\n\r")
            if not line:
                continue
            m = LINE_RE.match(line)
            if not m:
                continue
            rows.append({
                "lineno": line_no,
                "level": int(m.group("level")),
                "xref": m.group("xref"),
                "tag": m.group("tag"),
                "value": m.group("value"),
            })
    return pl.DataFrame(rows)

# ----------------------------
# MERGE CONC / CONT LINES
# ----------------------------
def merge_conc_cont(df: pl.DataFrame) -> pl.DataFrame:
    merged = []
    buffer = None
    for row in df.iter_rows(named=True):
        tag = row["tag"]
        val = row["value"]
        if tag in ("CONC", "CONT"):
            if buffer is not None:
                sep = "\n" if tag == "CONT" else ""
                buffer["value"] = (buffer["value"] or "") + sep + (val or "")
            continue
        if buffer is not None:
            merged.append(buffer)
        buffer = row.copy()
    if buffer is not None:
        merged.append(buffer)
    return pl.DataFrame(merged)

# ----------------------------
# ASSIGN PERSON / FAMILY IDs
# ----------------------------
def assign_block_ids(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        person_id = (pl.col("tag") == "INDI").cum_sum(),
        family_id = (pl.col("tag") == "FAM").cum_sum(),
    )

# ----------------------------
# FLATTEN HIERARCHY INTO KEY
# ----------------------------
def compute_flat_keys(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(parent_tag = pl.col("tag").shift(1))
    df = df.with_columns(
        key = (
            pl.when(pl.col("level") <= 1).then(pl.col("tag"))
              .when(pl.col("level") == 2).then(pl.col("parent_tag") + "_" + pl.col("tag"))
              .otherwise(pl.col("tag"))
        )
    )
    return df

# ----------------------------
# BUILD PERSONS TABLE
# ----------------------------
def build_persons_table(df: pl.DataFrame, base_uri: str) -> pl.DataFrame:
    person_rows = df.filter(pl.col("person_id") > 0)
    kv = person_rows.filter(pl.col("value").is_not_null())

    persons_wide = kv.pivot(
        index="person_id",
        columns="key",
        values="value",
        aggregate_function="first"
    ).sort("person_id")

    pointers = (
        person_rows
        .filter(pl.col("tag") == "INDI")
        .select(["person_id", "xref"])
        .with_columns(
            person_id_str = pl.col("xref").str.replace_all("@", ""),
            subject_uri = pl.lit(base_uri) + pl.col("xref").str.replace_all("@", "")
        )
    )

    persons = pointers.join(persons_wide, on="person_id", how="left")

    return persons

# ----------------------------
# BUILD FAMILIES TABLE
# ----------------------------
def build_families_table(df: pl.DataFrame, person_map: pl.DataFrame, base_uri: str) -> pl.DataFrame:
    family_rows = df.filter(pl.col("family_id") > 0)

    husbands = (
        family_rows.filter(pl.col("tag") == "HUSB")
        .select(["family_id", "value"])
        .group_by("family_id")
        .agg(pl.col("value").first().alias("HUSB_ptr"))
    )

    wives = (
        family_rows.filter(pl.col("tag") == "WIFE")
        .select(["family_id", "value"])
        .group_by("family_id")
        .agg(pl.col("value").first().alias("WIFE_ptr"))
    )

    children = (
        family_rows.filter(pl.col("tag") == "CHIL")
        .select(["family_id", "value"])
        .with_columns(child_id = pl.col("value").str.replace_all("@", ""))
        .group_by("family_id")
        .agg([pl.concat_list("child_id").alias("child_ids")])
    )

    fam_ids = (
        family_rows.filter(pl.col("tag") == "FAM")
        .select(["family_id"])
        .unique()
        .sort("family_id")
    )

    families = (
        fam_ids
        .join(husbands, on="family_id", how="left")
        .join(wives, on="family_id", how="left")
        .join(children, on="family_id", how="left")
    )

    families = families.with_columns(
        HUSB_id = pl.col("HUSB_ptr").str.replace_all("@", ""),
        WIFE_id = pl.col("WIFE_ptr").str.replace_all("@", "")
    )

    # lookup dictionary for subject URIs
    lookup_dict = {r["person_id_str"]: r["subject_uri"] for r in person_map.select(["person_id_str", "subject_uri"]).to_dicts()}

    # ----------------------------
    # MAP CHILDREN / HUSB / WIFE URIs USING PANDAS
    # ----------------------------
    families_pd = families.to_pandas()

    def child_map(lst: List[str]):
        if lst is None:
            return None
        # Convert numpy array to Python list if needed
        if hasattr(lst, "tolist"):
            lst = lst.tolist()
        result = []
        for cid in lst:
            # Handle numpy scalar arrays or None
            if hasattr(cid, "tolist"):
                cid = cid.tolist()
            if cid is not None:
                result.append(lookup_dict.get(str(cid), base_uri + str(cid)))
        return result if result else None

    families_pd["child_uris"] = families_pd["child_ids"].apply(child_map)
    families_pd["husband_uri"] = families_pd["HUSB_id"].apply(lambda cid: lookup_dict.get(cid))
    families_pd["wife_uri"]    = families_pd["WIFE_id"].apply(lambda cid: lookup_dict.get(cid))

    families = pl.from_pandas(families_pd)
    families = families.select(["family_id", "husband_uri", "wife_uri", "child_uris"])

    
    families_pd = families.to_pandas()

    families_pd["child_uris"] = families_pd["child_uris"].apply(
        lambda lst: [s.replace("[","").replace("]","").strip().replace("'", "").strip() for s in lst] 
                if (lst is not None and len(lst) > 0) else []
    )

    # Convert back to Polars
    families = pl.from_pandas(families_pd)

    families = families.with_columns(
        families["family_id"].cast(pl.Utf8).alias("family_id")
    )
    families = families.with_columns(
        (BASE_URI_FAM + pl.col("family_id")).alias("family_uri")
    )

    return families

# ----------------------------
# MAIN PARSING FUNCTION
# ----------------------------
def parse_gedcom_to_polars(path: str, base_uri: str = BASE_URI):
    raw = read_gedcom_lines(path)
    merged = merge_conc_cont(raw)
    with_ids = assign_block_ids(merged)
    keyed = compute_flat_keys(with_ids)

    persons = build_persons_table(keyed, base_uri)
    families = build_families_table(keyed, persons, base_uri)

    person_map = persons.select(["person_id", "person_id_str", "subject_uri"])

    return {
        "raw": raw,
        "merged": merged,
        "keyed": keyed,
        "persons": persons,
        "families": families,
        "person_map": person_map,
    }

