import polars as pl
pl.Config.set_fmt_str_lengths(150)

from maplib import Model
from maplib import explore
import parse_data as p
from utils import write_output
from utils import print_count

import time

out = p.parse_gedcom_to_polars("../data/data.ged")
df_persons = out["persons"]  # pick the persons DataFrame
df_family = out["families"]

m = Model()

with open("../ttl/tpl.ttl", "r") as file:
  tpl = file.read()

m.add_template(tpl)

m.map("urn:maplib_default:default_template_0", df_persons)
m.map("urn:maplib_default:default_template_1", df_family)

m.read("../ttl/ont.ttl")

print_count(m)
#write_output(m, "../ttl/out.ttl")


explore(m)
time.sleep(222)

