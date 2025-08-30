import polars as pl
pl.Config.set_fmt_str_lengths(150)

from maplib import Mapping

import parse_data as p

count = " SELECT (COUNT(?s) AS ?triples) WHERE { ?s ?p ?o . } "
# print(m.query(count))

# Read OTTR template and init mapping
with open ("tpl/tpl.ttl", "r") as file:
    tpl = file.read()

m = Mapping(tpl)

# Expand mappings -- serialize instance data according to OTTR templates into RDF
m.expand(p.nve_tpl, p.parse_nve())
m.expand(p.ssb_tpl, p.parse_ssb())

# Merge in the ontology
m.read_triples("ttl/ont.ttl")

# Write to file, but clear the output file first
with open("ttl/out.ttl", "w") as file:
    pass

m.write_triples("ttl/out.ttl", format="turtle")
