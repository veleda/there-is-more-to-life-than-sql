import polars as pl
pl.Config.set_fmt_str_lengths(150)

from maplib import Mapping

import parse_data as p


# Read OTTR template and init mapping
with open('tpl/tpl.ttl', 'r') as file:
    content = file.read()

m = Mapping(content)

# Expand mappings -- mapping instance data according to OTTR templates into RDF
m.expand("http://data.eksempel.no/tpl/Vannkraftverk", p.parse_nve())
m.expand("http://data.eksempel.no/tpl/Inntekt", p.parse_ssb())

# Merge in the ontology
m.read_triples('ttl/ont.ttl')

# Write to file, but clear the output file first
with open("ttl/out.ttl", "w") as file:
    pass
m.write_triples("ttl/out.ttl", format="turtle")