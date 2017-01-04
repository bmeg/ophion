#!/usr/bin/env python

import sys
import json
import ophion

con = ophion.Ophion("http://10.96.11.89")
gene = sys.argv[1]
q = con.query("gene").has("gid", ["gene:" + gene]).incoming("inGene").outgoing("effectOf").outgoing("tumorSample").outgoing("sampleOf").values(["tumorSite"])
#q = con.query("drug")
print json.dumps(con.execute(q), indent=4)