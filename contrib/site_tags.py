#!/usr/bin/env python

"""
Try to classify or tag endpoints into categories:

* institutional repository (IR)
* educational domain
* an OJS installation with 2+ sites "index.php"

"""

import fileinput
import re
from pydantic import BaseModel
from typing import Optional

class Site(BaseModel):
    url: str
    is_edu = False
    is_edu_world = False
    is_dspace = False
    is_ojs = False
    is_id = False
    is_museum = False


for line in fileinput.input():
    line = line.strip()
    if not line:
        continue
    site = Site(url=line)
    if ".edu/" in line:
        site.is_edu = True
    if ".ac." in line:
        site.is_edu = True
    if "uni-" in line:
        site.is_edu = True
    if "univ-" in line:
        site.is_edu = True
    if re.match(".*uni.*.it.*", line):
        site.is_edu = True
    if re.match(".*edu.([a-z]{2,3}).*", line):
        site.is_edu = True
        site.is_edu_world = True
    if re.match(".*dspace.*", line):
        site.is_dspace = True
    if re.match(".*/index.php/[^/]*/oai", line):
        site.is_ojs = True
    if "/ojs/" in line:
        site.is_ojs = True
    if ".id/" in line:
        site.is_id = True
    if "museum" in line:
        site.is_museum = True

    print(site.json())
