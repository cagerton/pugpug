Pugpug
======

Database schema migration helper for Postgres databases.

Pugpug is uses sha1s of the schema of each table to keep track of versions.
Pugpug keeps state in the ./pugpug/ directory along with all migration sql
and snapshots at each step.

requires:
 python3.3
 PyYaml
 docopt
 slugify
