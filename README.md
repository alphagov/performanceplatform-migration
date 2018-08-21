# performanceplatform-migration

We are moving the performance platform from Compose.io MongoDB to Postgres.

Here are some scripts we are using to do it

---

## backdrop-staging-mongo-to-postgres

Staging has a mongodb running on the PaaS using Compose.io, this spins up 5
instances of a worker which writes from Mongo to Postgres.

The Mongo "schema" was: one collection per dataset, with the `_id` field as
"primary key".

This script creates in Postgres, a table called `mongo` with the following
schema:
- `id`: primary key, string, format `$collection:$_id`
- `collection`: string, format `$collection`
- `record`: json, the entire mongo record
