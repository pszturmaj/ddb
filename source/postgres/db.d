module postgres.db;

// This file supports backwards compatibility only.
// Use `import ddb.postgres;` for new code

public import ddb.postgres;

import std.stdio;

shared static this() {
    stderr.writeln("WARNING: Module postgres.db deprecated. Replace with module ddb.postgres");
}
