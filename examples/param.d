import std.stdio;
import ddb.postgres;

struct KeyValue {
    string key;
    int val;
}

void main(string[] argv)
{
    // Point this to a valid test database before compiling
    auto conn = new PGConnection([
        "host" : "localhost",
        "database" : "test",
        "user" : "test",
        "password" : "test"
    ]);

    scope(exit) conn.close;

    auto cmd = new PGCommand(conn, "DROP TABLE MyTest;");
    try {
        cmd.executeNonQuery;
    }
    catch (ServerErrorException e) {
        // Probably table does not exist - ignore
    }

    // Re-use PGCommand object by reassigning to cmd.query
    // using strings, multi-line strings, or string imports
    cmd.query = import("test-create.sql");
    cmd.executeNonQuery;
    
    cmd.query = "INSERT INTO MyTest (name, value) VALUES ($1, $2);";
    cmd.parameters.add(1, PGType.TEXT).value = "foo";
    cmd.parameters.add(2, PGType.INT4).value = 1;
    assert(cmd.executeNonQuery() == 1); // 1 row inserted

    cmd.query = "SELECT name, value FROM MyTest WHERE name = $1;";
    cmd.parameters.add(1, PGType.TEXT).value = "foo";

    // When there is only one result (e.g. when querying using a primary or
    // unique key), use executeRow to save having to loop and close result.
    auto row = cmd.executeRow();

    assert(row["name"] == "foo");
    assert(row["value"] == 1);

    // Use a template parameter to store the result in a struct.
    auto kv = cmd.executeRow!KeyValue();

    assert(kv.key == "foo");
    assert(kv.val == 1);
}
