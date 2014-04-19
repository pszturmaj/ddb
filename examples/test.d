import std.stdio;
import ddb.postgres;

int main(string[] argv)
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
    
    cmd.query = "INSERT INTO MyTest (name, value) VALUES ('foo', 1);";
    assert(cmd.executeNonQuery == 1); // 1 row inserted

    cmd.query = "SELECT name, value FROM MyTest;";
    auto result = cmd.executeQuery;
    try
    {
        foreach (row; result)
        {
            // Access results using column name or column index
            assert(row["name"] == "foo");
            assert(row[0] == "foo");
            assert(row["value"] == 1);
            assert(row[1] == 1);
            writeln(row["name"], " = ", row[1]);
        }
    }
    finally
    {
        result.close;
    }

    cmd.query = "INSERT INTO MyTest (name, value) VALUES ('bar', 1);";
    assert(cmd.executeNonQuery == 1); // 1 row inserted

    cmd.query = "UPDATE MyTest SET value = 2 where value = 1";
    assert(cmd.executeNonQuery == 2); //  2 rows updated

    // reversing fields in SELECT means column indices change
    cmd.query = q"{SELECT value, name FROM MyTest
WHERE name = 'foo';}";;
    result = cmd.executeQuery;
    try
    {
        foreach (row; result)
        {
            assert(row["name"] == "foo");
            assert(row[0] == 2);
            assert(row["value"] == 2);
            assert(row[1] == "foo");
            writeln(row["name"], " = ", row["value"]);
        }
    }
    finally
    {
        result.close;
    }

    return 0;
}
