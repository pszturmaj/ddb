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
    cmd.executeNonQuery;

    cmd.query = q"{SELECT name, value FROM MyTest
WHERE name = 'foo';}";
    auto result = cmd.executeQuery;
    try
    {
        foreach (row; result)
        {
            // Access results using column name or column index
            writeln(row["name"], " = ", row[1]);
        }
    }
    finally
    {
        result.close;
    }

    return 0;
}