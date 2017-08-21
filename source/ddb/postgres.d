/**
PostgreSQL client implementation.

Features:
$(UL
    $(LI Standalone (does not depend on libpq))
    $(LI Binary formatting (avoids parsing overhead))
    $(LI Prepared statements)
    $(LI Parametrized queries (partially working))
    $(LI $(LINK2 http://www.postgresql.org/docs/9.0/static/datatype-enum.html, Enums))
    $(LI $(LINK2 http://www.postgresql.org/docs/9.0/static/arrays.html, Arrays))
    $(LI $(LINK2 http://www.postgresql.org/docs/9.0/static/rowtypes.html, Composite types))
)

TODOs:
$(UL
    $(LI Redesign parametrized queries)
    $(LI BigInt/Numeric types support)
    $(LI Geometric types support)
    $(LI Network types support)
    $(LI Bit string types support)
    $(LI UUID type support)
    $(LI XML types support)
    $(LI Transaction support)
    $(LI Asynchronous notifications)
    $(LI Better memory management)
    $(LI More friendly PGFields)
)

Bugs:
$(UL
    $(LI Support only cleartext and MD5 $(LINK2 http://www.postgresql.org/docs/9.0/static/auth-methods.html, authentication))
    $(LI Unfinished parameter handling)
    $(LI interval is converted to Duration, which does not support months)
)

$(B Data type mapping:)

$(TABLE
    $(TR $(TH PostgreSQL type) $(TH Aliases) $(TH Default D type) $(TH D type mapping possibilities))
    $(TR $(TD smallint) $(TD int2) $(TD short) <td rowspan="19">Any type convertible from default D type</td>)
    $(TR $(TD integer) $(TD int4) $(TD int))
    $(TR $(TD bigint) $(TD int8) $(TD long))
    $(TR $(TD oid) $(TD reg***) $(TD uint))
    $(TR $(TD decimal) $(TD numeric) $(TD not yet supported))
    $(TR $(TD real) $(TD float4) $(TD float))
    $(TR $(TD double precision) $(TD float8) $(TD double))
    $(TR $(TD character varying(n)) $(TD varchar(n)) $(TD string))
    $(TR $(TD character(n)) $(TD char(n)) $(TD string))
    $(TR $(TD text) $(TD) $(TD string))
    $(TR $(TD "char") $(TD) $(TD char))
    $(TR $(TD bytea) $(TD) $(TD ubyte[]))
    $(TR $(TD timestamp without time zone) $(TD timestamp) $(TD DateTime))
    $(TR $(TD timestamp with time zone) $(TD timestamptz) $(TD SysTime))
    $(TR $(TD date) $(TD) $(TD Date))
    $(TR $(TD time without time zone) $(TD time) $(TD TimeOfDay))
    $(TR $(TD time with time zone) $(TD timetz) $(TD SysTime))
    $(TR $(TD interval) $(TD) $(TD Duration (without months and years)))
    $(TR $(TD boolean) $(TD bool) $(TD bool))
    $(TR $(TD enums) $(TD) $(TD string) $(TD enum))
    $(TR $(TD arrays) $(TD) $(TD Variant[]) $(TD dynamic/static array with compatible element type))
    $(TR $(TD composites) $(TD record, row) $(TD Variant[]) $(TD dynamic/static array, struct or Tuple))
)

Examples:
with vibe.d use -version=Have_vibe_d_core and use a ConnectionPool (PostgresDB Object & lockConnection)
---

	auto pdb = new PostgresDB([
		"host" : "192.168.2.50",
		"database" : "postgres",
		"user" : "postgres",
		"password" : ""
	]);
	auto conn = pdb.lockConnection();

	auto cmd = new PGCommand(conn, "SELECT typname, typlen FROM pg_type");
	auto result = cmd.executeQuery;

	try
	{
		foreach (row; result)
		{
			writeln(row["typname"], ", ", row[1]);
		}
	}
	finally
	{
		result.close;
	}

---
without vibe.d you can use std sockets with PGConnection object

---
import std.stdio;
import ddb.postgres;

int main(string[] argv)
{
    auto conn = new PGConnection([
        "host" : "localhost",
        "database" : "test",
        "user" : "postgres",
        "password" : "postgres"
    ]);

    scope(exit) conn.close;

    auto cmd = new PGCommand(conn, "SELECT typname, typlen FROM pg_type");
    auto result = cmd.executeQuery;

    try
    {
        foreach (row; result)
        {
            writeln(row[0], ", ", row[1]);
        }
    }
    finally
    {
        result.close;
    }

    return 0;
}
---

Copyright: Copyright Piotr Szturmaj 2011-.
License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
Authors: Piotr Szturmaj
*//*
Documentation contains portions copied from PostgreSQL manual (mainly field information and
connection parameters description). License:

Portions Copyright (c) 1996-2010, The PostgreSQL Global Development Group
Portions Copyright (c) 1994, The Regents of the University of California

Permission to use, copy, modify, and distribute this software and its documentation for any purpose,
without fee, and without a written agreement is hereby granted, provided that the above copyright
notice and this paragraph and the following two paragraphs appear in all copies.

IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR DIRECT,
INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS,
ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE UNIVERSITY
OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE UNIVERSITY OF
CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS,
OR MODIFICATIONS.
*/
module ddb.postgres;

version (Have_vibe_d_core)
{
    import vibe.core.net;
    import vibe.core.stream;
}
else
{
    import std.socket;
}
import std.bitmanip;
import std.exception;
import std.conv;
import std.traits;
import std.typecons;
import std.string;
import std.digest.md;
import core.bitop;
import std.variant;
import std.algorithm;
import std.stdio;
import std.datetime;
import std.uuid;
public import ddb.db;

private:

const PGEpochDate = Date(2000, 1, 1);
const PGEpochDay = PGEpochDate.dayOfGregorianCal;
const PGEpochTime = TimeOfDay(0, 0, 0);
const PGEpochDateTime = DateTime(2000, 1, 1, 0, 0, 0);

class PGStream
{
    version (Have_vibe_d_core)
    {
        private TCPConnectionWrapper m_socket;

        @property TCPConnectionWrapper socket()
        {
            return m_socket;
        }

        this(TCPConnectionWrapper socket)
        {
            m_socket = socket;
        }
    }
    else
    {
        private Socket m_socket;

        @property Socket socket()
        {
            return m_socket;
        }

        this(Socket socket)
        {
            m_socket = socket;
        }
    }

    protected void read(ubyte[] buffer)
    {
        version(Have_vibe_d_core)
        {
            m_socket.read(buffer);
        }
        else
        {
            if (buffer.length > 0)
            {
                m_socket.receive(buffer);
            }
        }
    }

    void write(ubyte[] x)
    {
        version(Have_vibe_d_core)
        {
            m_socket.write(x);
        }
        else
        {
            if (x.length > 0)
            {
                m_socket.send(x);
            }
        }
    }

	void write(ubyte x)
	{
		write(nativeToBigEndian(x)); // ubyte[]
	}

    void write(short x)
	{
		write(nativeToBigEndian(x)); // ubyte[]
	}

    void write(int x)
	{
		write(nativeToBigEndian(x)); // ubyte[]
	}

    void write(long x)
    {
		write(nativeToBigEndian(x));
	}

    void write(float x)
    {
		write(nativeToBigEndian(x)); // ubyte[]
    }

    void write(double x)
    {
		write(nativeToBigEndian(x));
	}

    void writeString(string x)
    {
        ubyte[] ub = cast(ubyte[])(x);
        write(ub);
    }

    void writeCString(string x)
    {
        writeString(x);
        write('\0');
    }

    void writeCString(char[] x)
    {
        write(cast(ubyte[])x);
        write('\0');
    }

    void write(const ref Date x)
    {
        write(cast(int)(x.dayOfGregorianCal - PGEpochDay));
    }

    void write(Date x)
    {
        write(cast(int)(x.dayOfGregorianCal - PGEpochDay));
    }

    void write(const ref TimeOfDay x)
	{
		write(cast(int)((x - PGEpochTime).total!"usecs"));
    }

    void write(const ref DateTime x) // timestamp
	{
		write(cast(int)((x - PGEpochDateTime).total!"usecs"));
    }

    void write(DateTime x) // timestamp
	{
		write(cast(int)((x - PGEpochDateTime).total!"usecs"));
    }

    void write(const ref SysTime x) // timestamptz
	{
		write(cast(int)((x - SysTime(PGEpochDateTime, UTC())).total!"usecs"));
    }

    // BUG: Does not support months
    void write(const ref core.time.Duration x) // interval
	{
		int months = cast(int)(x.split!"weeks".weeks/28);
		int days = cast(int)x.split!"days".days;
        long usecs = x.total!"usecs" - convert!("days", "usecs")(days);

        write(usecs);
        write(days);
		write(months);
	}

    void writeTimeTz(const ref SysTime x) // timetz
	{
		TimeOfDay t = cast(TimeOfDay)x;
        write(t);
		write(cast(int)0);
	}
}

char[32] MD5toHex(T...)(in T data)
{
    return md5Of(data).toHexString!(LetterCase.lower);
}

struct Message
{
    PGConnection conn;
    char type;
    ubyte[] data;

    private size_t position = 0;

    T read(T, Params...)(Params p)
    {
        T value;
        read(value, p);
        return value;
    }

    void read()(out char x)
    {
        x = data[position++];
    }


    void read(Int)(out Int x) if((isIntegral!Int || isFloatingPoint!Int) && Int.sizeof > 1)
    {
        ubyte[Int.sizeof] buf;
        buf[] = data[position..position+Int.sizeof];
        x = bigEndianToNative!Int(buf);
        position += Int.sizeof;
    }

    string readCString()
    {
        string x;
        readCString(x);
        return x;
    }

    void readCString(out string x)
    {
        ubyte* p = data.ptr + position;

        while (*p > 0)
            p++;
		x = cast(string)data[position .. cast(size_t)(p - data.ptr)];
        position = cast(size_t)(p - data.ptr + 1);
    }

    string readString(int len)
    {
        string x;
        readString(x, len);
        return x;
    }

    void readString(out string x, int len)
	{
		x = cast(string)(data[position .. position + len]);
		position += len;
	}

    void read()(out bool x)
    {
        x = cast(bool)data[position++];
    }

    void read()(out ubyte[] x, int len)
    {
        enforce(position + len <= data.length);
        x = data[position .. position + len];
        position += len;
    }

    void read()(out UUID u) // uuid
    {
        ubyte[16] uuidData = data[position .. position + 16];
        position += 16;
        u = UUID(uuidData);
    }

    void read()(out Date x) // date
    {
        int days = read!int; // number of days since 1 Jan 2000
        x = PGEpochDate + dur!"days"(days);
    }

    void read()(out TimeOfDay x) // time
    {
        long usecs = read!long;
        x = PGEpochTime + dur!"usecs"(usecs);
    }

    void read()(out DateTime x) // timestamp
    {
        long usecs = read!long;
        x = PGEpochDateTime + dur!"usecs"(usecs);
    }

    void read()(out SysTime x) // timestamptz
    {
        long usecs = read!long;
        x = SysTime(PGEpochDateTime + dur!"usecs"(usecs), UTC());
        x.timezone = LocalTime();
    }

    // BUG: Does not support months
    void read()(out core.time.Duration x) // interval
    {
        long usecs = read!long;
        int days = read!int;
        int months = read!int;

        x = dur!"days"(days) + dur!"usecs"(usecs);
    }

    SysTime readTimeTz() // timetz
    {
        TimeOfDay time = read!TimeOfDay;
        int zone = read!int / 60; // originally in seconds, convert it to minutes
        Duration duration = dur!"minutes"(zone);
        auto stz = new immutable SimpleTimeZone(duration);
        return SysTime(DateTime(Date(0, 1, 1), time), stz);
    }

    T readComposite(T)()
    {
        alias DBRow!T Record;

        static if (Record.hasStaticLength)
        {
            alias Record.fieldTypes fieldTypes;

            static string genFieldAssigns() // CTFE
            {
                string s = "";

                foreach (i; 0 .. fieldTypes.length)
                {
                    s ~= "read(fieldOid);\n";
                    s ~= "read(fieldLen);\n";
                    s ~= "if (fieldLen == -1)\n";
                    s ~= text("record.setNull!(", i, ");\n");
                    s ~= "else\n";
                    s ~= text("record.set!(fieldTypes[", i, "], ", i, ")(",
                              "readBaseType!(fieldTypes[", i, "])(fieldOid, fieldLen)",
                              ");\n");
                    // text() doesn't work with -inline option, CTFE bug
                }

                return s;
            }
        }

        Record record;

        int fieldCount, fieldLen;
        uint fieldOid;

        read(fieldCount);

        static if (Record.hasStaticLength)
            mixin(genFieldAssigns);
        else
        {
            record.setLength(fieldCount);

            foreach (i; 0 .. fieldCount)
            {
                read(fieldOid);
                read(fieldLen);

                if (fieldLen == -1)
                    record.setNull(i);
                else
                    record[i] = readBaseType!(Record.ElemType)(fieldOid, fieldLen);
            }
        }

        return record.base;
    }
	mixin template elmnt(U : U[])
	{
		alias U ElemType;
	}
    private AT readDimension(AT)(int[] lengths, uint elementOid, int dim)
    {

        mixin elmnt!AT;

        int length = lengths[dim];

        AT array;
        static if (isDynamicArray!AT)
            array.length = length;

        int fieldLen;

        foreach(i; 0 .. length)
        {
            static if (isArray!ElemType && !isSomeString!ElemType)
                array[i] = readDimension!ElemType(lengths, elementOid, dim + 1);
            else
            {
                static if (isNullable!ElemType)
                    alias nullableTarget!ElemType E;
                else
                    alias ElemType E;

                read(fieldLen);
                if (fieldLen == -1)
                {
                    static if (isNullable!ElemType || isSomeString!ElemType)
                        array[i] = null;
                    else
                        throw new Exception("Can't set NULL value to non nullable type");
                }
                else
                    array[i] = readBaseType!E(elementOid, fieldLen);
            }
        }

        return array;
    }

    T readArray(T)()
        if (isArray!T)
    {
        alias multiArrayElemType!T U;

        // todo: more validation, better lowerBounds support
        int dims, hasNulls;
        uint elementOid;
        int[] lengths, lowerBounds;

        read(dims);
        read(hasNulls); // 0 or 1
        read(elementOid);

        if (dims == 0)
            return T.init;

        enforce(arrayDimensions!T == dims, "Dimensions of arrays do not match");
        static if (!isNullable!U && !isSomeString!U)
            enforce(!hasNulls, "PostgreSQL returned NULLs but array elements are not Nullable");

        lengths.length = lowerBounds.length = dims;

        int elementCount = 1;

        foreach(i; 0 .. dims)
        {
            int len;

            read(len);
            read(lowerBounds[i]);
            lengths[i] = len;

            elementCount *= len;
        }

        T array = readDimension!T(lengths, elementOid, 0);

        return array;
    }

    T readEnum(T)(int len)
    {
        string genCases() // CTFE
        {
            string s;

            foreach (name; __traits(allMembers, T))
            {
                s ~= text(`case "`, name, `": return T.`, name, `;`);
            }

            return s;
        }

        string enumMember = readString(len);

        switch (enumMember)
        {
            mixin(genCases);
            default: throw new ConvException("Can't set enum value '" ~ enumMember ~ "' to enum type " ~ T.stringof);
        }
    }

    T readBaseType(T)(uint oid, int len = 0)
    {
        auto convError(T)()
        {
            string* type = oid in baseTypes;
            return new ConvException("Can't convert PostgreSQL's type " ~ (type ? *type : to!string(oid)) ~ " to " ~ T.stringof);
        }

        switch (oid)
        {
            case 16: // bool
                static if (isConvertible!(T, bool))
                    return _to!T(read!bool);
                else
                    throw convError!T();
            case 26, 24, 2202, 2203, 2204, 2205, 2206, 3734, 3769: // oid and reg*** aliases
                static if (isConvertible!(T, uint))
                    return _to!T(read!uint);
                else
                    throw convError!T();
            case 21: // int2
                static if (isConvertible!(T, short))
                    return _to!T(read!short);
                else
                    throw convError!T();
            case 23: // int4
                static if (isConvertible!(T, int))
                    return _to!T(read!int);
                else
                    throw convError!T();
            case 20: // int8
                static if (isConvertible!(T, long))
                    return _to!T(read!long);
                else
                    throw convError!T();
            case 700: // float4
                static if (isConvertible!(T, float))
                    return _to!T(read!float);
                else
                    throw convError!T();
            case 701: // float8
                static if (isConvertible!(T, double))
                    return _to!T(read!double);
                else
                    throw convError!T();
            case 1042, 1043, 25, 19, 705: // bpchar, varchar, text, name, unknown
                static if (isConvertible!(T, string))
                    return _to!T(readString(len));
                else
                    throw convError!T();
            case 17: // bytea
                static if (isConvertible!(T, ubyte[]))
                    return _to!T(read!(ubyte[])(len));
                else
                    throw convError!T();
            case 2950: // UUID
                static if(isConvertible!(T, UUID))
                    return _to!T(read!UUID());
                else
                    throw convError!T();
            case 18: // "char"
                static if (isConvertible!(T, char))
                    return _to!T(read!char);
                else
                    throw convError!T();
            case 1082: // date
                static if (isConvertible!(T, Date))
                    return _to!T(read!Date);
                else
                    throw convError!T();
            case 1083: // time
                static if (isConvertible!(T, TimeOfDay))
                    return _to!T(read!TimeOfDay);
                else
                    throw convError!T();
            case 1114: // timestamp
                static if (isConvertible!(T, DateTime))
                    return _to!T(read!DateTime);
                else
                    throw convError!T();
            case 1184: // timestamptz
                static if (isConvertible!(T, SysTime))
                    return _to!T(read!SysTime);
                else
                    throw convError!T();
            case 1186: // interval
                static if (isConvertible!(T, core.time.Duration))
                    return _to!T(read!(core.time.Duration));
                else
                    throw convError!T();
            case 1266: // timetz
                static if (isConvertible!(T, SysTime))
                    return _to!T(readTimeTz);
                else
                    throw convError!T();
            case 2249: // record and other composite types
                static if (isVariantN!T && T.allowed!(Variant[]))
                    return T(readComposite!(Variant[]));
                else
                    return readComposite!T;
            case 2287: // _record and other arrays
                static if (isArray!T && !isSomeString!T)
                    return readArray!T;
                else static if (isVariantN!T && T.allowed!(Variant[]))
                    return T(readArray!(Variant[]));
                else
                    throw convError!T();
            case 114: //JSON
                static if (isConvertible!(T, string))
                    return _to!T(readString(len));
                else
                    throw convError!T();
            default:
                if (oid in conn.arrayTypes)
                    goto case 2287;
                else if (oid in conn.compositeTypes)
                    goto case 2249;
                else if (oid in conn.enumTypes)
                {
                    static if (is(T == enum))
                        return readEnum!T(len);
                    else static if (isConvertible!(T, string))
                        return _to!T(readString(len));
                    else
                        throw convError!T();
                }
        }

        throw convError!T();
    }
}

// workaround, because std.conv currently doesn't support VariantN
template _to(T)
{
    static if (isVariantN!T)
        T _to(S)(S value) { T t = value; return t; }
    else
        T _to(A...)(A args) { return std.conv.to!T(args); }
}

template isConvertible(T, S)
{
    static if (__traits(compiles, { S s; _to!T(s); }) || (isVariantN!T && T.allowed!S))
        enum isConvertible = true;
    else
        enum isConvertible = false;
}

template arrayDimensions(T : T[])
{
	static if (isArray!T && !isSomeString!T)
		enum arrayDimensions = arrayDimensions!T + 1;
	else
		enum arrayDimensions = 1;
}

template arrayDimensions(T)
{
		enum arrayDimensions = 0;
}

template multiArrayElemType(T : T[])
{
    static if (isArray!T && !isSomeString!T)
        alias multiArrayElemType!T multiArrayElemType;
    else
        alias T multiArrayElemType;
}

template multiArrayElemType(T)
{
	alias T multiArrayElemType;
}

static assert(arrayDimensions!(int) == 0);
static assert(arrayDimensions!(int[]) == 1);
static assert(arrayDimensions!(int[][]) == 2);
static assert(arrayDimensions!(int[][][]) == 3);

enum TransactionStatus : char { OutsideTransaction = 'I', InsideTransaction = 'T', InsideFailedTransaction = 'E' };

enum string[int] baseTypes = [
    // boolean types
    16 : "bool",
    // bytea types
    17 : "bytea",
    // character types
    18 : `"char"`, // "char" - 1 byte internal type
    1042 : "bpchar", // char(n) - blank padded
    1043 : "varchar",
    25 : "text",
    19 : "name",
    // numeric types
    21 : "int2",
    23 : "int4",
    20 : "int8",
    700 : "float4",
    701 : "float8",
    1700 : "numeric"
];

public:

enum PGType : int
{
    OID = 26,
    NAME = 19,
    REGPROC = 24,
    BOOLEAN = 16,
    BYTEA = 17,
    CHAR = 18, // 1 byte "char", used internally in PostgreSQL
    BPCHAR = 1042, // Blank Padded char(n), fixed size
    VARCHAR = 1043,
    TEXT = 25,
    INT2 = 21,
    INT4 = 23,
    INT8 = 20,
    FLOAT4 = 700,
    FLOAT8 = 701,

    // reference https://github.com/lpsmith/postgresql-simple/blob/master/src/Database/PostgreSQL/Simple/TypeInfo/Static.hs#L74
    DATE = 1082,
    TIME = 1083,
    TIMESTAMP = 1114,
    TIMESTAMPTZ = 1184,
    INTERVAL = 1186,
    TIMETZ = 1266,

    JSON = 114,
    JSONARRAY = 199
};

class ParamException : Exception
{
    this(string msg, string fn = __FILE__, size_t ln = __LINE__) @safe pure nothrow
    {
        super(msg, fn, ln);
    }
}

/// Exception thrown on server error
class ServerErrorException: Exception
{
    /// Contains information about this _error. Aliased to this.
    ResponseMessage error;
    alias error this;

    this(string msg, string fn = __FILE__, size_t ln = __LINE__) @safe pure nothrow
    {
        super(msg, fn, ln);
    }

    this(ResponseMessage error, string fn = __FILE__, size_t ln = __LINE__)
    {
        super(error.toString(), fn, ln);
        this.error = error;
    }
}

/**
Class encapsulating errors and notices.

This class provides access to fields of ErrorResponse and NoticeResponse
sent by the server. More information about these fields can be found
$(LINK2 http://www.postgresql.org/docs/9.0/static/protocol-error-fields.html,here).
*/
class ResponseMessage
{
    private string[char] fields;

    private string getOptional(char type)
    {
        string* p = type in fields;
        return p ? *p : "";
    }

    /// Message fields
    @property string severity()
    {
        return fields['S'];
    }

    /// ditto
    @property string code()
    {
        return fields['C'];
    }

    /// ditto
    @property string message()
    {
        return fields['M'];
    }

    /// ditto
    @property string detail()
    {
        return getOptional('D');
    }

    /// ditto
    @property string hint()
    {
        return getOptional('H');
    }

    /// ditto
    @property string position()
    {
        return getOptional('P');
    }

    /// ditto
    @property string internalPosition()
    {
        return getOptional('p');
    }

    /// ditto
    @property string internalQuery()
    {
        return getOptional('q');
    }

    /// ditto
    @property string where()
    {
        return getOptional('W');
    }

    /// ditto
    @property string file()
    {
        return getOptional('F');
    }

    /// ditto
    @property string line()
    {
        return getOptional('L');
    }

    /// ditto
    @property string routine()
    {
        return getOptional('R');
    }

    /**
    Returns summary of this message using the most common fields (severity,
    code, message, detail, hint)
    */
    override string toString()
    {
        string s = severity ~ ' ' ~ code ~ ": " ~ message;

        string* detail = 'D' in fields;
        if (detail)
            s ~= "\nDETAIL: " ~ *detail;

        string* hint = 'H' in fields;
        if (hint)
            s ~= "\nHINT: " ~ *hint;

        return s;
    }
}

/**
Class representing connection to PostgreSQL server.
*/
class PGConnection
{
    private:
        PGStream stream;
        string[string] serverParams;
        int serverProcessID;
        int serverSecretKey;
        TransactionStatus trStatus;
        ulong lastPrepared = 0;
        uint[uint] arrayTypes;
        uint[][uint] compositeTypes;
        string[uint][uint] enumTypes;
        bool activeResultSet;

        string reservePrepared()
        {
            synchronized (this)
            {

                return to!string(lastPrepared++);
            }
        }

        Message getMessage()
        {

            char type;
            int len;
			ubyte[1] ub;
			stream.read(ub); // message type

			type = bigEndianToNative!char(ub);
			ubyte[4] ubi;
			stream.read(ubi); // message length, doesn't include type byte

			len = bigEndianToNative!int(ubi) - 4;

            ubyte[] msg;
            if (len > 0)
            {
                msg = new ubyte[len];
                stream.read(msg);
            }

            return Message(this, type, msg);
        }

        void sendStartupMessage(const string[string] params)
        {
            bool localParam(string key)
            {
                switch (key)
                {
                    case "host", "port", "password": return true;
                    default: return false;
                }
            }

            int len = 9; // length (int), version number (int) and parameter-list's delimiter (byte)

            foreach (key, value; params)
            {
                if (localParam(key))
                    continue;

                len += key.length + value.length + 2;
            }

            stream.write(len);
            stream.write(0x0003_0000); // version number 3
            foreach (key, value; params)
            {
                if (localParam(key))
                    continue;
                stream.writeCString(key);
                stream.writeCString(value);
            }
		stream.write(cast(ubyte)0);
	}

        void sendPasswordMessage(string password)
        {
            int len = cast(int)(4 + password.length + 1);

            stream.write('p');
            stream.write(len);
            stream.writeCString(password);
        }

        void sendParseMessage(string statementName, string query, int[] oids)
        {
            int len = cast(int)(4 + statementName.length + 1 + query.length + 1 + 2 + oids.length * 4);

            stream.write('P');
            stream.write(len);
            stream.writeCString(statementName);
            stream.writeCString(query);
            stream.write(cast(short)oids.length);

            foreach (oid; oids)
                stream.write(oid);
        }

        void sendCloseMessage(DescribeType type, string name)
		{
			stream.write('C');
            stream.write(cast(int)(4 + 1 + name.length + 1));
            stream.write(cast(char)type);
            stream.writeCString(name);
        }

        void sendTerminateMessage()
		{
			stream.write('X');
            stream.write(cast(int)4);
        }

        void sendBindMessage(string portalName, string statementName, PGParameters params)
        {
            int paramsLen = 0;
            bool hasText = false;

			foreach (param; params)
            {
                enforce(param.value.hasValue, new ParamException("Parameter $" ~ to!string(param.index) ~ " value is not initialized"));

                void checkParam(T)(int len)
                {
                    if (param.value != null)
                    {
                        enforce(param.value.convertsTo!T, new ParamException("Parameter's value is not convertible to " ~ T.stringof));
                        paramsLen += len;
                    }
                }

                /*final*/ switch (param.type)
                {
                    case PGType.INT2: checkParam!short(2); break;
                    case PGType.INT4: checkParam!int(4); break;
                    case PGType.INT8: checkParam!long(8); break;
                    case PGType.TEXT:
                        paramsLen += param.value.coerce!string.length;
                        hasText = true;
                        break;
                    case PGType.BYTEA:
                        paramsLen += param.value.length;
                        break;
                    case PGType.JSON:
                        paramsLen += param.value.coerce!string.length; // TODO: object serialisation
                        break;
                    case PGType.DATE:
                        paramsLen += 4; break;
                    case PGType.FLOAT4: checkParam!float(4); break;
                    case PGType.FLOAT8: checkParam!double(8); break;
                    case PGType.BOOLEAN: checkParam!bool(1); break;
                    default: assert(0, "Not implemented " ~ to!string(param.type));
                }
            }

            int len = cast(int)( 4 + portalName.length + 1 + statementName.length + 1 + (hasText ? (params.length*2) : 2) + 2 + 2 +
                params.length * 4 + paramsLen + 2 + 2 );

            stream.write('B');
            stream.write(len);
            stream.writeCString(portalName);
            stream.writeCString(statementName);
            if(hasText)
            {
                stream.write(cast(short) params.length);
                foreach(param; params)
                    if(param.type == PGType.TEXT)
                        stream.write(cast(short) 0); // text format
                    else
                        stream.write(cast(short) 1); // binary format
            } else {
                stream.write(cast(short)1); // one parameter format code
                stream.write(cast(short)1); // binary format
            }
            stream.write(cast(short)params.length);

            foreach (param; params)
            {
                if (param.value == null)
                {
                    stream.write(-1);
                    continue;
                }

                switch (param.type)
                {
                    case PGType.INT2:
                        stream.write(cast(int)2);
                        stream.write(param.value.coerce!short);
                        break;
                    case PGType.INT4:
                        stream.write(cast(int)4);
                        stream.write(param.value.coerce!int);
                        break;
                    case PGType.INT8:
                        stream.write(cast(int)8);
                        stream.write(param.value.coerce!long);
                        break;
                    case PGType.FLOAT4:
                        stream.write(cast(int)4);
                        stream.write(param.value.coerce!float);
                        break;
                    case PGType.FLOAT8:
                        stream.write(cast(int)8);
                        stream.write(param.value.coerce!double);
                        break;
                    case PGType.TEXT:
                        auto s = param.value.coerce!string;
                        stream.write(cast(int) s.length);
                        if(s.length) stream.write(cast(ubyte[]) s);
                        break;
                    case PGType.BYTEA:
                        auto s = param.value;
                        stream.write(cast(int) s.length);

                        ubyte[] x;
                        x.length = s.length;
                        for (int i = 0; i < x.length; i++) {
                            x[i] = s[i].get!(ubyte);
                        }
                        stream.write(x);
                        break;
                    case PGType.JSON:
                        auto s = param.value.coerce!string;
                        stream.write(cast(int) s.length);
                        stream.write(cast(ubyte[]) s);
                        break;
                    case PGType.DATE:
                        stream.write(cast(int) 4);
                        stream.write(Date.fromISOString(param.value.coerce!string));
                        break;
                    case PGType.BOOLEAN:
                        stream.write(cast(int) 1);
                        stream.write(param.value.coerce!bool);
                        break;
                    default:
						assert(0, "Not implemented " ~ to!string(param.type));
                }
            }

            stream.write(cast(short)1); // one result format code
            stream.write(cast(short)1); // binary format
        }

        enum DescribeType : char { Statement = 'S', Portal = 'P' }

        void sendDescribeMessage(DescribeType type, string name)
		{
			stream.write('D');
            stream.write(cast(int)(4 + 1 + name.length + 1));
            stream.write(cast(char)type);
            stream.writeCString(name);
        }

        void sendExecuteMessage(string portalName, int maxRows)
		{
			stream.write('E');
            stream.write(cast(int)(4 + portalName.length + 1 + 4));
            stream.writeCString(portalName);
            stream.write(cast(int)maxRows);
        }

        void sendFlushMessage()
		{
			stream.write('H');
            stream.write(cast(int)4);
        }

        void sendSyncMessage()
		{
			stream.write('S');
            stream.write(cast(int)4);
        }

        ResponseMessage handleResponseMessage(Message msg)
        {
            enforce(msg.data.length >= 2);

			char ftype;
            string fvalue;
            ResponseMessage response = new ResponseMessage;

            while (true)
            {
                msg.read(ftype);
                if(ftype <=0) break;

                msg.readCString(fvalue);
                response.fields[ftype] = fvalue;
            }

            return response;
        }

        void checkActiveResultSet()
        {
            enforce(!activeResultSet, "There's active result set, which must be closed first.");
        }

        void prepare(string statementName, string query, PGParameters params)
        {
            checkActiveResultSet();
            sendParseMessage(statementName, query, params.getOids());

            sendFlushMessage();

	receive:

            Message msg = getMessage();

		switch (msg.type)
            {
                case 'E':
                    // ErrorResponse
                    ResponseMessage response = handleResponseMessage(msg);
                    sendSyncMessage();
                    throw new ServerErrorException(response);
                case '1':
                    // ParseComplete
                    return;
                default:
                    // async notice, notification
                    goto receive;
            }
        }

        void unprepare(string statementName)
        {
            checkActiveResultSet();
            sendCloseMessage(DescribeType.Statement, statementName);
            sendFlushMessage();

        receive:

            Message msg = getMessage();

            switch (msg.type)
            {
                case 'E':
                    // ErrorResponse
                    ResponseMessage response = handleResponseMessage(msg);
                    throw new ServerErrorException(response);
                case '3':
                    // CloseComplete
                    return;
                default:
                    // async notice, notification
                    goto receive;
            }
        }

        PGFields bind(string portalName, string statementName, PGParameters params)
        {
            checkActiveResultSet();
            sendCloseMessage(DescribeType.Portal, portalName);
            sendBindMessage(portalName, statementName, params);
            sendDescribeMessage(DescribeType.Portal, portalName);
            sendFlushMessage();

        receive:

            Message msg = getMessage();

            switch (msg.type)
            {
                case 'E':
                    // ErrorResponse
                    ResponseMessage response = handleResponseMessage(msg);
                    sendSyncMessage();
                    throw new ServerErrorException(response);
                case '3':
                    // CloseComplete
                    goto receive;
                case '2':
                    // BindComplete
                    goto receive;
                case 'T':
                    // RowDescription (response to Describe)
                    PGField[] fields;
                    short fieldCount;
                    short formatCode;
                    PGField fi;

                    msg.read(fieldCount);

                    fields.length = fieldCount;

                    foreach (i; 0..fieldCount)
                    {
                        msg.readCString(fi.name);
                        msg.read(fi.tableOid);
                        msg.read(fi.index);
                        msg.read(fi.oid);
                        msg.read(fi.typlen);
                        msg.read(fi.modifier);
                        msg.read(formatCode);

                        enforce(formatCode == 1, new Exception("Field's format code returned in RowDescription is not 1 (binary)"));

                        fields[i] = fi;
                    }

                    return cast(PGFields)fields;
                case 'n':
                    // NoData (response to Describe)
                    return new immutable(PGField)[0];
                default:
                    // async notice, notification
                    goto receive;
            }
        }

        ulong executeNonQuery(string portalName, out uint oid)
        {
            checkActiveResultSet();
            ulong rowsAffected = 0;

            sendExecuteMessage(portalName, 0);
            sendSyncMessage();
            sendFlushMessage();

        receive:

            Message msg = getMessage();

            switch (msg.type)
            {
                case 'E':
                    // ErrorResponse
                    ResponseMessage response = handleResponseMessage(msg);
                    throw new ServerErrorException(response);
                case 'D':
                    // DataRow
                    finalizeQuery();
                    throw new Exception("This query returned rows.");
                case 'C':
                    // CommandComplete
                    string tag;

                    msg.readCString(tag);

                    // GDC indexOf name conflict in std.string and std.algorithm
                    auto s1 = std.string.indexOf(tag, ' ');
                    if (s1 >= 0) {
                        switch (tag[0 .. s1]) {
                            case "INSERT":
                                // INSERT oid rows
                                auto s2 = lastIndexOf(tag, ' ');
                                assert(s2 > s1);
                                oid = to!uint(tag[s1 + 1 .. s2]);
                                rowsAffected = to!ulong(tag[s2 + 1 .. $]);
                                break;
                            case "DELETE", "UPDATE", "MOVE", "FETCH":
                                // DELETE rows
                                rowsAffected = to!ulong(tag[s1 + 1 .. $]);
                                break;
                            default:
                                // CREATE TABLE
                                break;
                         }
                    }

                    goto receive;

                case 'I':
                    // EmptyQueryResponse
                    goto receive;
                case 'Z':
                    // ReadyForQuery
                    return rowsAffected;
                default:
                    // async notice, notification
                    goto receive;
            }
        }

        DBRow!Specs fetchRow(Specs...)(ref Message msg, ref PGFields fields)
        {
            alias DBRow!Specs Row;

            static if (Row.hasStaticLength)
            {
                alias Row.fieldTypes fieldTypes;

                static string genFieldAssigns() // CTFE
                {
                    string s = "";

                    foreach (i; 0 .. fieldTypes.length)
                    {
                        s ~= "msg.read(fieldLen);\n";
                        s ~= "if (fieldLen == -1)\n";
                        s ~= text("row.setNull!(", i, ")();\n");
                        s ~= "else\n";
                        s ~= text("row.set!(fieldTypes[", i, "], ", i, ")(",
                                  "msg.readBaseType!(fieldTypes[", i, "])(fields[", i, "].oid, fieldLen)",
                                  ");\n");
                        // text() doesn't work with -inline option, CTFE bug
                    }

                    return s;
                }
            }

            Row row;
            short fieldCount;
            int fieldLen;

            msg.read(fieldCount);

            static if (Row.hasStaticLength)
            {
                Row.checkReceivedFieldCount(fieldCount);
                mixin(genFieldAssigns);
            }
            else
            {
                row.setLength(fieldCount);

                foreach (i; 0 .. fieldCount)
                {
                    msg.read(fieldLen);
                    if (fieldLen == -1)
                        row.setNull(i);
                    else
                        row[i] = msg.readBaseType!(Row.ElemType)(fields[i].oid, fieldLen);
                }
            }

            return row;
        }

        void finalizeQuery()
        {
            Message msg;

            do
            {
                msg = getMessage();

                // TODO: process async notifications
            }
            while (msg.type != 'Z'); // ReadyForQuery
        }

        PGResultSet!Specs executeQuery(Specs...)(string portalName, ref PGFields fields)
        {
            checkActiveResultSet();

            PGResultSet!Specs result = new PGResultSet!Specs(this, fields, &fetchRow!Specs);

            ulong rowsAffected = 0;

            sendExecuteMessage(portalName, 0);
            sendSyncMessage();
            sendFlushMessage();

        receive:

            Message msg = getMessage();

            switch (msg.type)
            {
                case 'D':
                    // DataRow
                    alias DBRow!Specs Row;

                    result.row = fetchRow!Specs(msg, fields);
                    static if (!Row.hasStaticLength)
                        result.row.columnToIndex = &result.columnToIndex;
                    result.validRow = true;
                    result.nextMsg = getMessage();

                    activeResultSet = true;

                    return result;
                case 'C':
                    // CommandComplete
                    string tag;

                    msg.readCString(tag);

                    auto s2 = lastIndexOf(tag, ' ');
                    if (s2 >= 0)
                    {
                        rowsAffected = to!ulong(tag[s2 + 1 .. $]);
                    }

                    goto receive;
                case 'I':
                    // EmptyQueryResponse
                    throw new Exception("Query string is empty.");
                case 's':
                    // PortalSuspended
                    throw new Exception("Command suspending is not supported.");
                case 'Z':
                    // ReadyForQuery
                    result.nextMsg = msg;
                    return result;
                case 'E':
                    // ErrorResponse
                    ResponseMessage response = handleResponseMessage(msg);
                    throw new ServerErrorException(response);
                default:
                    // async notice, notification
                    goto receive;
            }

            assert(0);
        }

    public:


        /**
        Opens connection to server.

        Params:
        params = Associative array of string keys and values.

        Currently recognized parameters are:
        $(UL
            $(LI host - Host name or IP address of the server. Required.)
            $(LI port - Port number of the server. Defaults to 5432.)
            $(LI user - The database user. Required.)
            $(LI database - The database to connect to. Defaults to the user name.)
            $(LI options - Command-line arguments for the backend. (This is deprecated in favor of setting individual run-time parameters.))
        )

        In addition to the above, any run-time parameter that can be set at backend start time might be listed.
        Such settings will be applied during backend start (after parsing the command-line options if any).
        The values will act as session defaults.

        Examples:
        ---
        auto conn = new PGConnection([
            "host" : "localhost",
            "database" : "test",
            "user" : "postgres",
            "password" : "postgres"
        ]);
        ---
        */
        this(const string[string] params)
        {
            enforce("host" in params, new ParamException("Required parameter 'host' not found"));
            enforce("user" in params, new ParamException("Required parameter 'user' not found"));

            string[string] p = cast(string[string])params;

            ushort port = "port" in params? parse!ushort(p["port"]) : 5432;

            version(Have_vibe_d_core)
            {
                stream = new PGStream(new TCPConnectionWrapper(params["host"], port));
            }
            else
            {
                stream = new PGStream(new TcpSocket);
                stream.socket.connect(new InternetAddress(params["host"], port));
            }
            sendStartupMessage(params);

        receive:

    		Message msg = getMessage();

            switch (msg.type)
            {
                case 'E', 'N':
                    // ErrorResponse, NoticeResponse

                    ResponseMessage response = handleResponseMessage(msg);

				    if (msg.type == 'N')
                        goto receive;

                    throw new ServerErrorException(response);
                case 'R':
                    // AuthenticationXXXX
                    enforce(msg.data.length >= 4);

                    int atype;

                    msg.read(atype);

                    switch (atype)
                    {
                        case 0:
                            // authentication successful, now wait for another messages
                            goto receive;
                        case 3:
                            // clear-text password is required
                            enforce("password" in params, new ParamException("Required parameter 'password' not found"));
                            enforce(msg.data.length == 4);

                            sendPasswordMessage(params["password"]);

                            goto receive;
                        case 5:
                            // MD5-hashed password is required, formatted as:
                            // "md5" + md5(md5(password + username) + salt)
                            // where md5() returns lowercase hex-string
                            enforce("password" in params, new ParamException("Required parameter 'password' not found"));
                            enforce(msg.data.length == 8);

                            char[3 + 32] password;
                            password[0 .. 3] = "md5";
                            password[3 .. $] = MD5toHex(MD5toHex(
                                params["password"], params["user"]), msg.data[4 .. 8]);

                            sendPasswordMessage(to!string(password));

                            goto receive;
                        default:
                            // non supported authentication type, close connection
                            this.close();
                            throw new Exception("Unsupported authentication type");
                    }

                case 'S':
                    // ParameterStatus
                    enforce(msg.data.length >= 2);

                    string pname, pvalue;

                    msg.readCString(pname);
                    msg.readCString(pvalue);

                    serverParams[pname] = pvalue;

                    goto receive;

                case 'K':
                    // BackendKeyData
                    enforce(msg.data.length == 8);

                    msg.read(serverProcessID);
                    msg.read(serverSecretKey);

                    goto receive;

                case 'Z':
                    // ReadyForQuery
                    enforce(msg.data.length == 1);

                    msg.read(cast(char)trStatus);

                    // check for validity
                    switch (trStatus)
                    {
                        case 'I', 'T', 'E': break;
                        default: throw new Exception("Invalid transaction status");
                    }

                    // connection is opened and now it's possible to send queries
                    reloadAllTypes();
                    return;
                default:
                    // unknown message type, ignore it
                    goto receive;
            }
        }

        /// Closes current connection to the server.
        void close()
        {
            sendTerminateMessage();
            stream.socket.close();
        }

        /// Shorthand methods using temporary PGCommand. Semantics is the same as PGCommand's.
        ulong executeNonQuery(string query)
        {
            scope cmd = new PGCommand(this, query);
            return cmd.executeNonQuery();
        }

        /// ditto
        PGResultSet!Specs executeQuery(Specs...)(string query)
        {
            scope cmd = new PGCommand(this, query);
            return cmd.executeQuery!Specs();
        }

        /// ditto
        DBRow!Specs executeRow(Specs...)(string query, bool throwIfMoreRows = true)
        {
            scope cmd = new PGCommand(this, query);
            return cmd.executeRow!Specs(throwIfMoreRows);
        }

        /// ditto
        T executeScalar(T)(string query, bool throwIfMoreRows = true)
        {
            scope cmd = new PGCommand(this, query);
            return cmd.executeScalar!T(throwIfMoreRows);
        }

        void reloadArrayTypes()
        {
            auto cmd = new PGCommand(this, "SELECT oid, typelem FROM pg_type WHERE typcategory = 'A'");
            auto result = cmd.executeQuery!(uint, "arrayOid", uint, "elemOid");
            scope(exit) result.close;

            arrayTypes = null;

            foreach (row; result)
            {
                arrayTypes[row.arrayOid] = row.elemOid;
            }

            arrayTypes.rehash;
        }

        void reloadCompositeTypes()
        {
            auto cmd = new PGCommand(this, "SELECT a.attrelid, a.atttypid FROM pg_attribute a JOIN pg_type t ON
                                     a.attrelid = t.typrelid WHERE a.attnum > 0 ORDER BY a.attrelid, a.attnum");
            auto result = cmd.executeQuery!(uint, "typeOid", uint, "memberOid");
            scope(exit) result.close;

            compositeTypes = null;

            uint lastOid = 0;
            uint[]* memberOids;

            foreach (row; result)
            {
                if (row.typeOid != lastOid)
                {
                    compositeTypes[lastOid = row.typeOid] = new uint[0];
                    memberOids = &compositeTypes[lastOid];
                }

                *memberOids ~= row.memberOid;
            }

            compositeTypes.rehash;
        }

        void reloadEnumTypes()
        {
            auto cmd = new PGCommand(this, "SELECT enumtypid, oid, enumlabel FROM pg_enum ORDER BY enumtypid, oid");
            auto result = cmd.executeQuery!(uint, "typeOid", uint, "valueOid", string, "valueLabel");
            scope(exit) result.close;

            enumTypes = null;

            uint lastOid = 0;
            string[uint]* enumValues;

            foreach (row; result)
            {
                if (row.typeOid != lastOid)
                {
                    if (lastOid > 0)
                        (*enumValues).rehash;

                    enumTypes[lastOid = row.typeOid] = null;
                    enumValues = &enumTypes[lastOid];
                }

                (*enumValues)[row.valueOid] = row.valueLabel;
            }

            if (lastOid > 0)
                (*enumValues).rehash;

            enumTypes.rehash;
        }

        void reloadAllTypes()
        {
            // todo: make simpler type lists, since we need only oids of types (without their members)
            reloadArrayTypes();
            reloadCompositeTypes();
            reloadEnumTypes();
        }
}

/// Class representing single query parameter
class PGParameter
{
    private PGParameters params;
    immutable short index;
    immutable PGType type;
    private Variant _value;

    /// Value bound to this parameter
    @property Variant value()
    {
        return _value;
    }
    /// ditto
    @property Variant value(T)(T v)
    {
        params.changed = true;
        return _value = Variant(v);
    }

    private this(PGParameters params, short index, PGType type)
    {
        enforce(index > 0, new ParamException("Parameter's index must be > 0"));
        this.params = params;
        this.index = index;
        this.type = type;
    }
}

/// Collection of query parameters
class PGParameters
{
    private PGParameter[short] params;
    private PGCommand cmd;
    private bool changed;

    private int[] getOids()
    {
        short[] keys = params.keys;
        sort(keys);

        int[] oids = new int[params.length];

        foreach (int i, key; keys)
        {
            oids[i] = params[key].type;
        }

        return oids;
    }

    ///
    @property short length()
    {
        return cast(short)params.length;
    }

    private this(PGCommand cmd)
    {
        this.cmd = cmd;
    }

    /**
    Creates and returns new parameter.
    Examples:
    ---
    // without spaces between $ and number
    auto cmd = new PGCommand(conn, "INSERT INTO users (name, surname) VALUES ($ 1, $ 2)");
    cmd.parameters.add(1, PGType.TEXT).value = "John";
    cmd.parameters.add(2, PGType.TEXT).value = "Doe";

    assert(cmd.executeNonQuery == 1);
    ---
    */
    PGParameter add(short index, PGType type)
    {
        enforce(!cmd.prepared, "Can't add parameter to prepared statement.");
        changed = true;
        return params[index] = new PGParameter(this, index, type);
    }

    // todo: remove()

    PGParameter opIndex(short index)
    {
        return params[index];
    }

    int opApply(int delegate(ref PGParameter param) dg)
    {
        int result = 0;

        foreach (number; sort(params.keys))
        {
            result = dg(params[number]);

            if (result)
                break;
        }

        return result;
    }
}

/// Array of fields returned by the server
alias immutable(PGField)[] PGFields;

/// Contains information about fields returned by the server
struct PGField
{
    /// The field name.
    string name;
    /// If the field can be identified as a column of a specific table, the object ID of the table; otherwise zero.
    uint tableOid;
    /// If the field can be identified as a column of a specific table, the attribute number of the column; otherwise zero.
    short index;
    /// The object ID of the field's data type.
    uint oid;
    /// The data type size (see pg_type.typlen). Note that negative values denote variable-width types.
    short typlen;
    /// The type modifier (see pg_attribute.atttypmod). The meaning of the modifier is type-specific.
    int modifier;
}

/// Class encapsulating prepared or non-prepared statements (commands).
class PGCommand
{
    private PGConnection conn;
    private string _query;
    private PGParameters params;
    private PGFields _fields = null;
    private string preparedName;
    private uint _lastInsertOid;
    private bool prepared;

    /// List of parameters bound to this command
    @property PGParameters parameters()
    {
        return params;
    }

    /// List of fields that will be returned from the server. Available after successful call to bind().
    @property PGFields fields()
    {
        return _fields;
    }

    /**
    Checks if this is query or non query command. Available after successful call to bind().
    Returns: true if server returns at least one field (column). Otherwise false.
    */
    @property bool isQuery()
    {
        enforce(_fields !is null, new Exception("bind() must be called first."));
        return _fields.length > 0;
    }

    /// Returns: true if command is currently prepared, otherwise false.
    @property bool isPrepared()
    {
        return prepared;
    }

    /// Query assigned to this command.
    @property string query()
    {
        return _query;
    }
    /// ditto
    @property string query(string query)
    {
        enforce(!prepared, "Can't change query for prepared statement.");
        return _query = query;
    }

    /// If table is with OIDs, it contains last inserted OID.
    @property uint lastInsertOid()
    {
        return _lastInsertOid;
    }

    this(PGConnection conn, string query = "")
    {
        this.conn = conn;
        _query = query;
        params = new PGParameters(this);
        _fields = new immutable(PGField)[0];
        preparedName = "";
        prepared = false;
    }

    /// Prepare this statement, i.e. cache query plan.
    void prepare()
    {
        enforce(!prepared, "This command is already prepared.");
        preparedName = conn.reservePrepared();
        conn.prepare(preparedName, _query, params);
        prepared = true;
        params.changed = true;
    }

    /// Unprepare this statement. Goes back to normal query planning.
    void unprepare()
    {
        enforce(prepared, "This command is not prepared.");
        conn.unprepare(preparedName);
        preparedName = "";
        prepared = false;
        params.changed = true;
    }

    /**
    Binds values to parameters and updates list of returned fields.

    This is normally done automatically, but it may be useful to check what fields
    would be returned from a query, before executing it.
    */
    void bind()
    {
        checkPrepared(false);
        _fields = conn.bind(preparedName, preparedName, params);
        params.changed = false;
    }

    private void checkPrepared(bool bind)
    {
        if (!prepared)
        {
            // use unnamed statement & portal
            conn.prepare("", _query, params);
            if (bind)
            {
                _fields = conn.bind("", "", params);
                params.changed = false;
            }
        }
    }

    private void checkBound()
    {
        if (params.changed)
            bind();
    }

    /**
    Executes a non query command, i.e. query which doesn't return any rows. Commonly used with
    data manipulation commands, such as INSERT, UPDATE and DELETE.
    Examples:
    ---
    auto cmd = new PGCommand(conn, "DELETE * FROM table");
    auto deletedRows = cmd.executeNonQuery;
    cmd.query = "UPDATE table SET quantity = 1 WHERE price > 100";
    auto updatedRows = cmd.executeNonQuery;
    cmd.query = "INSERT INTO table VALUES(1, 50)";
    assert(cmd.executeNonQuery == 1);
    ---
    Returns: Number of affected rows.
    */
    ulong executeNonQuery()
    {
        checkPrepared(true);
        checkBound();
        return conn.executeNonQuery(preparedName, _lastInsertOid);
    }

    /**
    Executes query which returns row sets, such as SELECT command.
    Params:
    bufferedRows = Number of rows that may be allocated at the same time.
    Returns: InputRange of DBRow!Specs.
    */
    PGResultSet!Specs executeQuery(Specs...)()
    {
        checkPrepared(true);
        checkBound();
        return conn.executeQuery!Specs(preparedName, _fields);
    }

    /**
    Executes query and returns only first row of the result.
    Params:
    throwIfMoreRows = If true, throws Exception when result contains more than one row.
    Examples:
    ---
    auto cmd = new PGCommand(conn, "SELECT 1, 'abc'");
    auto row1 = cmd.executeRow!(int, string); // returns DBRow!(int, string)
    assert(is(typeof(i[0]) == int) && is(typeof(i[1]) == string));
    auto row2 = cmd.executeRow; // returns DBRow!(Variant[])
    ---
    Throws: Exception if result doesn't contain any rows or field count do not match.
    Throws: Exception if result contains more than one row when throwIfMoreRows is true.
    */
    DBRow!Specs executeRow(Specs...)(bool throwIfMoreRows = true)
    {
        auto result = executeQuery!Specs();
        scope(exit) result.close();
        enforce(!result.empty(), "Result doesn't contain any rows.");
        auto row = result.front();
        if (throwIfMoreRows)
        {
            result.popFront();
            enforce(result.empty(), "Result contains more than one row.");
        }
        return row;
    }

    /**
    Executes query returning exactly one row and field. By default, returns Variant type.
    Params:
    throwIfMoreRows = If true, throws Exception when result contains more than one row.
    Examples:
    ---
    auto cmd = new PGCommand(conn, "SELECT 1");
    auto i = cmd.executeScalar!int; // returns int
    assert(is(typeof(i) == int));
    auto v = cmd.executeScalar; // returns Variant
    ---
    Throws: Exception if result doesn't contain any rows or if it contains more than one field.
    Throws: Exception if result contains more than one row when throwIfMoreRows is true.
    */
    T executeScalar(T = Variant)(bool throwIfMoreRows = true)
    {
        auto result = executeQuery!T();
        scope(exit) result.close();
        enforce(!result.empty(), "Result doesn't contain any rows.");
        T row = result.front();
        if (throwIfMoreRows)
        {
            result.popFront();
            enforce(result.empty(), "Result contains more than one row.");
        }
        return row;
    }
}

/// Input range of DBRow!Specs
class PGResultSet(Specs...)
{
    alias DBRow!Specs Row;
    alias Row delegate(ref Message msg, ref PGFields fields) FetchRowDelegate;

    private FetchRowDelegate fetchRow;
    private PGConnection conn;
    private PGFields fields;
    private Row row;
    private bool validRow;
    private Message nextMsg;
    private size_t[][string] columnMap;

    private this(PGConnection conn, ref PGFields fields, FetchRowDelegate dg)
    {
        this.conn = conn;
        this.fields = fields;
        this.fetchRow = dg;
        validRow = false;

        foreach (i, field; fields)
        {
            size_t[]* indices = field.name in columnMap;

            if (indices)
                *indices ~= i;
            else
                columnMap[field.name] = [i];
        }
    }

    private size_t columnToIndex(string column, size_t index)
    {
        size_t[]* indices = column in columnMap;
        enforce(indices, "Unknown column name");
        return (*indices)[index];
    }

    pure nothrow bool empty()
    {
        return !validRow;
    }

    void popFront()
    {
        if (nextMsg.type == 'D')
        {
            row = fetchRow(nextMsg, fields);
            static if (!Row.hasStaticLength)
                row.columnToIndex = &columnToIndex;
            validRow = true;
            nextMsg = conn.getMessage();
        }
        else
            validRow = false;
    }

    pure nothrow Row front()
    {
        return row;
    }

    /// Closes current result set. It must be closed before issuing another query on the same connection.
    void close()
    {
        if (nextMsg.type != 'Z')
            conn.finalizeQuery();
        conn.activeResultSet = false;
    }

    int opApply(int delegate(ref Row row) dg)
    {
        int result = 0;

        while (!empty)
        {
            result = dg(row);
            popFront;

            if (result)
                break;
        }

        return result;
    }

    int opApply(int delegate(ref size_t i, ref Row row) dg)
    {
        int result = 0;
        size_t i;

        while (!empty)
        {
            result = dg(i, row);
            popFront;
            i++;

            if (result)
                break;
        }

        return result;
    }
}


version(Have_vibe_d_core)
{
    import vibe.core.connectionpool;

    // wrap vibe.d TCPConnection class with the scope of reopening the tcp connection if closed 
    // by PostgreSQL it for some reason.
    // see https://forum.rejectedsoftware.com/groups/rejectedsoftware.vibed/thread/44097/
    private class TCPConnectionWrapper 
    {
        this(string host, ushort port, string bindInterface = null, ushort bindPort = cast(ushort)0u)
        {
            this.host = host;
            this.port = port;
            this.bindInterface = bindInterface;
            this.bindPort = bindPort;

            connect();
        }

        void close(){ tcpConnection.close(); }

        void write(const(ubyte[]) bytes)
        {
            // Vibe:  "... If connected is false, writing to the connection will trigger an exception ..."
            if (!tcpConnection.connected)
            {
                // Vibe: " ... Note that close must always be called, even if the remote has already closed the
                //             connection. Failure to do so will result in resource and memory leakage.
                tcpConnection.close();
                connect();
            }
            tcpConnection.write(bytes);
        }

        void read(ubyte[] dst)
        {
            if (!tcpConnection.connected)
            {
                tcpConnection.close();
                connect();
            }
            if (!tcpConnection.empty)
            {
                tcpConnection.read(dst);
            }
        }

        private
        {
            void connect()
            {
                tcpConnection = connectTCP(host, port, bindInterface, bindPort);
            }

            string host;
            string bindInterface;
            ushort port;
            ushort bindPort;

            TCPConnection tcpConnection;
        }
    }

    class PostgresDB {
        private {
            string[string] m_params;
            ConnectionPool!PGConnection m_pool;
        }

        this(string[string] conn_params)
        {
            m_params = conn_params.dup;
            m_pool = new ConnectionPool!PGConnection(&createConnection);
        }

        auto lockConnection() { return m_pool.lockConnection(); }

        private PGConnection createConnection()
        {
            return new PGConnection(m_params);
        }
    }
}
else
{
	class PostgresDB() {
		static assert(false,
		              "The 'PostgresDB' connection pool requires Vibe.d and therefore "~
		              "must be used with -version=Have_vibe_d_core"
		              );
	}
}

