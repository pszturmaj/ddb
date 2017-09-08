module ddb.db;

/**
Common relational database interfaces.

Copyright: Copyright Piotr Szturmaj 2011-.
License: $(LINK2 http://boost.org/LICENSE_1_0.txt, Boost License 1.0).
Authors: Piotr Szturmaj
*/

//module db;

import std.conv, std.traits, std.typecons, std.typetuple, std.variant;
import std.format;

static import std.typecons;

/**
Data row returned from database servers.

DBRow may be instantiated with any number of arguments. It subtypes base type which
depends on that number:

$(TABLE
    $(TR $(TH Number of arguments) $(TH Base type))
    $(TR $(TD 0) $(TD Variant[] $(BR)$(BR)
    It is default dynamic row, which can handle arbitrary number of columns and any of their types.
    ))
    $(TR $(TD 1) $(TD Specs itself, more precisely Specs[0] $(BR)
    ---
    struct S { int i, float f }

    DBRow!int rowInt;
    DBRow!S rowS;
    DBRow!(Tuple!(string, bool)) rowTuple;
    DBRow!(int[10]) rowSA;
    DBRow!(bool[]) rowDA;
    ---
    ))
    $(TR $(TD >= 2) $(TD Tuple!Specs $(BR)
    ---
    DBRow!(int, string) row1; // two arguments
    DBRow!(int, "i") row2; // two arguments
    ---
    ))
)

If there is only one argument, the semantics depend on its type:

$(TABLE
    $(TR $(TH Type) $(TH Semantics))
    $(TR $(TD base type, such as int) $(TD Row contains only one column of that type))
    $(TR $(TD struct) $(TD Row columns are mapped to fields of the struct in the same order))
    $(TR $(TD Tuple) $(TD Row columns are mapped to tuple fields in the same order))
    $(TR $(TD static array) $(TD Row columns are mapped to array items, they share the same type))
    $(TR $(TD dynamic array) $(TD Same as static array, except that column count may change during runtime))
)
Note: String types are treated as base types.

There is an exception for RDBMSes which are capable of returning arrays and/or composite types. If such a
database server returns array or composite in one column it may be mapped to DBRow as if it was many columns.
For example:
---
struct S { string field1; int field2; }
DBRow!S row;
---
In this case row may handle result that either:
$(UL
    $(LI has two columns convertible to respectively, string and int)
    $(LI has one column with composite type compatible with S)
)

_DBRow's instantiated with dynamic array (and thus default Variant[]) provide additional bracket syntax
for accessing fields:
---
auto value = row["columnName"];
---
There are cases when result contains duplicate column names. Normally column name inside brackets refers
to the first column of that name. To access other columns with that name, use additional index parameter:
---
auto value = row["columnName", 1]; // second column named "columnName"

auto value = row["columnName", 0]; // first column named "columnName"
auto value = row["columnName"]; // same as above
---

Examples:

Default untyped (dynamic) _DBRow:
---
DBRow!() row1;
DBRow!(Variant[]) row2;

assert(is(typeof(row1.base == row2.base)));

auto cmd = new PGCommand(conn, "SElECT typname, typlen FROM pg_type");
auto result = cmd.executeQuery;

foreach (i, row; result)
{
    writeln(i, " - ", row["typname"], ", ", row["typlen"]);
}

result.close;
---
_DBRow with only one field:
---
DBRow!int row;
row = 10;
row += 1;
assert(row == 11);

DBRow!Variant untypedRow;
untypedRow = 10;
---
_DBRow with more than one field:
---
struct S { int i; string s; }
alias Tuple!(int, "i", string, "s") TS;

// all three rows are compatible
DBRow!S row1;
DBRow!TS row2;
DBRow!(int, "i", string, "s") row3;

row1.i = row2.i = row3.i = 10;
row1.s = row2.s = row3.s = "abc";

// these two rows are also compatible
DBRow!(int, int) row4;
DBRow!(int[2]) row5;

row4[0] = row5[0] = 10;
row4[1] = row5[1] = 20;
---
Advanced example:
---
enum Axis { x, y, z }
struct SubRow1 { string s; int[] nums; int num; }
alias Tuple!(int, "num", string, "s") SubRow2;
struct Row { SubRow1 left; SubRow2[] right; Axis axis; string text; }

auto cmd = new PGCommand(conn, "SELECT ROW('text', ARRAY[1, 2, 3], 100),
                                ARRAY[ROW(1, 'str'), ROW(2, 'aab')], 'x', 'anotherText'");

auto row = cmd.executeRow!Row;

assert(row.left.s == "text");
assert(row.left.nums == [1, 2, 3]);
assert(row.left.num == 100);
assert(row.right[0].num == 1 && row.right[0].s == "str");
assert(row.right[1].num == 2 && row.right[1].s == "aab");
assert(row.axis == Axis.x);
assert(row.s == "anotherText");
---
*/
struct DBRow(Specs...)
{
    static if (Specs.length == 0)
        alias Variant[] T;
    else static if (Specs.length == 1)
        alias Specs[0] T;
    else
        alias Tuple!Specs T;

    T base;
    alias base this;

    static if (isDynamicArray!T && !isSomeString!T)
    {
		mixin template elmnt(U : U[]){
			alias U ElemType;
		}
        mixin elmnt!T;
        enum hasStaticLength = false;

        void setLength(size_t length)
        {
            base.length = length;
        }

        void setNull(size_t index)
        {
            static if (isNullable!ElemType)
                base[index] = null;
            else
                throw new Exception("Cannot set NULL to field " ~ to!string(index) ~ " of " ~ T.stringof ~ ", it is not nullable");
        }

        ColumnToIndexDelegate columnToIndex;

        ElemType opIndex(string column, size_t index)
        {
            return base[columnToIndex(column, index)];
        }

        ElemType opIndexAssign(ElemType value, string column, size_t index)
        {
            return base[columnToIndex(column, index)] = value;
        }

        ElemType opIndex(string column)
        {
            return base[columnToIndex(column, 0)];
        }

        ElemType opIndexAssign(ElemType value, string column)
        {
            return base[columnToIndex(column, 0)] = value;
        }

        ElemType opIndex(size_t index)
        {
            return base[index];
        }

        ElemType opIndexAssign(ElemType value, size_t index)
        {
            return base[index] = value;
        }
    }
    else static if (isCompositeType!T)
    {
        static if (isStaticArray!T)
        {
            template ArrayTypeTuple(AT : U[N], U, size_t N)
            {
                static if (N > 1)
                    alias TypeTuple!(U, ArrayTypeTuple!(U[N - 1])) ArrayTypeTuple;
                else
                    alias TypeTuple!U ArrayTypeTuple;
            }

            alias ArrayTypeTuple!T fieldTypes;
        }
        else
            alias FieldTypeTuple!T fieldTypes;

        enum hasStaticLength = true;

        void set(U, size_t index)(U value)
        {
            static if (isStaticArray!T)
                base[index] = value;
            else
                base.tupleof[index] = value;
        }

        void setNull(size_t index)()
        {
            static if (isNullable!(fieldTypes[index]))
            {
                static if (isStaticArray!T)
                    base[index] = null;
                else static if (is(typeof(base.tupleof[index]) == Option!U, U))
                    base.tupleof[index].nullify;
                else
                    base.tupleof[index] = null;
            }
            else
                throw new Exception("Cannot set NULL to field " ~ to!string(index) ~ " of " ~ T.stringof ~ ", it is not nullable");
        }
    }
    else static if (Specs.length == 1)
    {
        alias TypeTuple!T fieldTypes;
        enum hasStaticLength = true;

        void set(T, size_t index)(T value)
        {
            base = value;
        }

        void setNull(size_t index)()
        {
            static if (isNullable!T)
                base = null;
            else
                throw new Exception("Cannot set NULL to " ~ T.stringof ~ ", it is not nullable");
        }
    }

    static if (hasStaticLength)
    {
        /**
        Checks if received field count matches field count of this row type.

        This is used internally by clients and it applies only to DBRow types, which have static number of fields.
        */
        static pure void checkReceivedFieldCount(int fieldCount)
        {
            if (fieldTypes.length != fieldCount)
                throw new Exception(format("Received field(%s) count is not equal to %s's field count(%s)", fieldCount, T.stringof, fieldTypes.length));
        }
    }

    string toString()
    {
        return to!string(base);
    }
}

alias size_t delegate(string column, size_t index) ColumnToIndexDelegate;

/**
Check if type is a composite.

Composite is a type with static number of fields. These types are:
$(UL
    $(LI Tuples)
    $(LI structs)
    $(LI static arrays)
)
*/
template isCompositeType(T)
{
    import std.datetime : SysTime;
    static if (isTuple!T || (is(T == struct) && !(is(T == SysTime))) || isStaticArray!T)
        enum isCompositeType = true;
    else
        enum isCompositeType = false;
}

deprecated("Please used std.typecons.Nullable instead") template Nullable(T)
    if (!__traits(compiles, { T t = null; }))
{
    /*
    Currently with void*, because otherwise it wont accept nulls.
    VariantN need to be changed to support nulls without using void*, which may
    be a legitimate type to store, as pointed out by Andrei.
    Preferable alias would be then Algebraic!(T, void) or even Algebraic!T, since
    VariantN already may hold "uninitialized state".
    */
    alias Algebraic!(T, void*) Nullable;
}

template isVariantN(T)
{
    //static if (is(T X == VariantN!(N, Types), uint N, Types...)) // doesn't work due to BUG 5784
    static if (T.stringof.length >= 8 && T.stringof[0..8] == "VariantN") // ugly temporary workaround
        enum isVariantN = true;
    else
        enum isVariantN = false;
}

static assert(isVariantN!Variant);
static assert(isVariantN!(Algebraic!(int, string)));
static assert(isVariantN!(Nullable!int));

// an alias is used due to a bug in the compiler not allowing fully
// qualified names in an is expression
private alias Option = std.typecons.Nullable;

template isNullable(T)
{
    static if ((isVariantN!T && T.allowed!(void*)) || is(T X == Nullable!U, U) || is(T == Option!U, U))
        enum isNullable = true;
    else
        enum isNullable = false;
}

static assert(isNullable!Variant);
static assert(isNullable!(Nullable!int));

template nullableTarget(T)
    if (isVariantN!T && T.allowed!(void*))
{
    alias T nullableTarget;
}

template nullableTarget(T : Nullable!U, U)
{
    alias U nullableTarget;
}
