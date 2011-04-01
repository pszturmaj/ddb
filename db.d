module db;

import std.conv, std.traits, std.typecons, std.typetuple, std.variant;

/**
Data row returned from database servers.

DBRow may be instantiated with any number of arguments. It subtypes base type which
depends on that number:

$(TABLE
    $(TR $(TH Number of arguments) $(TH Base type))
    $(TR $(TD 0) $(TD Variant[]))
    $(TR $(TD 1) $(TD Specs itself, more precisely Specs[0]))
    $(TR $(TD >= 2) $(TD Tuple!Specs))
)

Examples:

Default untyped _DBRow:
---
DBRow!() row1;
DBRow!(Variant[]) row2;

assert(is(typeof(row1.base == row2.base)));
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
        alias typeof(T[0]) ElemType;
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
        static pure void checkReceivedFieldCount(int fieldCount)
        {
            if (fieldTypes.length != fieldCount)
                throw new Exception("Received field count is not equal to " ~ T.stringof ~ "'s field count");
        }
    }
    
    string toString()
    {
        return to!string(base);
    }
}

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
    static if (isTuple!T || is(T == struct) || isStaticArray!T)
        enum isCompositeType = true;
    else
        enum isCompositeType = false;
}

template Nullable(T)
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

template isNullable(T)
{
    static if ((isVariantN!T && T.allowed!(void*)) || is(T X == Nullable!U, U))
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