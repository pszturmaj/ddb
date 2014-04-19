
test: libddb.a
	dmd -I../source -J./sql test.d libddb.a

libddb.a:
	dmd -lib -oflibddb.a -debug -g -w -I../source ../source/ddb/db.d ../source/ddb/postgres.d ../source/postgres/db.d

clean:
	-rm test test.o libddb.a
