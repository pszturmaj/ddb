
all: basic param

basic: basic.d libddb.a
	dmd -I../source -J./sql basic.d libddb.a

param: param.d libddb.a
	dmd -I../source -J./sql param.d libddb.a

libddb.a: ../source/ddb/db.d ../source/ddb/postgres.d
	dmd -lib -oflibddb.a -debug -g -w -I../source ../source/ddb/db.d ../source/ddb/postgres.d ../source/postgres/db.d

clean:
	-rm basic basic.o param param.o libddb.a
