/*-------------------------------------------------------------------------
 *
 * pg_amproc.h
 *	  definition of the "access method procedure" system catalog (pg_amproc)
 *
 * The amproc table identifies support procedures associated with index
 * operator families and classes.  These procedures can't be listed in pg_amop
 * since they are not the implementation of any indexable operator.
 *
 * The primary key for this table is <amprocfamily, amproclefttype,
 * amprocrighttype, amprocnum>.  The "default" support functions for a
 * particular opclass within the family are those with amproclefttype =
 * amprocrighttype = opclass's opcintype.  These are the ones loaded into the
 * relcache for an index and typically used for internal index operations.
 * Other support functions are typically used to handle cross-type indexable
 * operators with oprleft/oprright matching the entry's amproclefttype and
 * amprocrighttype. The exact behavior depends on the index AM, however, and
 * some don't pay attention to non-default functions at all.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_amproc.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AMPROC_H
#define PG_AMPROC_H

#include "catalog/genbki.h"
#include "catalog/pg_amproc_d.h"

/* ----------------
 *		pg_amproc definition.  cpp turns this into
 *		typedef struct FormData_pg_amproc
 * ----------------
 */
CATALOG(pg_amproc,2603,AccessMethodProcedureRelationId)
{
	Oid			oid;			/* oid */

	/* the index opfamily this entry is for */
	Oid			amprocfamily BKI_LOOKUP(pg_opfamily);

	/* procedure's left input data type */
	Oid			amproclefttype BKI_LOOKUP(pg_type);

	/* procedure's right input data type */
	Oid			amprocrighttype BKI_LOOKUP(pg_type);

	/* support procedure index */
	int16		amprocnum;

	/* OID of the proc */
	regproc		amproc BKI_LOOKUP(pg_proc);
} FormData_pg_amproc;

/* ----------------
 *		Form_pg_amproc corresponds to a pointer to a tuple with
 *		the format of pg_amproc relation.
 * ----------------
 */
typedef FormData_pg_amproc *Form_pg_amproc;

/*
 * graphid_ops
 */
/* BTree */
//Ibrar DATA(insert ( 7093 7002 7002  1 7094 ));
/* Hash */
//DATA(insert ( 7096 7002 7002  1 7097 ));
/* GIN (as BTree) */
//DATA(insert ( 7098 7002 7002  1 7094 ));
//DATA(insert ( 7098 7002 7002  2 7100 ));
//DATA(insert ( 7098 7002 7002  3 7101 ));
//DATA(insert ( 7098 7002 7002  4 7102 ));
//DATA(insert ( 7098 7002 7002  5 7103 ));
/* BRIN (minmax) */
//DATA(insert ( 7105 7002 7002  1 3383 ));
//DATA(insert ( 7105 7002 7002  2 3384 ));
//DATA(insert ( 7105 7002 7002  3 3385 ));
//DATA(insert ( 7105 7002 7002  4 3386 ));

/*
 * graphid_ops
 */
/* Hash */
//DATA(insert ( 7106 7012 7012  1 7107 ));

/*
 * rowid_ops
 */
/* BTree */
//DATA(insert ( 7167 7062 7062  1 7168 ));

#endif							/* PG_AMPROC_H */
