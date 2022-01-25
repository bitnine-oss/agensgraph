/*-------------------------------------------------------------------------
 *
 * pg_opfamily.h
 *	  definition of the "operator family" system catalog (pg_opfamily)
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_opfamily.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_OPFAMILY_H
#define PG_OPFAMILY_H

#include "catalog/genbki.h"
#include "catalog/pg_opfamily_d.h"

/* ----------------
 *		pg_opfamily definition. cpp turns this into
 *		typedef struct FormData_pg_opfamily
 * ----------------
 */
CATALOG(pg_opfamily,2753,OperatorFamilyRelationId)
{
	Oid			oid;			/* oid */

	/* index access method opfamily is for */
	Oid			opfmethod BKI_LOOKUP(pg_am);

	/* name of this opfamily */
	NameData	opfname;

	/* namespace of this opfamily */
	Oid			opfnamespace BKI_DEFAULT(PGNSP);

	/* opfamily owner */
	Oid			opfowner BKI_DEFAULT(PGUID);
} FormData_pg_opfamily;

/* ----------------
 *		Form_pg_opfamily corresponds to a pointer to a tuple with
 *		the format of pg_opfamily relation.
 * ----------------
 */
typedef FormData_pg_opfamily *Form_pg_opfamily;

#ifdef EXPOSE_TO_CLIENT_CODE

#define IsBooleanOpfamily(opfamily) \
	((opfamily) == BOOL_BTREE_FAM_OID || (opfamily) == BOOL_HASH_FAM_OID)

#endif							/* EXPOSE_TO_CLIENT_CODE */

/* HAMID */
#define GRAPHID_BTREE_FAM_OID 7093
/* Ibrar DATA(insert OID = 7093 (  403 graphid_ops           PGNSP PGUID ));
#define GRAPHID_BTREE_FAM_OID 7093
DATA(insert OID = 7096 (  405 graphid_ops           PGNSP PGUID ));
DATA(insert OID = 7098 ( 2742 graphid_ops           PGNSP PGUID ));
DATA(insert OID = 7105 ( 3580 graphid_minmax_ops    PGNSP PGUID ));

DATA(insert OID = 7106 (  405 vertex_ops            PGNSP PGUID ));

DATA(insert OID = 7167 (  403 rowid_ops             PGNSP PGUID ));
*/
#endif							/* PG_OPFAMILY_H */
