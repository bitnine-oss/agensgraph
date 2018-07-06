/*
 * graph.h
 *	  definitions for graph nodes
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/nodes/graphnodes.h
 */

#ifndef GRAPHNODES_H
#define GRAPHNODES_H

#include "nodes/execnodes.h"

typedef struct GraphPath
{
	NodeTag		type;
	char	   *variable;
	List	   *chain;			/* vertex, edge, vertex, ... */
} GraphPath;

typedef struct GraphVertex
{
	NodeTag		type;
	AttrNumber	resno;
	bool		create;			/* whether this vertex will be created or not */
	Oid			relid;
	Node	   *expr;
	ExprState  *es_expr;
} GraphVertex;

#define GRAPH_EDGE_DIR_NONE		0
#define GRAPH_EDGE_DIR_LEFT		(1 << 0)
#define GRAPH_EDGE_DIR_RIGHT	(1 << 1)

typedef struct GraphEdge
{
	NodeTag		type;
	uint32		direction;		/* bitmask of directions (see above) */
	AttrNumber	resno;
	Oid			relid;
	Node	   *expr;
	ExprState  *es_expr;
} GraphEdge;

typedef enum GSPKind
{
	GSP_NORMAL,
	GSP_ON_CREATE,
	GSP_ON_MATCH
} GSPKind;

typedef struct GraphSetProp
{
	NodeTag		type;
	GSPKind		kind;
	char	   *variable;
	Node	   *elem;			/* expression of vertex/edge */
	Node	   *expr;			/* expression of value */
	ExprState  *es_elem;
	ExprState  *es_expr;
} GraphSetProp;

typedef struct GraphDelElem
{
	NodeTag		type;
	char	   *variable;
	Node	   *elem;
	ExprState  *es_elem;
} GraphDelElem;

#endif	/* GRAPHNODES_H */
