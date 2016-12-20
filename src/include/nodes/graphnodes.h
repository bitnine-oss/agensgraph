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
	char	   *variable;
	char	   *label;
	bool		create;			/* whether this vertex will be created or not */
} GraphVertex;

#define GRAPH_EDGE_DIR_NONE		0
#define GRAPH_EDGE_DIR_LEFT		(1 << 0)
#define GRAPH_EDGE_DIR_RIGHT	(1 << 1)

typedef struct GraphEdge
{
	NodeTag		type;
	uint32		direction;		/* bitmask of directions (see above) */
	char	   *variable;
	char	   *label;
} GraphEdge;

typedef struct GraphSetProp
{
	NodeTag		type;
	Node	   *elem;			/* expression of vertex/edge */
	Node	   *path;			/* expression of path (text[]) */
	Node	   *expr;			/* expression of value */
	ExprState  *es_elem;
	ExprState  *es_path;
	ExprState  *es_expr;
} GraphSetProp;

#endif	/* GRAPHNODES_H */
