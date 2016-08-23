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

#include "c.h"
#include "nodes/pg_list.h"

typedef struct GraphPath
{
	NodeTag		type;
	char	   *variable;
	List	   *chain;		/* vertex, edge, vertex, ... */
} GraphPath;

typedef struct GraphVertex
{
	NodeTag		type;
	char	   *variable;
	char	   *label;
	char	   *prop_map;	/* JSON object string */
	bool		create;		/* whether this vertex will be created or not */
} GraphVertex;

#define GRAPH_EDGE_DIR_NONE		0
#define GRAPH_EDGE_DIR_LEFT		(1 << 0)
#define GRAPH_EDGE_DIR_RIGHT	(1 << 1)

typedef struct GraphEdge
{
	NodeTag		type;
	uint32		direction;	/* bitmask of directions (see above) */
	char	   *variable;
	char	   *label;
	char	   *prop_map;	/* JSON object string */
} GraphEdge;

#endif	/* GRAPHNODES_H */
