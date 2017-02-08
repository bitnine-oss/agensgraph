#ifndef NODENESTLOOPVLE_H
#define NODENESTLOOPVLE_H

#include "nodes/execnodes.h"

extern NestLoopVLEState *ExecInitNestLoopVLE(NestLoopVLE *node, EState *estate, int eflags);
extern TupleTableSlot *ExecNestLoopVLE(NestLoopVLEState *node);
extern void ExecEndNestLoopVLE(NestLoopVLEState *node);
extern void ExecReScanNestLoopVLE(NestLoopVLEState *node);

#endif   /* NODENESTLOOPVLE_H */
