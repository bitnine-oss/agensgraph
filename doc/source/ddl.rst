**********************************
Data Definition Language for Graph 
**********************************

=============
CREATE VLABEL
=============
*Name*

CREATE VLABEL –define a new vertex label

*Synopsis*

.. code-block:: sql

  CREATE VLABEL <vertex_label_name> [ INHERITS ( parent_label_name [, ...] ) ]

*Description*

CREATE VLABEL will create a new, initially empty vertex label in the current database.

A schema name cannot be given when creating a vertex label. It is create in the GRAPH schema always. If a schema name is given(for example, CREATE VLABEL myschema.myvlabel ...) the the query will return a error.

*Parameters*

.. glossary::
 vertex_label_name
  The name of the vertex label to be created.

 INHERITS ( parent_label [, ...] )
  The optional INHERITS clause specifies a list of vertex lables. If it is empty then the new vertex label inherits default vertex label.

  Use of INHERITS create a persistent relationship between the new child label and its parent label(s). The data of the child label is included in scans of the parent(s) by default.


=============
CREATE ELABEL
=============
*Name*

CREATE ELABEL –define a new edge label

*Synopsis*

.. code-block:: sql

  CREATE ELABEL <edge_label_name> [ INHERITS ( parent_label_name [, ...] ) ]

*Description*

.. note:: This contents are almost the same as CREATE VLABEL.

CREATE ELABEL will create a new, initially empty edge label in the current database.

A schema name cannot be given when creating a edge label. It is create in the GRAPH schema always. If a schema name is given(for example, CREATE ELABEL myschema.myelabel ...) the the query will return a error.

*Parameters*

.. glossary::
 edge_label_name
  The name of the edge label to be created.

 INHERITS ( parent_label [, ...] )
  The optional INHERITS clause specifies a list of edge lables. If it is empty then the new edge label inherits default edge label.

  Use of INHERITS create a persistent relationship between the new child label and its parent label(s). The data of the child label is included in scans of the parent(s) by default.


===========
DROP VLABEL
===========
*Name*

DROP VLABEL –remove a vertex label

*Synopsis*

.. code-block:: sql

  DROP VLABEL <vertex_label_name> [, ...] [CASCADE|RESTRICT]

*Description*

Drop VLABEL removes labels from the database.

Drop VLABEL removes constraints the exist for the label. To drop a label that is inherited by another label, CASCADE must be specified.

*Parameters*

.. glossary::
 vertex_label_name
  The name of the label to drop.

 CASCADE
  Automatically drop labels that inheriting the table(child table).

 RESTRICT
  Refuse to drop the label if any child table exist. This is the default.


===========
DROP ELABEL
===========
*Name*

DROP ELABEL –remove a edge label

*Synopsis*

.. code-block:: sql

  DROP ELABEL <edge_label_name> [, ...] [CASCADE|RESTRICT]

*Description*

.. note:: This contents are almost the same as DROP VLABEL.

Drop ELABEL removes labels from the database.

Drop ELABEL removes constraints the exist for the label. To drop a label that is inherited by another label, CASCADE must be specified.

*Parameters*

.. glossary::
 edge_label_name
  The name of the label to drop.

 CASCADE
  Automatically drop labels that inheriting the table(child table).

 RESTRICT
  Refuse to drop the label if any child table exist. This is the default.

