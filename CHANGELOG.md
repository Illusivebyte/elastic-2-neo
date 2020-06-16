# Change Log

### 06/16/2020 0.0.3a
- Updated project structure
- Fixed error in code relating to destination iterative nodes

### 04/04/2020 0.0.2a 
- Added -e "end after empty" command that will cause the program to end execution
after the elastic index is exhausted
- Fixed errors when nodes and relationships did not have any properties
- Added "uniqueProperties" to relationships
- Added "unique" property to designate relationships without properties that need
to be completely unique


### 03/31/2020 0.0.1a 
- Added iterator node and relationship types
- Added new mapping file fields to support iterator and standard types
- Updated README with simple mapping file walk through 
- elastic2neo no longer tries to connect to a Neo4j database when using the -n flag
- Added support for sub-document value reference
