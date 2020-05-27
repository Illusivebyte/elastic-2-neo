# Elastic2Neo
**Thank you for using Elastic2Neo!**    

Elastic2Neo is tool for populating a Neo4j graph database with information pulled from an Elasticsearch database. This 
tool will scroll a specified index in an Elasticsearch database and then transform that data to a node and relationship
format specified in a simple yaml based mapping file.  

This tool also provides users with the ability to conduct additional data processing in the form of pre-processing, 
post node processing, and post relationship processing modules. These modules can be built to adjust data formatting, 
adjust node properties and labels, or adjust relationship types and properties.

After the processing of all the nodes and relationships is complete, Neo4j Cypher statements are generated and executed
against the Neo4j database. Then using a Neo4j viewer of your choice you can discover new patterns in your data that may
not have been easily visible just using Elasticsearch and Kibana! Enjoy!   


## Usage
    elastic2neo.py [-d] [-f [-F LogFile]] [-C ConfigFile] [-M MappingFile] [-o] [-e] [-n] [-h]  
### Options
**-d** ***Enable debug messages***   
**-f** ***Enable logging to file***  
**-F (LogFile)** ***Specify log file (requires -f)***  
**-C (Config)** ***Specify config file***  
**-M (Mapping)** ***Specify mapping file***  
**-o** ***Execute Elasticsearch scroll  once***
**-e** ***End execution after the Elasticsearch index is empty***             
**-n** ***Do not execute cypher statements (for debugging)***  
**-h** ***View the usage syntax***
           

### Defaults
Unless specified using an option Elastic2Neo will look for the following files in the directory it is run from:  
1. config.yaml
2. mapping.yaml
## Configuration
Elastic2Neo uses a configuration file in order to get the required information to connect to both the Elasticsearch 
database and the Neo4j database. This file is in YAML format and needs to contain a number of required fields. The top
level required keys include:  
1. elastic
2. neo

Within each of these values a number of additional keys are required.  
### elastic  
#### required
1. **host : string** ***The IP or Domain that has the Elasticsearch***
2. **port : number** ***The port number for Elasticsearch***
3. **protocol : string** ***The protocol used for Elasticsearch (http/https)***
4. **scrollSize : number** ***The number of documents to scroll each time***
5. **sleepMin : number** ***The amount of time in minutes to wait if zero documents are returned***
#### optional
1. **user : string** ***If basic http authentication is needed provide the username***
2. **password : string** ***If basic http authentication is needed provide the password***

### neo
#### required
1. **host : string** ***The host IP or domain for Neo4j***
2. **port : number** ***The port number for Neo4j***
3. **protocol : string** ***The protocol to be used for Neo4j (bolt, http, https)***
4. **user : string** ***Provide the username for the Neo4j database***
5. **password : string** ***provide the password for the Neo4j database***

### Config.yaml Example
    elastic:
        host: "localhost"
        port: 9200
        protocol: "https"
        scrollSize: 1000
        sleepMin: 15
        user: "test"
        password: "Password"
    neo:
        host: "localhost"
        port: 7687
        protocol: "bolt"
        user: "test"
        password: "Password"

## Mapping an Index
One of the most important parts of the data conversion processes is the development of the mapping file. The mapping
file provides the basic template of how each individual document in an index will translate into nodes and 
relationships. Developing an effective mapping file requires an intimate knowledge of the data you are processing, 
so take some time to get to know your data before starting.  

### Mapping File Structure
just like the config file, the mapping file is in YAML format. Each mapping file is required to have several keys
including:  
1. **index: string** ***The name of the index to scroll***
2. **docType: string** ***The document type***
3. **nodes: list** ***The list of nodes to be generated from a single document***
4. **relationships: list** ***The list of relationships to be generated from a single document***

Both the nodes list and the relationships list have specific formatting that is required for each item in the list.
#### Nodes (Think Nouns)
1. **id: string** ***The string identifier used to build the relationship*** 
2. **nodeType: string** ***Indicates the type of node (standard | iterator)***
3. **required: bool** ***Bool indicating if this node has to be created for the document to be valid***
4. **labels: list** ***A list of strings that are the labels for the node***
5. **iterator: string** ***The key for the document list item that will be iterated on***
6. **properties: dictionary** ***A dictionary containing the names of the properties that each have a 
dictionary value of of key : string (the key to look for in the elastic document or the "ITER!" reserved word in the 
case of iterator nodes) and the type: string (the type of value stored in the elastic document, in the case of iterator 
nodes it is the type of item contained in the list)***
7. **uniqueLabels: list** ***A list of strings that specify which labels will be used during the node "MERGE" process
 (should only be values from the labels list above)***
8. **uniqueProperties: list** ***A list of strings that specify which properties are used during the node "MERGE" process
 (should only be the names of values in the properties dictionary)***
9. **requiredProperties: list** ***A list of strings that specify which properties must be present for a node to be
 created (should only be the names of values in the properties dictionary)***

#### Relationships (Think Verbs)
1. **type: string** ***The Neo4j type of relationship (per Neo4j standards should be uppercase with 
underscores)***
2. **required: bool** ***Indicates if this relationship must be generated in order for a document to be valid***
1. **relationshipType: string** ***NOT to be confused with "type", this refers to the the elastic2neo relationship type 
(standard | iterator)***
2. **directionality: string** ***The direction of the relationship from sourceNode to Destination node (< | >)***
3. **sourceNode: number** ***The index of the source node in the nodes list***
4. **destinationNode: number** ***The index of the destination node in the nodes list***
5. **properties: list** ***A dictionary containing the names of the properties that each have a 
dictionary value of of key : string (the key to look for in the elastic document) and the type: string (the type of 
value stored in the elastic document)***
6. **requiredProperties: list** ***A list of strings that specify which properties must be present for a relationship 
to be created (should only be the names of values in the properties dictionary)***
7. **uniqueProperties: list** ***A list of strings that specify which properties are used during the relationship 
"MERGE" process (should only be the names of values in the properties dictionary)***
8. **unique: bool** ***Used for relationships without properties that must be unique, causes a "MERGE" statement instead
of a "CREATE" statement***

### Types: Standard vs. Iterator 
Standard nodes and relationships are generated individually based on the input provided in the mapping file. Iterator 
nodes and relationships generate a dynamic number of similar nodes and relationships based on the number of values in a 
specified list item. 

### Mapping File Example:
    doc: {fname: "john", lname: "Doe", cars: ["civic", "tacoma", "jeep"], spouse: {fname: "jane", lname: "Doe"}, 
    marriedOn: 1/1/1960}
If I wanted to add the following document to the Neo4j database I would start with the making standard node.  

    nodes:
    # Basic Person Node
      - id: "person"
        nodeType: "standard"
        required: True
        labels:
          - "person"
        properties:
          fname:
            key: "fname"
            type: "string"
          lname:
            key: "lname"
            type: "string"
        uniqueLabels:
          - "person"
        uniqueProperties:
          - "fname"
          - "lname
        requiredProperties:
          - "fname"
          - "lname"

This node will represent "John Doe" on the graph. This particular node will have a label "person" and will have 
properties "fname" and "lname". The node is a required node, so in the instance that a required property cannot be found 
(in this instance "fname" and "lname") the document will be discarded and no nodes or relationships will be generated.

There is more information in this document that has not been represented on the graph. Lets generate a second standard 
node for representing a spouse.   
 
        # Spouse Node
          - id: "spouse"
            nodeType: "standard"
            required: True
            labels:
              - "person"
              - "spouse"
            properties:
              fname:
                key: "spouse.fname"
                type: "string"
              lname:
                key: "spouse.lname"
                type: "string"
            uniqueLabels:
              - "person"
            uniqueProperties:
              - "fname"
              - "lname
            requiredProperties:
              - "fname"
              - "lname"
This node is very similar to our first, although there are a few differences. First we have updated the "id" to a unique 
name. We have also have added an additional label to the node, "spouse". This might be useful later when looking at the 
data in the Neo4j database and adds some additional context. Finally we have updated "key" values in the properties to 
"spouse.fname" and "spouse.lname". This change indicates that the indicated keys are in the "spouse" object in the 
document above.

Now that we have captured the "person" data points within the document, there is one other field that seems to contain 
something worthy of being a "node". That field is the "cars" field. In this case a standard node would not work since 
"cars" is a list that can change with every new document processed. This is the perfect time for an "iterator" node.

        # Car Node
          - id: "car"
            nodeType: "iterator"
            required: False
            labels:
              - "car"
            iterator: "cars"
            properties:
              model:
                key: "ITER!"
                type: "string"
            uniqueLabels:
              - "model"
            uniqueProperties:
              - "model"
            requiredProperties:
              - "model"

This node looks a bit different than the person nodes. The most important changes are to the "nodeType", "iterator", and 
"properties". The nodeType has changed to "iterator" to reflect that a node will be generated for every value of a 
specified field. We specified the field using "iterator" which shows that we want to use the "cars" list in the document.
Finally we utilized each value of the list with the "model" property in "properties". The "ITER!" reserved word indicates
that the currently iterator value should be placed as the value.

Now we have created nodes for all of our nouns. Lets make some relationships to show how they are all connected together!
We will start with a simple relationship between them.  

     # MARRIED_TO relationship
      - type: "MARRIED_TO"
        relationshipType: "standard"
        required: True
        directionality: ">"
        sourceNode: "person"
        destinationNode: "spouse"
        properties:
          dateMarried:
            key: "marriedOn"
            type: "datetime"
        requiredProperties:
          - "dateMarried"
        uniqueProperties:
          - "dateMarried"

This relationship describes that our person node is married to our spouse node. Neo4j utilized the "type" field to define
the relationship between the nodes. "relationshipType" is a field specific to this tool and it currently indicates that
this will be a standard relationship. The relationship is required, and its directionality indicates that it will go from
the sourceNode to the destinationNode (Think of it like an arrow pointing!). "sourceNode" contains the "id" of the person
node and "destinationNode" has the "id" of our spouse node. Relationships like nodes can have properties, in this instance
we want to add the "marriedOn" datetime field as a property that we will rename "dateMarried". Finally set the 
"dateMarried" property as a "requiredProperty" and a "uniqueProperty" to ensure tha the relationship is valid and unique.
Now for this example we will continue on but if you so desired you could write the inverse relationship as a 
second "standard" relationship!  

Now we have our person nodes connected, lets show an iterator relationship between John Doe and his cars.  

     # DRIVES_A Relationship
      - type: "DRIVES_A"
        relationshipType: "iterator"
        required: False
        directionality: ">"
        sourceNode: "person"
        destinationNode: "car"
        unique: True

Now as you can see there were not to many drastic changes required to make an iterator relationship! By changing the 
"relationshipType" and supplying one iterator node we have now ensured that a "DRIVES_A" relationship is created for 
every car a person node owns regardless of how many are put into the list! Now since this relationship does not have 
 properties we can use the "unique" key to indicate that there should only be one of this type of relationship between 
 these two nodes. (Note: while this example does not have properties, they can be added like any other relationship)

So that is it! You have now generated a simple mapping file that would be able to parse and convert and entire index of 
similar documents. This tutorial should get you enough to get started with some of the basic feature of this framework. 
Try it out on your own data and see what new insights you can gain!


## Pre/Post Processing Data
Elastic2Neo currently provides users with the ability to manipulate each elastic document, node, and relationship before
the are converted into Cypher statements and executed against the database. This allows users to adjust data types and 
formats, add additional contextual information, remove misleading data, etc. before and after the information is
processed into nodes and relationships.

### Data Pipeline
1. Elastic Document In
2. Pre-Processors
3. Node Generation
4. Post-Node Processors
5. Relationship Generation
6. Post-Relationship Processors
7. Cypher Statement Generation

### Building a Processor
Building and using a processor a basic processor is trivial. Simply create a new .py script that contains the function
for the level of processing you wish to do. Write your logic to manipulate the data, and then return all the data when
you are done. 

**WARNING:** ***Any data that is not returned from your function will not be processed by the rest of the 
pipeline.***  

Place your new processor script in the processors directory and the next time Elastic2Neo is run your 
processor will be executed. ***Note: A single script can contain all three processor functions***    
 
#### Pre-Processor
    def pre_process_doc(doc):
        # Logic
        return doc
#### Post-Node Processor
    def post_process_nodes(nodes):
        # Logic
        return nodes
#### Post-Relationship Processor
    def post_process_relationships(relationships):
        # Logic
        return relationships