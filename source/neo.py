from neo4j import GraphDatabase
import logging
from os import listdir
from os.path import isfile, join
from importlib.machinery import SourceFileLoader
from copy import deepcopy

# Load up the overall module logger
module_logger = logging.getLogger('elastic2neo.neo')
module_logger.debug("module loaded")

# Not currently implemented
REQUIRED_MAPPING_KEYS = ["index", "docType", "nodes", "relationships"]
MAPPING_KEY_VALUE_TYPES = {"index": str, "docType": str, "nodes": list, "relationships": list}
REQUIRED_NODE_KEYS = ["labels", "nodeType", "required", "nodeType"]
REQUIRED_RELATIONSHIP_KEYS = ["type", "directionality", "sourceNode", "destinationNode", "required", "relationshipType"]

# Lists of functions required to be in processing modules
REQUIRED_PRE_FUNC = ['pre_process_doc']
REQUIRED_POST_NODE_FUNC = ['post_process_nodes']
REQUIRED_POST_RELATIONSHIP_FUNC = ['post_process_relationships']


class GraphBuilder:
    def __init__(self, uri, user, password, mapping, pre=True, post_node=True, post_relationship=True, execute=True):
        """
        A GraphBuilding class for generating and executing Cypher statements based on Elasticsearch documents.
        :param uri: URI of the Neo4j serer (include protocol and port e.g. bolt://localhost:7687 )
        :param user: user to login with
        :param password: password of the user
        :param mapping: mapping as a dictionary
        :param pre: should pre-processing be done?
        :param post_node: should post node processing be done?
        :param post_relationship: should post relationship processing be done?
        """
        self._logger = logging.getLogger('elastic2neo.neo.GraphBuilder')
        self._driver = None
        if execute:
            self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._mapping = mapping
        self._pre_modules = list()
        self._post_node_modules = list()
        self._post_relationship_modules = list()
        self._load_additional_processing_modules(pre, post_node, post_relationship)

    def close(self):
        """
        Properly close the Neo4j driver.
        """
        if self._driver:
            self._driver.close()

    def build(self, data, execute=True):
        """
        Build out the graph based on the provided elastic data.
        :param data: The elastic data
        :param execute: Should statements be executed against database? (False for debugging purposes)
        """
        nodes, relationships = self._process(data)
        node_statements, relationship_statements = self._gen_statements(nodes, relationships)
        if execute:
            self._execute_statements(node_statements, relationship_statements)

    def _execute_statements(self, node_statements, relationship_statements):
        """
        Executes the provided node and relationship generation statements.
        :param node_statements: list of node statements
        :param relationship_statements: list of relationship statements
        """
        self._logger.info("executing {} node statements and {} relationship statements against database".format(
            len(node_statements), len(relationship_statements)))
        with self._driver.session() as session:
            for statement in node_statements:
                self._logger.debug("executing statement: {}".format(statement))
                result = session.write_transaction(self._run_statement, statement)
                self._logger.debug('execution result: {}'.format(result))
            for statement in relationship_statements:
                self._logger.debug("executing statement: {}".format(statement))
                result = session.write_transaction(self._run_statement, statement)
                self._logger.debug('execution result: {}'.format(result))

    @staticmethod
    def _run_statement(tx, statement):
        """
        Execute the given statement against the Neo4j database.
        :param tx: function
        :param statement: Cypher statement string
        :return: results
        """
        result = tx.run(statement)
        return result.single()

    def _gen_statements(self, nodes, relationships):
        """
        Generates the proper Cypher CREATE and MERGE statements for the given nodes and relationships.
        :param nodes: The list of nodes
        :param relationships: The list of relationships
        :return: a tuple containing a list of node statements and a list of relationship statements
        """
        self._logger.debug("generating statements")
        node_statements = self._gen_node_statements(nodes)
        relationship_statements = self._gen_relationship_statements(relationships)
        return node_statements, relationship_statements

    def _gen_node_statements(self, nodes):
        """
        Break out function for generating node statements.
        :param nodes: list of nodes
        :return: list of node statements
        """
        node_statements = list()
        for node in nodes:
            if node['nodeType'] == "iterator":
                statements = self._gen_iterator_node_statements(node)
            else:
                statements = self._gen_standard_node_statements(node)
            for statement in statements:
                self._logger.debug("created node statement: {}".format(statement))
                node_statements.append(statement)
        return node_statements

    def _gen_standard_node_statements(self, node):
        """
        Generates a standard node statement based on the provided node.
        :param node: standard node
        :return: list of statements
        """
        not_in_unique = list()
        if 'uniqueProperties' in node:
            if 'uniqueLabels' in node:
                not_in_unique = GraphBuilder._get_missing_labels(node['labels'], node['uniqueLabels'])
                statement = "MERGE (n{}".format(self._gen_label_string(node['uniqueLabels']))
            else:
                statement = "MERGE (n{}".format(self._gen_label_string(node['labels']))
            statement += self._gen_properties_string(node['uniqueProperties']) + ")"
            need_to_set = dict()
            for prop in node['properties']:
                if prop not in node['uniqueProperties']:
                    need_to_set[prop] = node['properties'][prop]
            if len(need_to_set) > 0:
                statement += self._gen_properties_string(need_to_set, dict_style=False)
            if len(not_in_unique) > 0:
                if len(need_to_set) > 0:
                    statement += ", n{}".format(self._gen_label_string(not_in_unique))
                else:
                    statement += "SET n{}".format(self._gen_label_string(not_in_unique))
        else:
            if 'uniqueLabels' in node:
                not_in_unique = GraphBuilder._get_missing_labels(node['labels'], node['uniqueLabels'])
                statement = "MERGE (n{}".format(self._gen_label_string(node['uniqueLabels']))
            else:
                statement = "CREATE (n{}".format(self._gen_label_string(node['labels']))
            if 'properties' in node:
                statement += self._gen_properties_string(node['properties'])
            statement += ")"
            if len(not_in_unique) > 0:
                statement += "SET n{}".format(self._gen_label_string(not_in_unique))
        return [statement]

    def _gen_iterator_node_statements(self, node):
        """
        Generates iterator node statements based on the provided node.
        :param node: iterator node
        :return: list of statements
        """
        statements = list()
        for instance in node['instances']:
            not_in_unique = list()
            if 'uniqueProperties' in instance:
                if 'uniqueLabels' in instance:
                    not_in_unique = GraphBuilder._get_missing_labels(instance['labels'], instance['uniqueLabels'])
                    statement = "MERGE (n{}".format(self._gen_label_string(instance['uniqueLabels']))
                else:
                    statement = "MERGE (n{}".format(self._gen_label_string(instance['labels']))
                if 'uniqueProperties' in instance:
                    statement += self._gen_properties_string(instance['uniqueProperties']) + ")"
                need_to_set = dict()
                for prop in instance['properties']:
                    if prop not in instance['uniqueProperties']:
                        need_to_set[prop] = instance['uniqueProperties'][prop]
                if len(need_to_set) > 0:
                    statement += self._gen_properties_string(need_to_set, dict_style=False)
                if len(not_in_unique) > 0:
                    if len(need_to_set) > 0:
                        statement += ", n{}".format(self._gen_label_string(not_in_unique))
                    else:
                        statement += "SET n{}".format(self._gen_label_string(not_in_unique))
                statements.append(statement)
            else:
                if "uniqueLabels" in instance:
                    not_in_unique = GraphBuilder._get_missing_labels(instance['labels'], instance['uniqueLabels'])
                    statement = "MERGE (n{}".format(self._gen_label_string(instance['uniqueLabels']))
                else:
                    statement = "CREATE (n{}".format(self._gen_label_string(instance['labels']))
                if 'properties' in instance:
                    statement += self._gen_properties_string(instance['properties'])
                statement += ")"
                if len(not_in_unique) > 0:
                    statement += "SET n{}".format(self._gen_label_string(not_in_unique))
                statements.append(statement)
        return statements

    @staticmethod
    def _get_missing_labels(labels, unique_labels):
        """
        Simple key check for finding missing labels.
        param: labels: A list of labels to check for
        param: unique_labels: A list of labels to check against
        return: a list of labels that are in labels but not unique_labels
        """
        not_in_unique = list()
        for label in labels:
            if label not in unique_labels:
                not_in_unique.append(label)
        return not_in_unique

    def _gen_relationship_statements(self, relationships):
        """
        Break out function for generating relationship statements.
        :param relationships: list of relationships
        :return: list of relationship statements
        """
        relationship_statements = list()
        for relationship in relationships:
            if relationship['relationshipType'] == "iterator":
                statements = self._gen_iterative_relationship_statements(relationship)
            else:
                statements = self._gen_standard_relationship_statements(relationship)
            for statement in statements:
                self._logger.debug("created relationship statement: {}".format(statement))
                relationship_statements.append(statement)
        return relationship_statements

    def _gen_standard_relationship_statements(self, relationship):
        """
        Generates a standard relationship statement based on the provided relationship.
        :param relationship: a standard relationship
        :return: list of relationship statements
        """
        statement = "MATCH "
        if 'uniqueLabels' in relationship['sourceNode']:
            statement += "(s{}), ".format(self._gen_label_string(relationship["sourceNode"]["uniqueLabels"]))
        else:
            statement += "(s{}), ".format(self._gen_label_string(relationship["sourceNode"]["labels"]))
        if 'uniqueLabels' in relationship['destinationNode']:
            statement += "(d{}) ".format(self._gen_label_string(relationship["destinationNode"]["uniqueLabels"]))
        else:
            statement += "(d{}) ".format(self._gen_label_string(relationship["destinationNode"]["labels"]))
        first_has_props = False
        if 'uniqueProperties' in relationship['sourceNode']:
            statement += "{}".format(self._gen_properties_string(relationship["sourceNode"]["uniqueProperties"],
                                                                 dict_style=False, match_logic=True, variable="s"))
            first_has_props = True
        elif 'properties' in relationship['sourceNode']:
            statement += "{}".format(self._gen_properties_string(relationship["sourceNode"]["properties"],
                                                                 dict_style=False, match_logic=True, variable="s"))
            first_has_props = True
        if first_has_props:
            statement += " AND "
        if 'uniqueProperties' in relationship['destinationNode']:
            statement += "{}".format(self._gen_properties_string(relationship["destinationNode"]["properties"],
                                                                 dict_style=False, match_logic=True, variable="d",
                                                                 opening_statement=not first_has_props))
        elif 'properties' in relationship['destinationNode']:
            statement += "{}".format(self._gen_properties_string(relationship["destinationNode"]["properties"],
                                                                 dict_style=False, match_logic=True, variable="d",
                                                                 opening_statement=not first_has_props))
        unique = True if ('unique' in relationship and relationship['unique']) or 'uniqueProperties' in relationship \
            else False
        if unique:
            statement += " MERGE (s)"
        else:
            statement += " CREATE (s)"
        need_to_set = dict()
        if relationship["directionality"] == ">":
            if unique and 'uniqueProperties' in relationship:
                need_to_set = GraphBuilder._get_missing_props(relationship['properties'],
                                                              relationship['uniqueProperties'])
                statement += "-[r:{}".format(relationship["type"])
                statement += "{}]->(d)".format(self._gen_properties_string(relationship['uniqueProperties']))
            else:
                statement += "-[r:{}]->(d)".format(relationship["type"])
        else:
            if unique and 'uniqueProperties' in relationship:
                need_to_set = GraphBuilder._get_missing_props(relationship['properties'],
                                                              relationship['uniqueProperties'])
                statement += "<-[r:{}".format(relationship["type"])
                statement += "{}]-(d)".format(self._gen_properties_string(relationship['uniqueProperties']))
            else:
                statement += "<-[r:{}]-(d)".format(relationship["type"])
        if len(need_to_set.keys()) > 0:
            statement += "{}".format(self._gen_properties_string(need_to_set, dict_style=False,
                                                                 variable="r"))
        elif not unique and 'properties' in relationship:
            statement += "{}".format(self._gen_properties_string(relationship['properties'], dict_style=False,
                                                                 variable="r"))
        return [statement]

    def _gen_iterative_relationship_statements(self, relationship):
        """
        Generates iterator relationship statements based on the provided relationship.
        :param relationship: an iterator relationship
        :return: list of relationship statements
        """
        statements = list()
        if relationship["sourceNode"]["nodeType"] == "iterator":
            for node_instance, rel_instance in zip(relationship['sourceNode']['instances'], relationship['instances']):
                statement = "MATCH "
                if 'uniqueLabels' in node_instance:
                    statement += "(s{}), ".format(self._gen_label_string(node_instance["uniqueLabels"]))
                else:
                    statement += "(s{}), ".format(self._gen_label_string(node_instance["labels"]))
                if 'uniqueLabels' in relationship['destinationNode']:
                    statement += "(d{}) ".format(
                        self._gen_label_string(relationship["destinationNode"]["uniqueLabels"]))
                else:
                    statement += "(d{}) ".format(self._gen_label_string(relationship["destinationNode"]["labels"]))
                first_has_props = False
                if 'uniqueProperties' in node_instance:
                    statement += "{}".format(self._gen_properties_string(node_instance["uniqueProperties"],
                                                                         dict_style=False, match_logic=True,
                                                                         variable="s"))
                    first_has_props = True
                elif 'properties' in node_instance:
                    statement += "{}".format(self._gen_properties_string(node_instance["properties"],
                                                                         dict_style=False, match_logic=True,
                                                                         variable="s"))
                    first_has_props = True
                if first_has_props:
                    statement += " AND "
                if 'uniqueProperties' in relationship['destinationNode']:
                    statement += "{}".format(self._gen_properties_string(relationship["destinationNode"]
                                                                         ["properties"],
                                                                         dict_style=False, match_logic=True,
                                                                         variable="d",
                                                                         opening_statement=not first_has_props))
                elif 'properties' in relationship['destinationNode']:
                    statement += "{}".format(self._gen_properties_string(relationship["destinationNode"]
                                                                         ["properties"],
                                                                         dict_style=False, match_logic=True,
                                                                         variable="d",
                                                                         opening_statement=not first_has_props))
                unique = True if ('unique' in rel_instance and rel_instance[
                    'unique']) or 'uniqueProperties' in rel_instance \
                    else False
                if unique:
                    statement += " MERGE (s)"
                else:
                    statement += " CREATE (s)"
                need_to_set = dict()
                if rel_instance["directionality"] == ">":
                    if unique and 'uniqueProperties' in rel_instance:
                        need_to_set = GraphBuilder._get_missing_props(rel_instance['properties'],
                                                                      rel_instance['uniqueProperties'])
                        statement += "-[r:{}".format(rel_instance["type"])
                        statement += "{}]->(d)".format(self._gen_properties_string(rel_instance['uniqueProperties']))
                    else:
                        statement += "-[r:{}]->(d)".format(rel_instance["type"])
                else:
                    if unique and 'uniqueProperties' in rel_instance:
                        need_to_set = GraphBuilder._get_missing_props(rel_instance['properties'],
                                                                      rel_instance['uniqueProperties'])
                        statement += "<-[r:{}".format(rel_instance["type"])
                        statement += "{}]-(d)".format(self._gen_properties_string(rel_instance['uniqueProperties']))
                    else:
                        statement += "<-[r:{}]-(d)".format(rel_instance["type"])
                if len(need_to_set.keys()) > 0:
                    statement += "{}".format(self._gen_properties_string(need_to_set, dict_style=False,
                                                                         variable="r"))
                elif not unique and 'properties' in rel_instance:
                    statement += "{}".format(self._gen_properties_string(rel_instance['properties'], dict_style=False,
                                                                         variable="r"))
                statements.append(statement)
        else:
            for node_instance, rel_instance in zip(relationship['destinationNode']['instances'],
                                                   relationship['instances']):
                statement = "MATCH "
                if 'uniqueLabels' in relationship['sourceNode']:
                    statement += "(s{}), ".format(self._gen_label_string(relationship["sourceNode"]["uniqueLabels"]))
                else:
                    statement += "(s{}), ".format(self._gen_label_string(relationship["sourceNode"]["labels"]))
                if 'uniqueLabels' in node_instance:
                    statement += "(d{}) ".format(
                        self._gen_label_string(node_instance["uniqueLabels"]))
                else:
                    statement += "(d{}) ".format(self._gen_label_string(node_instance["labels"]))
                first_has_props = False
                if 'uniqueProperties' in relationship["sourceNode"]:
                    statement += "{}".format(self._gen_properties_string(relationship["sourceNode"]["uniqueProperties"],
                                                                         dict_style=False, match_logic=True,
                                                                         variable="s"))
                    first_has_props = True
                elif 'properties' in relationship["sourceNode"]:
                    statement += "{}".format(self._gen_properties_string(relationship["sourceNode"]["properties"],
                                                                         dict_style=False, match_logic=True,
                                                                         variable="s"))
                    first_has_props = True
                if first_has_props:
                    statement += " AND "
                if 'uniqueProperties' in node_instance:
                    statement += "{}".format(self._gen_properties_string(node_instance["properties"],
                                                                         dict_style=False, match_logic=True,
                                                                         variable="d",
                                                                         opening_statement=not first_has_props))
                elif 'properties' in node_instance:
                    statement += "{}".format(self._gen_properties_string(node_instance["properties"],
                                                                         dict_style=False, match_logic=True,
                                                                         variable="d",
                                                                         opening_statement=not first_has_props))
                unique = True if ('unique' in rel_instance and rel_instance[
                    'unique']) or 'uniqueProperties' in rel_instance \
                    else False
                if unique:
                    statement += " MERGE (s)"
                else:
                    statement += " CREATE (s)"
                need_to_set = dict()
                if rel_instance["directionality"] == ">":
                    if unique and 'uniqueProperties' in rel_instance:
                        need_to_set = GraphBuilder._get_missing_props(rel_instance['properties'],
                                                                      rel_instance['uniqueProperties'])
                        statement += "-[r:{}".format(rel_instance["type"])
                        statement += "{}]->(d)".format(self._gen_properties_string(rel_instance['uniqueProperties']))
                    else:
                        statement += "-[r:{}]->(d)".format(rel_instance["type"])
                else:
                    if unique and 'uniqueProperties' in rel_instance:
                        need_to_set = GraphBuilder._get_missing_props(rel_instance['properties'],
                                                                      rel_instance['uniqueProperties'])
                        statement += "<-[r:{}".format(rel_instance["type"])
                        statement += "{}]-(d)".format(self._gen_properties_string(rel_instance['uniqueProperties']))
                    else:
                        statement += "<-[r:{}]-(d)".format(rel_instance["type"])
                if len(need_to_set.keys()) > 0:
                    statement += "{}".format(self._gen_properties_string(need_to_set, dict_style=False,
                                                                         variable="r"))
                elif not unique and 'properties' in rel_instance:
                    statement += "{}".format(self._gen_properties_string(rel_instance['properties'], dict_style=False,
                                                                         variable="r"))
                statements.append(statement)
        return statements

    @staticmethod
    def _get_missing_props(properties, unique_properties):
        need_to_set = dict()
        for prop in properties:
            if prop not in unique_properties:
                need_to_set[prop] = properties[prop]
        return need_to_set

    @staticmethod
    def _gen_label_string(labels):
        """
        Creates a properly formatted node labels string.
        :param labels: the list of labels to be applied
        :return: node labels string
        """
        label_string = ""
        for label in labels:
            label_string += ":{}".format(label)
        return label_string

    @staticmethod
    def _gen_properties_string(properties, dict_style=True, match_logic=False, variable="n", opening_statement=True):
        """
        Takes the given dictionary of properties and translates it to Cypher format.
        :param properties: dictionary of properties
        :param dict_style: Is the style dictionary format or WHERE/SET?
        :param match_logic: Is this a statement that needs logical AND joins? (e.g. WHERE)
        :param variable: The variable used in the statements
        :param opening_statement: Does there need to be an opening WHERE or SET statement?
        :return: a properties statement string
        """
        if dict_style:
            properties_string = " {"
            for i, prop in enumerate(properties.keys()):
                properties_string += "{}: {}".format(prop, GraphBuilder._get_property_value(properties[prop]))
                if i != len(properties) - 1:
                    properties_string += ", "
            properties_string += "}"
        else:
            if len(properties) > 0:
                if match_logic:
                    if opening_statement:
                        properties_string = " WHERE "
                    else:
                        properties_string = ""
                else:
                    properties_string = " SET "
            else:
                properties_string = ""
            for i, prop in enumerate(properties.keys()):
                properties_string += "{}.{} = {}".format(variable, prop, GraphBuilder._get_property_value(
                    properties[prop]))
                if i != len(properties) - 1:
                    if match_logic:
                        properties_string += " AND "
                    else:
                        properties_string += ", "
        return properties_string

    @staticmethod
    def _get_property_value(prop):
        """
        Takes the given property dictionary and translates the Cypher format
        :param prop: dictionary containing keys "type" and "value"
        :return: formatted property
        """
        if prop["type"] == "number":
            property_value = prop["value"]
        elif prop["type"] == "datetime":
            # property_value = 'datetime("{}")'.format(prop["value"])
            property_value = 'datetime("{}")'.format(prop["value"])
        elif prop["type"] == "list":
            property_value = prop["value"]
        else:
            property_value = '"{}"'.format(prop["value"])
        return property_value

    def _process(self, data):
        """
        Conducts the processing of all the documents returned from elastic.
        :param data: The elastic data
        :return: tuple containing a list of nodes and a list of relationships
        """
        nodes = list()
        relationships = list()
        self._logger.debug("processing data")
        for doc in data:
            doc = self._pre_process_doc(doc['_source'])
            doc_nodes, nodes_valid = self._gen_nodes(doc)
            if nodes_valid:
                doc_nodes = self._post_process_nodes(deepcopy(doc_nodes))
                doc_relationships, rels_valid = self._gen_relationships(doc, doc_nodes)
                if rels_valid:
                    doc_relationships = self._post_process_relationships(deepcopy(doc_relationships))
                    nodes.extend(doc_nodes)
                    relationships.extend(doc_relationships)
                else:
                    self._logger.debug("document did not generate all required relationships and is invalid")
            else:
                self._logger.debug("document did not generate all required nodes and is invalid")
        return nodes, relationships

    def _pre_process_doc(self, doc):
        """
        Calls the pre data processing modules.
        :param doc: the elastic document to be processed
        :return: the updated elastic document
        """
        if len(self._pre_modules):
            self._logger.debug("running pre-processors")
            for module in self._pre_modules:
                doc = module.pre_process_doc(doc)
        return doc

    def _post_process_nodes(self, nodes):
        """
        Calls the post node processing modules.
        :param nodes: the list of generated nodes
        :return: the updated list of nodes
        """
        if len(self._post_node_modules):
            self._logger.debug("running post-node processors")
            for module in self._post_node_modules:
                nodes = module.post_process_nodes(nodes)
        return nodes

    def _post_process_relationships(self, relationships):
        """
        Calls the post relationship processing modules.
        :param relationships: the list of generated relationships
        :return: the updated list of relationships
        """
        if len(self._post_relationship_modules):
            self._logger.debug("running post-relationship processors")
            for module in self._post_relationship_modules:
                relationships = module.post_process_relationships(relationships)
        return relationships

    def _gen_nodes(self, doc):
        """
        Generates the nodes based on the mapping for the given document.
        :param doc: The elastic document to be processed
        :return: A tuple consisting of a list of nodes and a boolean value if it generated all expected nodes
        """
        nodes = list()
        valid_doc = True
        for node in self._mapping['nodes']:
            new_node = {"nodeType": node['nodeType'], "labels": node['labels'], "id": node['id']}
            if node['nodeType'] == 'standard':
                new_node, valid = self._gen_standard_node(doc, node, deepcopy(new_node))
            elif node['nodeType'] == 'iterator':
                new_node, valid = self._gen_iterative_node(doc, node, deepcopy(new_node))
            else:
                valid = False
            if valid:
                nodes.append(new_node)
            elif node['required']:
                valid_doc = False
                break
        return nodes, valid_doc

    def _gen_standard_node(self, doc, node, new_node):
        """
        Generates a standard node.
        :param doc: the document to parse
        :param node: the node mapping
        :param new_node: the new node
        :return: the new node
        """
        properties = dict()
        unique_properties = dict()
        valid = True
        if 'properties' in node:
            for key in node["properties"]:
                if self._recursive_key_check(node["properties"][key]['key'].split("."), doc):
                    properties[key] = {"value": self._recursive_get_value(node["properties"][key]['key'].split("."), doc),
                                       "type": node["properties"][key]["type"]}
                    if 'uniqueProperties' in node and key in node['uniqueProperties']:
                        unique_properties[key] = deepcopy(properties[key])
                elif 'requiredProperties' in node and key in node['requiredProperties']:
                    self._logger.debug("document is missing required property for node: {}".format(
                        node["properties"][key]['key']))
                    valid = False
        if valid:
            if len(properties.keys()) > 0:
                new_node["properties"] = properties
            if len(unique_properties.keys()) > 0:
                new_node["uniqueProperties"] = unique_properties
            if "uniqueLabels" in node:
                new_node["uniqueLabels"] = node["uniqueLabels"]
        return new_node, valid

    def _gen_iterative_node(self, doc, node, new_node):
        """
        Generates an iterator node.
        :param doc: the document to parse
        :param node: the node mapping
        :param new_node: the new node
        :return: the new node
        """
        valid_iter = False
        node_list = []
        if self._recursive_key_check(node['iterator'].split("."), doc):
            for value in self._recursive_get_value(node['iterator'].split("."), doc):
                valid = True
                node_instance = {"labels": node['labels']}
                if 'uniqueLabels' in node:
                    node_instance['uniqueLabels'] = node['uniqueLabels']
                properties = dict()
                unique_properties = dict()
                if 'properties' in node:
                    for key in node["properties"]:
                        if self._recursive_key_check(node["properties"][key]['key'].split("."), doc):
                            properties[key] = {"value": self._recursive_get_value(node["properties"][key]['key'].split(".")
                                                                                  , doc),
                                               "type": node["properties"][key]["type"]}
                            if 'uniqueProperties' in node and key in node['uniqueProperties']:
                                unique_properties[key] = deepcopy(properties[key])
                        elif node["properties"][key]['key'] == 'ITER!':
                            properties[key] = {"value": value,
                                               "type": node["properties"][key]["type"]}
                            if 'uniqueProperties' in node and key in node['uniqueProperties']:
                                unique_properties[key] = deepcopy(properties[key])
                        elif 'requiredProperties' in node and key in node['requiredProperties']:
                            self._logger.debug("document is missing required property for node: {}".format(
                                node["properties"][key]['key']))
                            valid = False
                            break
                if valid:
                    if len(properties.keys()) > 0:
                        node_instance['properties'] = properties
                    if len(unique_properties.keys()) > 0:
                        node_instance['uniqueProperties'] = unique_properties
                    node_list.append(node_instance)
        if len(node_list) > 0:
            new_node["instances"] = node_list
            valid_iter = True
        return new_node, valid_iter

    def _gen_relationships(self, doc, nodes):
        """
        Generates the relationships based on the mapping for the given document and nodes.
        :param doc: The elastic document to be processed
        :param nodes: The nodes generated from the document
        :return: A tuple consisting of a list of relationships and a boolean value if it generated all expected
        relationships
        """
        relationships = list()
        valid_doc = True
        for relationship in self._mapping["relationships"]:
            new_relationship = {"type": relationship["type"], "relationshipType": relationship["relationshipType"]}
            if relationship['relationshipType'] == "standard":
                new_relationship, valid = self._gen_standard_relationship(doc, relationship, nodes,
                                                                          deepcopy(new_relationship))
            elif relationship["relationshipType"] == "iterator":
                new_relationship, valid = self._gen_iterative_relationship(doc, relationship, nodes,
                                                                           deepcopy(new_relationship))
            else:
                valid = False
            if valid:
                relationships.append(new_relationship)
            elif relationship['required']:
                valid_doc = False
                break
        return relationships, valid_doc

    def _gen_standard_relationship(self, doc, relationship, nodes, new_relationship):
        """
        Generates a standard relationship.
        :param doc: document to parse
        :param relationship: the mapping for the relationship
        :param nodes: related nodes
        :param new_relationship: the new relationship
        :return: the new relationship
        """
        valid = True
        new_relationship["directionality"] = relationship["directionality"]
        if 'unique' in relationship:
            new_relationship["unique"] = relationship["unique"]
        for rel_node in ["sourceNode", "destinationNode"]:
            node_found = False
            for node in nodes:
                if node['id'] == relationship[rel_node]:
                    new_relationship[rel_node] = node
                    node_found = True
            if not node_found:
                valid = False
        properties = dict()
        unique_properties = dict()
        if valid:
            if 'properties' in relationship:
                for key in relationship["properties"]:
                    if self._recursive_key_check(relationship["properties"][key]["key"].split("."), doc):
                        properties[key] = {"value": self._recursive_get_value(
                            relationship["properties"][key]["key"].split("."), doc),
                                           "type": relationship["properties"][key]["type"]}
                    elif "requiredProperties" in relationship and key in relationship['requiredProperties']:
                        self._logger.debug("document missing required property for relationship: {}".format(
                            relationship["properties"][key]["key"]))
                        valid = False
                        break
                    if 'uniqueProperties' in relationship and key in relationship['uniqueProperties']:
                        unique_properties[key] = deepcopy(properties[key])
        if valid:
            if len(properties.keys()) > 0:
                new_relationship['properties'] = properties
            if len(unique_properties.keys()) > 0:
                new_relationship['uniqueProperties'] = unique_properties
        return new_relationship, valid

    def _gen_iterative_relationship(self, doc, relationship, nodes, new_relationship):
        """
        Generates an iterator relationship.
        :param doc: document to parse
        :param relationship: the mapping for the relationship
        :param nodes: related nodes
        :param new_relationship: the new relationship
        :return: the new relationship
        """
        valid_iter = True
        iterative_nodes = 0
        iterative_node = None
        for rel_node in ["sourceNode", "destinationNode"]:
            node_found = False
            for node in nodes:
                if node['id'] == relationship[rel_node]:
                    new_relationship[rel_node] = node
                    node_found = True
                    if node['nodeType'] == "iterator":
                        iterative_nodes += 1
                        iterative_node = node
            if not node_found:
                valid_iter = False
        if valid_iter and iterative_nodes == 0:
            self._logger.error("iterative relationship that does not contain an iterator node")
            valid_iter = False
        elif valid_iter and iterative_nodes > 1:
            self._logger.error("iterative relationship with multiple iterator nodes is unsupported")
            valid_iter = False
        instances = list()
        if valid_iter:
            for i in range(0, len(iterative_node['instances'])):
                valid = True
                instance = {"type": relationship["type"], "directionality": relationship["directionality"]}
                if 'unique' in relationship:
                    instance["unique"] = relationship["unique"]
                properties = dict()
                unique_properties = dict()
                if 'properties' in relationship:
                    for key in relationship["properties"]:
                        if self._recursive_key_check(relationship["properties"][key]["key"].split("."), doc):
                            properties[key] = {"value": self._recursive_get_value(
                                relationship["properties"][key]["key"].split("."), doc),
                                               "type": relationship["properties"][key]["type"]}
                        elif "requiredProperties" in relationship and key in relationship['requiredProperties']:
                            self._logger.debug("document missing required property for relationship: {}".format(
                                relationship["properties"][key]["key"]))
                            valid = False
                            break
                        if 'uniqueProperties' in relationship and key in relationship['uniqueProperties']:
                            unique_properties[key] = deepcopy(properties[key])
                if valid:
                    if len(properties.keys()) > 0:
                        instance['properties'] = properties
                    if len(unique_properties.keys()) > 0:
                        instance['uniqueProperties'] = unique_properties
                    instances.append(instance)
        if len(instances) > 0:
            new_relationship["instances"] = instances
        else:
            valid_iter = False
        return new_relationship, valid_iter

    def _recursive_key_check(self, keys, item):
        """
        Recursively checks the provided item of a key at each level.
        :param keys: list of keys
        :param item: the item to check
        :return: bool indicating if all keys were found
        """
        if len(keys) > 1:
            if keys[0] in item:
                key = keys.pop(0)
                return self._recursive_key_check(keys, item[key])
            else:
                return False
        else:
            if keys[0] in item:
                return True
            else:
                return False

    def _recursive_get_value(self, keys, item):
        """
        Recursively retrieves the value of the last key in the list of keys.
        :param keys: list of keys
        :param item: the item to check
        :return: the value of the last key value if found or None
        """
        if len(keys) > 1:
            if keys[0] in item:
                key = keys.pop(0)
                return self._recursive_get_value(keys, item[key])
            else:
                return None
        else:
            if keys[0] in item:
                return item[keys[0]]
            else:
                return None

    def _load_additional_processing_modules(self, pre=True, post_node=True, post_relationship=True):
        """
        Loads processing modules from the processors folder. Checks each script in the for the required
        functions for each step. If the module has it then it is added to the appropriate list to be executed.
        :param pre: should pre-processing modules be loaded?
        :param post_node: should post node generation processing modules be loaded?
        :param post_relationship: should post relationship modules be loaded?
        """
        base_url = 'processors'
        try:
            files = [f for f in listdir(base_url) if isfile(join(base_url, f))]
            for f in files:
                if '.py' in f and '.pyc' not in f:
                    self._logger.debug('found potential processor: {}'.format(f))
                    module = SourceFileLoader(f.strip('.py'), './{}/{}'.format(base_url, f)).load_module()
                    if pre:
                        if all(func in dir(module) for func in REQUIRED_PRE_FUNC):
                            self._logger.debug('loaded pre processor: {}'.format(f))
                            self._pre_modules.append(module)
                    if post_node:
                        if all(func in dir(module) for func in REQUIRED_POST_NODE_FUNC):
                            self._logger.debug('loaded post node processor: {}'.format(f))
                            self._post_node_modules.append(module)
                    if post_relationship:
                        if all(func in dir(module) for func in REQUIRED_POST_RELATIONSHIP_FUNC):
                            self._logger.debug('loaded post relationship processor: {}'.format(f))
                            self._post_relationship_modules.append(module)

            self._logger.debug("loaded pre-process: {}, loaded post node: {}, loaded post relationship: {}".format(
                self._pre_modules, self._post_node_modules, self._post_relationship_modules))
        except FileNotFoundError as e:
            self._logger.error("{}".format(e))