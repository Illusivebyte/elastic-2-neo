#!/usr/bin/python
import logging
from source.neo import GraphBuilder
from source.elastic import ElasticScroller
from yaml import full_load, YAMLError
import getopt
import sys
from time import sleep

logger = logging.getLogger('elastic2neo')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s')


def _execute(scroller, builder, scroll=True, execute=True, sleep_delay=15, end_after_empty=False):
    """
    Execute the main functions.
    :param scroller: the elastic Scroller object
    :param builder: the Neo4j GraphBuilder object
    :param scroll: Should we keep scrolling?
    :param execute: Should the statements generated be executed against Neo4j?
    """
    try:
        if scroll:
            while 1:
                logger.info("scrolling elastic index")
                data = scroller.scroll()
                if len(data):
                    logger.info("building graph")
                    builder.build(data, execute)
                else:
                    if end_after_empty:
                        break
                    logger.info("scroll was empty sleeping for {} minutes".format(sleep_delay))
                    sleep(sleep_delay*60)
        else:
            logger.info("scrolling elastic index")
            data = scroller.scroll()
            logger.info("building graph")
            builder.build(data, execute)
    except KeyboardInterrupt:
        logger.info("interrupt detected")
    finally:
        builder.close()
        logger.info("complete")


def _setup_logging(enable_file=False, file_path="e2n.log", debug=False):
    """
    Setup logging for the application.
    :param enable_file: Should file logging be enabled?
    :param file_path: What is the full path where you want the log to be saved?
    :param debug: Set to debug level?
    """
    # Setup logging for this application
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    if debug:
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.INFO)
    logger.addHandler(ch)
    if enable_file:
        fh = logging.FileHandler(file_path)
        if debug:
            fh.setLevel(logging.DEBUG)
        else:
            fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    if debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


def _load_mapping(mapping_file):
    """
    Loads the mapping file specified.
    :param mapping_file: The name or full path to mapping file
    """
    logger.debug("loading mapping file: {}".format(mapping_file))
    mapping = None
    try:
        mapping = full_load(open(mapping_file).read())
    except IOError as e:
        logger.error(e)
        exit(1)
    except YAMLError as e:
        logger.error(e)
        exit(1)
    # TODO: mapping file validation
    logger.debug("completed load of mapping file: {}".format(mapping_file))
    return mapping


def _load_config_file(name="config.yaml"):
    """
    Load the specified config file and return the contents.
    :param name: name or path to auth file
    :return: dictionary of contents
    """
    logger.debug("loading config file: {}".format(name))
    auth = None
    try:
        auth = full_load(open(name).read())
    except IOError as e:
        logger.error(e)
        exit(1)
    except YAMLError as e:
        logger.error(e)
        exit(1)
    logger.debug("completed load of config file: {}".format(name))
    return auth


# Used to ensure that the config has its required values
REQUIRED_CONFIG_VALUES = ['elastic', 'neo']
REQUIRED_ES_CONFIG_VALUES = ['host', 'port', 'protocol', 'scrollSize', 'sleepMin']
REQUIRED_NEO_CONFIG_VALUES = ['host', 'port', 'protocol', 'user', 'password']


def _setup_objects(config, mapping, execute):
    """
    Sets up the required class objects for execution
    :param config: config as a dictionary
    :param mapping: mapping as a dictionary
    :param execute: are the statements being executed?
    :return: tuple of objects
    """
    scroller = None
    builder = None
    if all(keys in config for keys in REQUIRED_CONFIG_VALUES):
        elastic = config['elastic']
        if all(keys in elastic for keys in REQUIRED_ES_CONFIG_VALUES):
            https = False
            if elastic['protocol'] == 'https':
                https = True
            auth_required = False
            if all(key in elastic for key in ['user', 'password']):
                auth_required = True
            if auth_required:
                scroller = ElasticScroller(elastic['host'], elastic['port'], index=mapping['index'],
                                           doc_type=mapping['docType'], https=https,
                                           http_auth=(elastic['user'], elastic['password']), size=elastic['scrollSize'])
            else:
                scroller = ElasticScroller(elastic['host'], elastic['port'], index=mapping['index'],
                                           doc_type=mapping['docType'], https=https, size=elastic['scrollSize'])
        else:
            logger.error("config file is missing required values")
            exit(1)
        neo = config['neo']
        if all(keys in neo for keys in REQUIRED_NEO_CONFIG_VALUES):
            builder = GraphBuilder("{}://{}:{}".format(neo['protocol'], neo['host'], neo['port']),
                                   user=neo['user'], password=neo['password'], mapping=mapping, execute=execute)
        else:
            logger.error("config file is missing required values")
            exit(1)
    else:
        logger.error("config file is missing required values")
        exit(1)
    return scroller, builder


def _help():
    """
    Prints the help statement
    """
    print("usage: elastic2neo.py [-d] [-f [-F LogFile]] [-C ConfigFile] [-M MappingFile] [-o] [-e] [-n] [-h]"
          "\n-d\tEnable debug messages"
          "\n-f\tEnable logging to file"
          "\n-F (LogFile)\tSpecify log file (requires -f)"
          "\n-C (Config)\tSpecify config file"
          "\n-M (Mapping\tSpecify mapping file"
          "\n-o\tExecute Elasticsearch scroll  once"
          "\n-e\tEnd execution after the Elasticsearch index is empty"
          "\n-n\tDo not execute cypher statements (for debugging)"
          "\n-h\tView the usage syntax")


def _usage():
    """
    Prints the usage reminder
    """
    print("usage: elastic2neo.py [-d] [-f [-F LogFile]] [-C ConfigFile] [-M MappingFile] [-o] [-e] [-n] [-h]")


def main(argv):
    """
    Main function, parses commandline options and arguments and executes them.
    :param argv: argv from system
    """
    try:
        opts, args = getopt.getopt(argv, "dfF:C:M:onh", [])
        debug = False
        enable_file = False
        log_file = "e2n.log"
        config_file = "config.yaml"
        mapping_file = "mapping.yaml"
        scroll = True
        execute = True
        end_after_empty = False
        for opt, arg in opts:
            if opt == '-h':
                _help()
                exit(0)
            elif opt == '-d':
                debug = True
            elif opt == '-f':
                enable_file = True
            elif opt == '-F':
                if enable_file:
                    mapping_file = arg
                else:
                    _usage()
                    exit(1)
            elif opt == '-C':
                config_file = arg
            elif opt == '-M':
                mapping_file = arg
            elif opt == '-o':
                scroll = False
            elif opt == '-n':
                execute = False
            elif opt == "-e":
                end_after_empty = True
        _setup_logging(enable_file, log_file, debug)
        mapping = _load_mapping(mapping_file)
        config = _load_config_file(config_file)
        scroller, builder = _setup_objects(config, mapping, execute)
        _execute(scroller, builder, scroll=scroll, execute=execute, sleep_delay=config['elastic']['sleepMin'],
                 end_after_empty=end_after_empty)
    except getopt.GetoptError:
        _usage()
        exit(1)


if __name__ == '__main__':
    main(sys.argv[1:])




