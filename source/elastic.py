from elasticsearch import Elasticsearch
import logging

module_logger = logging.getLogger('elastic2neo.elastic')
module_logger.debug("module loaded")


class ElasticScroller:
    def __init__(self, host, port, index, https=False, verify_certs=False, http_auth=None, timeout=1000,
                 doc_type=None, size=1000, body=None):
        """
        A simple index scroller for Elasticsearch.
        :param host: the es host
        :param port: the es port
        :param index: the index to scroll
        :param https: is this an https connection?
        :param verify_certs: should certificates be verified (currently verification not supported)
        :param http_auth: Is there basic http authentication? If so provide a tuple (user, password)
        :param timeout: How long do we wait for elastic to return the data
        :param doc_type: what document type should be returned
        :param size: how many documents should be returned
        :param body: used to provide a more targeted query
        """
        self._logger = logging.getLogger('elastic2neo.elastic.ElasticScroller')

        if https:
            url = "https://{}:{}".format(host, port)
        else:
            url = "http://{}:{}".format(host, port)

        self._verify_certs = verify_certs
        self._http_auth = http_auth
        self._timeout = timeout
        if http_auth:
            self._es = Elasticsearch(url, http_auth=self._http_auth,
                                     timeout=self._timeout, verify_certs=self._verify_certs)
        else:
            self._es = Elasticsearch(url, timeout=self._timeout, verify_certs=self._verify_certs)

        self._index = index
        self._doc_type = doc_type
        self._size = size
        if not body:
            self._body = dict()
        else:
            self._body = body
        self._sid = None

    def _init_scroll(self):
        """
        Called if a scroll id is not valid, does the initial call for the scroll.
        :return: elastic data as a dictionary
        """
        if not self._es.indices.exists(index=self._index):
            self._logger.error("index {} does not exist".format(self._index))
            return None
        if self._doc_type:
            data = self._es.search(index=self._index, doc_type=self._doc_type, scroll='2m', size=self._size,
                                   body=self._body)
        else:
            data = self._es.search(index=self._index, scroll='2m', size=self._size, body=self._body)
        return data

    def scroll(self):
        """
        Scroll and return the data
        :return: elastic data as a dictionary
        """
        if not self._sid:
            data = self._init_scroll()
        else:
            data = self._es.scroll(scroll_id=self._sid, scroll='2m')
        if '_scroll_id' in data:
            self._sid = data['_scroll_id']
            # Get the number of results that returned in the last scroll
            self._logger.debug("{} hits on scroll for index {}".format(len(data['hits']['hits']), self._index))
            return data['hits']['hits']
        else:
            self._logger.error("elastic did not return a scroll id")
            self._logger.debug("{}".format(data))
            self._sid = None
            return None







