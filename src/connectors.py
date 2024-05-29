from neo4j import GraphDatabase
import json
from typing import Optional


class Client:
    def __init__(self):
        self.driver = None

    def close(self):
        raise NotImplementedError

    def query(self, query, parameters):
        raise NotImplementedError

    def query_as_json(self, query: str, parameters: Optional[dict]):
        raise NotImplementedError


class Neo4jClient(Client):
    def __init__(self, uri, user, password):
        super().__init__()
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """
        Closes the connection
        :return:
        """
        self.driver.close()

    def query(self, query: str, parameters: Optional[dict] = None):
        """
        Buils a session
        :param cypher_query:
        :param parameters:
        :return:
        """
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return [record.data() for record in result]

    def query_as_json(self, cypher_query: str, parameters=None):
        result = self.query(cypher_query, parameters)
        self.close()
        return result
