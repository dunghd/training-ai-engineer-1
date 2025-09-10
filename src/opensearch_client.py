import os
import json
from typing import Iterable, Dict, Any
from opensearchpy import OpenSearch, helpers


def get_client() -> OpenSearch:
    endpoint = os.environ.get('OPENSEARCH_ENDPOINT')
    if not endpoint:
        raise RuntimeError('OPENSEARCH_ENDPOINT environment variable is required')
    http_auth = None
    user = os.environ.get('OPENSEARCH_USER')
    pwd = os.environ.get('OPENSEARCH_PASS')
    if user and pwd:
        http_auth = (user, pwd)
    return OpenSearch(hosts=[endpoint], http_auth=http_auth)


def bulk_index(index: str, docs: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    client = get_client()
    actions = []
    for d in docs:
        actions.append({
            '_op_type': 'index',
            '_index': index,
            '_source': d,
        })
    resp = helpers.bulk(client, actions)
    return {'indexed': resp[0], 'errors': resp[1]}
