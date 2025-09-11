import os
import json
from typing import Iterable, Dict, Any, Optional
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


def _parse_current_total_fields_limit(settings: Dict[str, Any], index: str) -> Optional[int]:
    # Settings can be returned in slightly different shapes depending on ES/OS version.
    try:
        idx_settings = settings.get(index, {}).get('settings', {})
    except Exception:
        idx_settings = settings

    # Try nested structure: index -> settings -> index -> mapping -> total_fields -> limit
    try:
        return int(idx_settings.get('index', {}).get('mapping', {}).get('total_fields', {}).get('limit'))
    except Exception:
        pass

    # Try flattened key names
    try:
        v = idx_settings.get('index.mapping.total_fields.limit') or idx_settings.get('index.mapping.total_fields.limit', None)
        if v is not None:
            return int(v)
    except Exception:
        pass

    # As a last resort, try to search for any integer-looking value in the settings dict
    def find_int(d):
        if isinstance(d, dict):
            for k, vv in d.items():
                try:
                    return int(vv)
                except Exception:
                    found = find_int(vv)
                    if found is not None:
                        return found
        return None

    return find_int(idx_settings)


def bulk_index(index: str, docs: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Bulk index a list of documents. If OpenSearch complains about the total fields limit
    we attempt to increase the index setting `index.mapping.total_fields.limit` and retry once.

    Returns a structured dict with at least the keys: 'indexed', 'errors'. On retry this will
    include 'increased_total_fields_limit_to'. On failure returns 'error' with the exception text.
    """
    client = get_client()
    actions = []
    for d in docs:
        actions.append({
            '_op_type': 'index',
            '_index': index,
            '_source': d,
        })

    try:
        resp = helpers.bulk(client, actions)
        return {'indexed': resp[0], 'errors': resp[1]}
    except Exception as e:
        err = str(e)
        # Detect the common "Limit of total fields" error message from OpenSearch/Elasticsearch
        if ('Limit of total fields' in err) or ('total fields' in err and 'exceeded' in err):
            print('Detected total_fields limit error for index', index)
            try:
                # Try to read current limit
                settings = client.indices.get_settings(index=index)
                cur_limit = _parse_current_total_fields_limit(settings, index) or 1000
                new_limit = max(cur_limit * 2, cur_limit + 1000, 2000)

                # Apply new setting. Use dot-notation which is accepted by put_settings.
                client.indices.put_settings(index=index, body={"index.mapping.total_fields.limit": str(new_limit)})
                print(f'Increased index.mapping.total_fields.limit from {cur_limit} to {new_limit}, retrying bulk index')

                # Retry once
                resp = helpers.bulk(client, actions)
                return {
                    'indexed': resp[0],
                    'errors': resp[1],
                    'increased_total_fields_limit_to': new_limit,
                }
            except Exception as e2:
                return {'indexed': 0, 'errors': True, 'error': err, 'retry_error': str(e2)}

        # Non-retriable or unexpected error: return structured info
        return {'indexed': 0, 'errors': True, 'error': err}

