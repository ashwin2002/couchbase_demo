import json
from argparse import ArgumentParser

from sdk import CouchbaseSDK

# Update this to your cluster
server_ip = "172.23.108.67"
username = "Administrator"
password = "password"
bucket_name = "demo_bucket"


def validate_document(document):
    assert document is not None
    try:
        return json.loads(document)
    except json.decoder.JSONDecodeError:
        return document
    except Exception as e:
        raise e


def main():
    parser = ArgumentParser()
    parser.add_argument("--op_type", dest="op_type", required=True,
                        choices=['create', 'update', 'read', 'delete'])
    parser.add_argument("--doc_key", dest="doc_key", required=True)
    parser.add_argument("--document", dest="document", required=False,
                        default=None)
    user_args = parser.parse_args()

    sdk_conn = CouchbaseSDK(server_ip, username, password)
    sdk_conn.select_bucket(bucket_name)
    sdk_conn.select_collection("_default", "_default")
    if user_args.op_type == "create":
        document = validate_document(user_args.document)
        result = sdk_conn.create(user_args.doc_key, document)
        print(f"CAS: {result.cas}")
    elif user_args.op_type == "update":
        document = validate_document(user_args.document)
        result = sdk_conn.update(user_args.doc_key, document)
        print(f"CAS: {result.cas}")
    elif user_args.op_type == "read":
        result = sdk_conn.read(user_args.doc_key)
        print(f"Doc: {result.value}")
        print(f"CAS: {result.cas}")
    elif user_args.op_type == "delete":
        result = sdk_conn.delete(user_args.doc_key)
        print(f"CAS: {result.cas}")
    sdk_conn.close()


if __name__ == "__main__":
    main()
