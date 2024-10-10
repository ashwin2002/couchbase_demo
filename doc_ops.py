import json
from argparse import ArgumentParser

from sdk import CouchbaseSDK


def sample_data(sdk_conn):
    c1_docs = [
        ("CSE_000001", {"specialization":"data_science", "name": "John F",
                        "year": 2022, "register_no":"CSE_000001"}),
        ("CSE_000002", {"specialization":"data_science", "name": "Azad M",
                        "year": 2024, "register_no":"CSE_000002"}),
        ("CSE_000003", {"specialization":"data_science", "name": "Bala E",
                        "year": 2024, "register_no":"CSE_000003"}),
        ("CSE_000004", {"specialization":"data_science", "name": "Fiyaz M",
                        "year": 2023, "register_no":"CSE_000004"}),
        ("CSE_000005", {"specialization":"data_science", "name": "Nathan",
                        "year": 2023, "register_no":"CSE_000005"}),
        ("CSE_000006", {"specialization":"block_chain", "name": "Gautam S",
                        "year": 2024, "register_no":"CSE_000006"}),
        ("CSE_000007", {"specialization":"block_chain", "name": "Ravi S",
                        "year": 2023, "register_no":"CSE_000007"}),
        ("CSE_000008", {"specialization":"block_chain", "name": "Christopher C",
                        "year": 2022, "register_no":"CSE_000008"}),
        ("CSE_000009", {"specialization":"block_chain", "name": "Aamir S",
                        "year": 2024, "register_no":"CSE_000009"}),
        ("CSE_000010", {"specialization":"ai_ml", "name": "Fariq A",
                        "year": 2024, "register_no":"CSE_000010"}),
        ("CSE_000011", {"specialization":"ai_ml", "name": "Ishaan S",
                        "year": 2024, "register_no":"CSE_000011"}),
        ("CSE_000012", {"specialization":"ai_ml", "name": "Ikbal K",
                        "year": 2024, "register_no":"CSE_000012"}),
        ("CSE_000013", {"specialization":"ai_ml", "name": "Hafiz M",
                        "year": 2024, "register_no":"CSE_000013"}),
        ("CSE_000014", {"specialization":"ai_ml", "name": "Jai G",
                        "year": 2024, "register_no":"CSE_000014"}),
    ]

    c2_docs = [
        ("CSE_010001", {"specialization":"bio_informatics", "name": "John F",
                        "year": 2022, "register_no":"CSE_010001"}),
        ("CSE_010002", {"specialization":"data_science", "name": "Azad M",
                        "year": 2024, "register_no":"CSE_010002"}),
        ("CSE_010003", {"specialization":"data_science", "name": "Bala E",
                        "year": 2024, "register_no":"CSE_010003"}),
        ("CSE_010004", {"specialization":"bio_informatics", "name": "Fiyaz M",
                        "year": 2023, "register_no":"CSE_010004"}),
        ("CSE_010005", {"specialization":"information_security", "name": "Nathan",
                        "year": 2023, "register_no":"CSE_010005"}),
        ("CSE_010006", {"specialization":"block_chain", "name": "Gautam S",
                        "year": 2024, "register_no":"CSE_010006"}),
        ("CSE_010007", {"specialization":"block_chain", "name": "Ravi S",
                        "year": 2023, "register_no":"CSE_010007"}),
        ("CSE_010008", {"specialization":"information_security", "name": "Christopher C",
                        "year": 2022, "register_no":"CSE_010008"}),
        ("CSE_010009", {"specialization":"block_chain", "name": "Aamir S",
                        "year": 2024, "register_no":"CSE_010009"}),
        ("CSE_010010", {"specialization":"data_science", "name": "Fariq A",
                        "year": 2024, "register_no":"CSE_010010"}),
        ("CSE_010011", {"specialization":"ai_ml", "name": "Ishaan S",
                        "year": 2022, "register_no":"CSE_010011"}),
        ("CSE_010012", {"specialization":"ai_ml", "name": "Ikbal K",
                        "year": 2022, "register_no":"CSE_010012"}),
        ("CSE_010013", {"specialization":"ai_ml", "name": "Hafiz M",
                        "year": 2023, "register_no":"CSE_010013"}),
        ("CSE_010014", {"specialization":"data_science", "name": "Jai G",
                        "year": 2023, "register_no":"CSE_010014"}),
        ("CSE_010015", {"specialization":"ai_ml", "name": "Pallavi G",
                        "year": 2024, "register_no":"CSE_010015"}),
        ("CSE_010016", {"specialization":"ai_ml", "name": "Haseena Z",
                        "year": 2024, "register_no":"CSE_010016"}),
        ("CSE_010017", {"specialization":"data_science", "name": "Naveen A",
                        "year": 2024, "register_no":"CSE_010017"}),
        ("CSE_010018", {"specialization":"ai_ml", "name": "Kevin S",
                        "year": 2024, "register_no":"CSE_010018"}),
    ]

    sdk_conn.select_collection("campus_1", "cse")
    for key, doc in c1_docs:
        sdk_conn.create(key, doc)

    sdk_conn.select_collection("campus_2", "cse")
    for key, doc in c2_docs:
        sdk_conn.create(key, doc)


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
    parser.add_argument("--server", dest="server", default="127.0.0.1")
    parser.add_argument("--username", dest="username", default="Administrator")
    parser.add_argument("--password", dest="password", default="password")
    parser.add_argument("--bucket", dest="bucket", default="demo")
    parser.add_argument("--scope", dest="scope", default="_default")
    parser.add_argument("--collection", dest="collection", default="_default")
    parser.add_argument("--op_type", dest="op_type", required=False,
                        choices=['create', 'update', 'read', 'delete', 'load_sample_dataset'])
    parser.add_argument("--doc_key", dest="doc_key", default=None)
    parser.add_argument("--document", dest="document", required=False,
                        default=None)
    parser.add_argument("--query", dest="query_str", default=None)
    user_args = parser.parse_args()

    sdk_conn = CouchbaseSDK(user_args.server, user_args.username, user_args.password)
    sdk_conn.select_bucket(user_args.bucket)
    sdk_conn.select_collection(user_args.scope, user_args.collection)
    if user_args.op_type == "create":
        assert user_args.doc_key is not None
        document = validate_document(user_args.document)
        result = sdk_conn.create(user_args.doc_key, document)
        print(f"CAS: {result.cas}")
    elif user_args.op_type == "update":
        assert user_args.doc_key is not None
        document = validate_document(user_args.document)
        result = sdk_conn.update(user_args.doc_key, document)
        print(f"CAS: {result.cas}")
    elif user_args.op_type == "read":
        assert user_args.doc_key is not None
        result = sdk_conn.read(user_args.doc_key)
        print(f"Doc: {result.value}")
        print(f"CAS: {result.cas}")
    elif user_args.op_type == "delete":
        assert user_args.doc_key is not None
        result = sdk_conn.delete(user_args.doc_key)
        print(f"CAS: {result.cas}")
    elif user_args.op_type == "load_sample_dataset":
        sample_data(sdk_conn)
    elif user_args.query_str is not None:
        assert user_args.query_str is not None
        result = sdk_conn.query(user_args.query_str)
        for row in result:
            print(row)
    sdk_conn.close()


if __name__ == "__main__":
    main()
