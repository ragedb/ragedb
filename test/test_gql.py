# Copyright RageDB Contributors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import time
import requests
import sys

def run_tests():
    # 1. Start ragedb server
    print("Starting ragedb server...")
    server_process = subprocess.Popen(
        ["./build/ragedb"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    # Wait for server to start up
    connected = False
    for attempt in range(15):
        try:
            r = requests.get("http://localhost:7243/db/rage/schema/nodes", timeout=1)
            if r.status_code == 200:
                connected = True
                break
        except Exception:
            pass
        print(f"Waiting for ragedb server to start (attempt {attempt + 1})...")
        time.sleep(1)

    if not connected:
        print("Failed to contact server schema node endpoint after 15 seconds")
        server_process.terminate()
        stdout, stderr = server_process.communicate()
        print("STDOUT:", stdout.decode())
        print("STDERR:", stderr.decode())
        sys.exit(1)

    try:
        url_base = "http://localhost:7243"
        graph = "rage"

        # 3. Create node schemas
        print("Creating Node schemas...")
        requests.post(f"{url_base}/db/{graph}/schema/nodes/Person")
        requests.post(f"{url_base}/db/{graph}/schema/nodes/Movie")
        requests.post(f"{url_base}/db/{graph}/schema/nodes/Person/properties/name/string")
        requests.post(f"{url_base}/db/{graph}/schema/nodes/Person/properties/age/integer")
        requests.post(f"{url_base}/db/{graph}/schema/nodes/Movie/properties/title/string")

        # 4. Create relationship schemas
        print("Creating Relationship schemas...")
        requests.post(f"{url_base}/db/{graph}/schema/relationships/ACTED_IN")

        # 5. Populate nodes
        print("Creating Nodes...")
        requests.post(f"{url_base}/db/{graph}/node/Person/alice", json={"name": "Alice", "age": 30})
        requests.post(f"{url_base}/db/{graph}/node/Person/bob", json={"name": "Bob", "age": 35})
        requests.post(f"{url_base}/db/{graph}/node/Movie/matrix", json={"title": "The Matrix"})
        requests.post(f"{url_base}/db/{graph}/node/Movie/avatar", json={"title": "Avatar"})

        # 6. Populate relationships
        print("Creating Relationships...")
        requests.post(f"{url_base}/db/{graph}/node/Person/alice/relationship/Movie/matrix/ACTED_IN")
        requests.post(f"{url_base}/db/{graph}/node/Person/bob/relationship/Movie/avatar/ACTED_IN")

        # 7. Test GQL endpoint
        print("\n--- Running GQL Tests ---")

        # Test 1: Simple Node Match
        q1 = "MATCH (p:Person) RETURN p.name, p.age"
        print(f"Query 1: {q1}")
        r = requests.post(f"{url_base}/db/{graph}/gql", data=q1)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 200
        assert "Alice" in r.text
        assert "Bob" in r.text

        # Test 2: Node Match with WHERE Filter
        q2 = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name, p.age"
        print(f"Query 2: {q2}")
        r = requests.post(f"{url_base}/db/{graph}/gql", data=q2)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 200
        assert "Alice" in r.text
        assert "Bob" not in r.text

        # Test 3: Traversal Match (1-hop)
        q3 = "MATCH (p:Person)-[e:ACTED_IN]->(m:Movie) RETURN p.name, m.title"
        print(f"Query 3: {q3}")
        r = requests.post(f"{url_base}/db/{graph}/gql", data=q3)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 200
        assert "The Matrix" in r.text
        assert "Avatar" in r.text

        # Test 4: Traversal Match with Filter
        q4 = "MATCH (p:Person)-[:ACTED_IN]->(m:Movie) WHERE p.name = 'Bob' RETURN p.name, m.title"
        print(f"Query 4: {q4}")
        r = requests.post(f"{url_base}/db/{graph}/gql", data=q4)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 200
        assert "Bob" in r.text
        assert "Avatar" in r.text
        assert "Alice" not in r.text

        # Test 5: Optional Match
        q5 = "MATCH (p:Person) OPTIONAL MATCH (p)-[:ACTED_IN]->(m:Movie) WHERE m.title = 'The Matrix' RETURN p.name, m.title"
        print(f"Query 5: {q5}")
        r = requests.post(f"{url_base}/db/{graph}/gql", data=q5)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 200
        assert "Alice" in r.text
        assert "The Matrix" in r.text
        assert "Bob" in r.text  # Bob should be returned with null movie
        assert "null" in r.text

        # Test 6: ORDER BY and LIMIT
        q6 = "MATCH (p:Person) RETURN p.name ORDER BY p.age DESC LIMIT 1"
        print(f"Query 6: {q6}")
        r = requests.post(f"{url_base}/db/{graph}/gql", data=q6)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 200
        assert "Bob" in r.text
        assert "Alice" not in r.text

        # Test 7: Size Limit Enforcement (64KB)
        print("Testing 64KB query size limit...")
        large_query = "MATCH (p:Person) " + (" " * 70000) + "RETURN p.name"
        r = requests.post(f"{url_base}/db/{graph}/gql", data=large_query)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 400
        assert "size" in r.text

        # Test 8: Aggregations (Global COUNT and SUM)
        q8 = "MATCH (p:Person) RETURN count(*), sum(p.age)"
        print(f"Query 8: {q8}")
        r = requests.post(f"{url_base}/db/{graph}/gql", data=q8)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 200
        assert '"count(*)": 2' in r.text
        assert '"sum(p.age)": 65' in r.text

        # Test 9: Aggregations (Grouping by age and sorting)
        print("Adding Charlie for grouping test...")
        requests.post(f"{url_base}/db/{graph}/node/Person/charlie", json={"name": "Charlie", "age": 30})

        q9 = "MATCH (p:Person) RETURN p.age, count(*), sum(p.age) ORDER BY p.age DESC"
        print(f"Query 9: {q9}")
        r = requests.post(f"{url_base}/db/{graph}/gql", data=q9)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 200
        assert '"p.age": 35' in r.text
        assert '"p.age": 30' in r.text
        assert r.text.index('"p.age": 35') < r.text.index('"p.age": 30')

        # Test 10: Set Operations (UNION distinct)
        q10 = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name UNION MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name"
        print(f"Query 10: {q10}")
        r = requests.post(f"{url_base}/db/{graph}/gql", data=q10)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 200
        assert "Alice" in r.text
        assert "Bob" in r.text

        # Test 11: Set Operations (INTERSECT distinct)
        q11 = "MATCH (p:Person) WHERE p.name = 'Alice' RETURN p.name INTERSECT MATCH (p:Person) RETURN p.name"
        print(f"Query 11: {q11}")
        r = requests.post(f"{url_base}/db/{graph}/gql", data=q11)
        print("Response:", r.status_code, r.text)
        assert r.status_code == 200
        assert "Alice" in r.text
        assert "Bob" not in r.text

        print("\nAll GQL tests passed successfully!")

    finally:
        print("Stopping ragedb server...")
        server_process.terminate()
        server_process.wait()

if __name__ == "__main__":
    run_tests()
