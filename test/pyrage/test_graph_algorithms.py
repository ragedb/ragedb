import unittest
from unittest.mock import patch, MagicMock
from pyragedb.semantics import Model, Integer, Float, String
from pyragedb.semantics.reasoners.graph import Graph

class TestGraphAlgorithms(unittest.TestCase):
    @patch("requests.post")
    def test_graph_construction_and_manual_edges(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200, text="[]")

        m = Model("graph_test_model")
        User = m.Concept("User", identify_by={"id": Integer})
        
        # Construct graph (Pattern 2)
        graph = Graph(m, directed=False, weighted=True, node_concept=User)
        self.assertEqual(graph.Node, User)
        self.assertTrue(graph.weighted)
        self.assertFalse(graph.directed)

        # Create manual edges with weight
        u1 = User.new(id=1)
        u2 = User.new(id=2)
        edge = graph.Edge.new(src=u1, dst=u2, weight=4.5)
        self.assertEqual(edge._properties["weight"], 4.5)
        self.assertEqual(edge._attribute_ref._name, graph.edge_type)

        # Verify GQL compilation of edge with properties
        captured_gqls = []
        m.exec_gql = lambda query: captured_gqls.append(query) or "[]"
        m.define(edge)
        self.assertEqual(len(captured_gqls), 1)
        self.assertIn("weight: 4.5", captured_gqls[0])
        self.assertIn(graph.edge_type.upper(), captured_gqls[0])

    @patch("requests.post")
    def test_pagerank_and_lua_generation(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200, text="[]")

        m = Model("graph_pr_model", graph="graph_pr_model")
        User = m.Concept("User", identify_by={"id": Integer})
        graph = Graph(m, directed=True, weighted=False, node_concept=User)

        # Assign pagerank
        graph.Node.rank = graph.pagerank(damping=0.82, iterations=15)
        self.assertEqual(len(m._pending_graph_algorithms), 1)

        # Trigger query evaluation
        captured_posts = []
        def mock_post_impl(url, data=None, **kwargs):
            captured_posts.append((url, data))
            return MagicMock(status_code=200, text="[]")
        mock_post.side_effect = mock_post_impl

        # Run select query
        m.select(User.id, User.rank).to_dict()

        # Check schema creation and Lua script execution
        # Check that one of the posts went to the lua endpoint
        lua_posts = [p for p in captured_posts if p[0].endswith("/lua")]
        self.assertEqual(len(lua_posts), 1)
        lua_url, lua_script = lua_posts[0]
        self.assertIn("db/graph_pr_model/lua", lua_url)

        # Verify script contains key elements of PageRank implementation
        self.assertIn("damping = 0.82", lua_script)
        self.assertIn("iterations = 15", lua_script)
        self.assertIn("next_pr", lua_script)
        self.assertIn("get_neighbors", lua_script)
        self.assertIn('NodeSetProperty(node_id, "rank", val)', lua_script)

    @patch("requests.post")
    def test_jaccard_similarity_and_wcc_lua_generation(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200, text="[]")

        m = Model("graph_sim_model", graph="graph_sim_model")
        User = m.Concept("User", identify_by={"id": Integer})
        graph = Graph(m, directed=False, weighted=False, node_concept=User)

        # Assign similarity (ternary relationship) and wcc
        graph.Node.similarity = graph.jaccard_similarity()
        graph.Node.comp = graph.weakly_connected_component()

        self.assertEqual(len(m._pending_graph_algorithms), 2)

        captured_posts = []
        def mock_post_impl(url, data=None, **kwargs):
            captured_posts.append((url, data))
            return MagicMock(status_code=200, text="[]")
        mock_post.side_effect = mock_post_impl

        # Run select query
        m.select(User.id, User.comp).to_dict()

        # Check Lua posts
        lua_posts = [p for p in captured_posts if p[0].endswith("/lua")]
        self.assertEqual(len(lua_posts), 2)
        
        sim_script = lua_posts[0][1]
        wcc_script = lua_posts[1][1]

        self.assertIn("RelationshipAdd", sim_script)
        self.assertIn("intersect_count", sim_script)
        self.assertIn("union_count", sim_script)

        self.assertIn("union", wcc_script)
        self.assertIn("find", wcc_script)
        self.assertIn('NodeSetProperty(node_id, "comp"', wcc_script)

    @patch("requests.post")
    def test_triangles_lua_generation(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200, text="[]")

        m = Model("graph_tri_model", graph="graph_tri_model")
        User = m.Concept("User", identify_by={"id": Integer})
        graph = Graph(m, directed=False, weighted=False, node_concept=User)

        # Assign triangles
        graph.Node.tri = graph.triangle()
        graph.Node.uniq_tri = graph.unique_triangle()

        self.assertEqual(len(m._pending_graph_algorithms), 2)

        captured_posts = []
        def mock_post_impl(url, data=None, **kwargs):
            captured_posts.append((url, data))
            return MagicMock(status_code=200, text="[]")
        mock_post.side_effect = mock_post_impl

        # Run select query
        m.select(User.id).to_dict()

        # Check Lua posts
        lua_posts = [p for p in captured_posts if p[0].endswith("/lua")]
        self.assertEqual(len(lua_posts), 2)
        
        tri_script = lua_posts[0][1]
        uniq_tri_script = lua_posts[1][1]

        self.assertIn("RelationshipAdd", tri_script)
        self.assertIn("unique = false", tri_script)
        self.assertIn("RelationshipAdd", uniq_tri_script)
        self.assertIn("unique = true", uniq_tri_script)
        self.assertIn("u_id < v_id and v_id < w_id", uniq_tri_script)

if __name__ == "__main__":
    unittest.main()
