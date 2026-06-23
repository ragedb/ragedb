# Graph reasoning algorithms for pyragedb-semantics
import requests

class GraphAlgorithmExpr:
    def __init__(self, graph, name, **kwargs):
        self.graph = graph
        self.name = name
        self.kwargs = kwargs
        # Map algorithm to target datatype
        if name in ("pagerank", "eigenvector_centrality", "betweenness_centrality", "degree_centrality", 
                    "jaccard_similarity", "cosine_similarity", "adamic_adar", "preferential_attachment", 
                    "local_clustering_coefficient", "average_clustering_coefficient"):
            self.target_type = "Float"
        elif name in ("louvain", "leiden", "infomap", "label_propagation", "weakly_connected_component"):
            self.target_type = "Integer"
        elif name in ("triangle", "unique_triangle"):
            self.target_type = "String"
        elif name in ("triangle_count", "distance"):
            self.target_type = "Integer"
        elif name in ("is_connected"):
            self.target_type = "Boolean"
        else:
            self.target_type = "Float"


class GraphNodeProxy:
    def __init__(self, graph):
        self._graph = graph

    def __setattr__(self, name, value):
        if name.startswith('_'):
            super().__setattr__(name, value)
            return
        
        # Forward the assignment to all node concepts in the graph
        concepts = self._graph.node_concepts
        if not concepts and self._graph.node_concept:
            concepts = [self._graph.node_concept]
        
        for concept in concepts:
            concept.__setattr__(name, value)


class GraphEdgeProxy:
    def __init__(self, graph):
        self._graph = graph

    def new(self, src, dst, weight=None):
        from ..concept import RelationshipFact, AttributeRef
        # Ensure schema type for the relationship exists
        rel_name = self._graph.edge_type.upper()
        try:
            requests.post(f"{self._graph.model.host}/db/{self._graph.model.graph}/schema/relationships/{rel_name}")
        except Exception:
            pass
        
        # Track node concepts in the graph
        if src._concept not in self._graph.node_concepts:
            self._graph.node_concepts.append(src._concept)
        if dst._concept not in self._graph.node_concepts:
            self._graph.node_concepts.append(dst._concept)

        ref = AttributeRef(src._concept, self._graph.edge_type, is_relationship=True, target=dst._concept)
        props = {"weight": float(weight)} if weight is not None else {}
        return RelationshipFact(src, ref, dst, properties=props)


class Graph:
    def __init__(self, model, directed=False, weighted=False, node_concept=None, edge_concept=None,
                 edge_src_relationship=None, edge_dst_relationship=None, edge_weight_relationship=None,
                 aggregator=None):
        self.model = model
        self.directed = directed
        self.weighted = weighted
        self.node_concept = node_concept
        self.edge_concept = edge_concept
        self.edge_src_relationship = edge_src_relationship
        self.edge_dst_relationship = edge_dst_relationship
        self.edge_weight_relationship = edge_weight_relationship
        self.aggregator = aggregator
        
        # Unique edge type for manual edges
        self.edge_type = f"edge_gen_{id(self)}"
        self.node_concepts = []

        # Expose Node and Edge proxies
        if node_concept is not None:
            self.Node = node_concept
            self.node_concepts = [node_concept]
        else:
            self.Node = GraphNodeProxy(self)

        if edge_concept is not None:
            self.Edge = edge_concept
        else:
            self.Edge = GraphEdgeProxy(self)

    # Algorithms
    def degree_centrality(self, direction="BOTH"):
        return GraphAlgorithmExpr(self, "degree_centrality", direction=direction)

    def eigenvector_centrality(self, iterations=20, tolerance=1e-6):
        return GraphAlgorithmExpr(self, "eigenvector_centrality", iterations=iterations, tolerance=tolerance)

    def betweenness_centrality(self):
        return GraphAlgorithmExpr(self, "betweenness_centrality")

    def pagerank(self, damping=0.85, iterations=20, tolerance=1e-6):
        return GraphAlgorithmExpr(self, "pagerank", damping=damping, iterations=iterations, tolerance=tolerance)

    def jaccard_similarity(self, **kwargs):
        return GraphAlgorithmExpr(self, "jaccard_similarity", **kwargs)

    def cosine_similarity(self, **kwargs):
        return GraphAlgorithmExpr(self, "cosine_similarity", **kwargs)

    def adamic_adar(self, **kwargs):
        return GraphAlgorithmExpr(self, "adamic_adar", **kwargs)

    def preferential_attachment(self, **kwargs):
        return GraphAlgorithmExpr(self, "preferential_attachment", **kwargs)

    def infomap(self):
        return GraphAlgorithmExpr(self, "infomap")

    def louvain(self):
        return GraphAlgorithmExpr(self, "louvain")

    def leiden(self):
        return GraphAlgorithmExpr(self, "leiden")

    def label_propagation(self):
        return GraphAlgorithmExpr(self, "label_propagation")

    def triangle(self):
        return GraphAlgorithmExpr(self, "triangle")

    def unique_triangle(self):
        return GraphAlgorithmExpr(self, "unique_triangle")

    def triangle_count(self):
        return GraphAlgorithmExpr(self, "triangle_count")

    def local_clustering_coefficient(self):
        return GraphAlgorithmExpr(self, "local_clustering_coefficient")

    def average_clustering_coefficient(self):
        return GraphAlgorithmExpr(self, "average_clustering_coefficient")

    def reachable(self, **kwargs):
        return GraphAlgorithmExpr(self, "reachable", **kwargs)

    def weakly_connected_component(self):
        return GraphAlgorithmExpr(self, "weakly_connected_component")

    def is_connected(self):
        return GraphAlgorithmExpr(self, "is_connected")

    def distance(self, **kwargs):
        return GraphAlgorithmExpr(self, "distance", **kwargs)

    def diameter_range(self):
        return GraphAlgorithmExpr(self, "diameter_range")


def generate_lua_script(graph, property_name, expr):
    """
    Generates a sandboxed Lua script that executes the graph algorithm
    on RageDB using C++ native bindings.
    """
    # 1. Determine node types
    node_types_list = []
    if graph.node_concept:
        node_types_list.append(graph.node_concept._name)
    else:
        for c in graph.node_concepts:
            node_types_list.append(c._name)
    
    node_types_lua = "{" + ", ".join(f'"{t}"' for t in node_types_list) + "}"
    
    # 2. Determine edge structure (simple direct vs intermediate pattern 3)
    edge_concept = ""
    edge_type = graph.edge_type
    src_rel = ""
    dst_rel = ""
    weight_prop = "weight"
    
    if graph.edge_concept:
        edge_concept = graph.edge_concept._name
        edge_type = ""
        if graph.edge_src_relationship:
            src_rel = graph.edge_src_relationship.name.upper()
        if graph.edge_dst_relationship:
            dst_rel = graph.edge_dst_relationship.name.upper()
        if graph.edge_weight_relationship:
            weight_prop = graph.edge_weight_relationship.name
    elif graph.edge_weight_relationship:
        weight_prop = graph.edge_weight_relationship.name

    directed_lua = str(graph.directed).lower()
    weighted_lua = str(graph.weighted).lower()
    
    algo_name = expr.name
    
    if algo_name == "pagerank":
        damping = expr.kwargs.get("damping", 0.85)
        iterations = expr.kwargs.get("iterations", 20)
        tolerance = expr.kwargs.get("tolerance", 1e-6)
        return f"""
        local res = PageRank("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua}, {damping}, {iterations}, {tolerance})
        for node_id, score in pairs(res) do
            NodeSetProperty(tonumber(node_id), "{property_name}", score)
        end
        """
        
    elif algo_name == "eigenvector_centrality":
        iterations = expr.kwargs.get("iterations", 20)
        tolerance = expr.kwargs.get("tolerance", 1e-6)
        return f"""
        local res = EigenvectorCentrality("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua}, {iterations}, {tolerance})
        for node_id, score in pairs(res) do
            NodeSetProperty(tonumber(node_id), "{property_name}", score)
        end
        """
        
    elif algo_name == "betweenness_centrality":
        return f"""
        local res = BetweennessCentrality("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        for node_id, score in pairs(res) do
            NodeSetProperty(tonumber(node_id), "{property_name}", score)
        end
        """
        
    elif algo_name == "degree_centrality":
        dir_val = expr.kwargs.get("direction", "BOTH").upper()
        return f"""
        local res = DegreeCentrality("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua}, Direction.{dir_val})
        for node_id, score in pairs(res) do
            NodeSetProperty(tonumber(node_id), "{property_name}", score)
        end
        """
        
    elif algo_name in ("jaccard_similarity", "cosine_similarity", "adamic_adar", "preferential_attachment"):
        cpp_name = {
            "jaccard_similarity": "JaccardSimilarity",
            "cosine_similarity": "CosineSimilarity",
            "adamic_adar": "AdamicAdar",
            "preferential_attachment": "PreferentialAttachment"
        }[algo_name]
        return f"""
        local res = {cpp_name}("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        for _, tup in ipairs(res) do
            local u_id = tup[1]
            local v_id = tup[2]
            local score = tup[3]
            if score > 0.0001 then
                local props = '{{"score": ' .. score .. '}}'
                RelationshipAdd("{property_name}", u_id, v_id, props)
                RelationshipAdd("{property_name}", v_id, u_id, props)
            end
        end
        """
        
    elif algo_name in ("louvain", "leiden", "infomap", "label_propagation", "weakly_connected_component"):
        cpp_name = {
            "louvain": "Louvain",
            "leiden": "Leiden",
            "infomap": "Infomap",
            "label_propagation": "LabelPropagation",
            "weakly_connected_component": "WeaklyConnectedComponents"
        }[algo_name]
        return f"""
        local res = {cpp_name}("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        for node_id, comm in pairs(res) do
            NodeSetProperty(tonumber(node_id), "{property_name}", comm)
        end
        """
        
    elif algo_name in ("triangle", "unique_triangle"):
        cpp_name = {
            "triangle": "Triangles",
            "unique_triangle": "UniqueTriangles"
        }[algo_name]
        return f"""
        local res = {cpp_name}("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        for _, tup in ipairs(res) do
            local u_id = tup[1]
            local v_id = tup[2]
            local w_id = tup[3]
            local props = '{{"node3": "' .. tostring(w_id) .. '"}}'
            RelationshipAdd("{property_name}", u_id, v_id, props)
        end
        """
        
    elif algo_name == "triangle_count":
        return f"""
        local res = Triangles("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        local counts = {{}}
        for _, tup in ipairs(res) do
            local u = tup[1]
            counts[u] = (counts[u] or 0) + 0.5
        end
        local node_types = {node_types_lua}
        for _, t in ipairs(node_types) do
            local count = AllNodesCount(t)
            local ids = AllNodeIds(t, 0, count)
            for _, id in ipairs(ids) do
                local cnt = counts[id] or 0
                NodeSetProperty(id, "{property_name}", math.floor(cnt + 0.5))
            end
        end
        """
        
    elif algo_name == "local_clustering_coefficient":
        return f"""
        local res = LocalClusteringCoefficient("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        for node_id, score in pairs(res) do
            NodeSetProperty(tonumber(node_id), "{property_name}", score)
        end
        """
        
    elif algo_name == "average_clustering_coefficient":
        return f"""
        local avg = AverageClusteringCoefficient("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        local node_types = {node_types_lua}
        for _, t in ipairs(node_types) do
            local count = AllNodesCount(t)
            local ids = AllNodeIds(t, 0, count)
            for _, id in ipairs(ids) do
                NodeSetProperty(id, "{property_name}", avg)
            end
        end
        """
        
    elif algo_name == "reachable":
        return f"""
        local res = Reachable("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        for _, pair in ipairs(res) do
            local u_id = pair.first or pair[1]
            local v_id = pair.second or pair[2]
            RelationshipAdd("{property_name}", u_id, v_id, '{{}}')
        end
        """
        
    elif algo_name == "is_connected":
        return f"""
        local is_conn = IsConnected("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        local node_types = {node_types_lua}
        for _, t in ipairs(node_types) do
            local count = AllNodesCount(t)
            local ids = AllNodeIds(t, 0, count)
            for _, id in ipairs(ids) do
                NodeSetProperty(id, "{property_name}", is_conn)
            end
        end
        """
        
    elif algo_name == "distance":
        return f"""
        local res = Distance("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        for _, tup in ipairs(res) do
            local u_id = tup[1]
            local v_id = tup[2]
            local dist = tup[3]
            local props = '{{"distance": ' .. dist .. '}}'
            RelationshipAdd("{property_name}", u_id, v_id, props)
        end
        """
        
    elif algo_name == "diameter_range":
        return f"""
        local res = DiameterRange("{edge_type}", "{edge_concept}", "{src_rel}", "{dst_rel}", "{weight_prop}", {directed_lua}, {weighted_lua})
        local max_dist = res.second or res[2]
        local node_types = {node_types_lua}
        for _, t in ipairs(node_types) do
            local count = AllNodesCount(t)
            local ids = AllNodeIds(t, 0, count)
            for _, id in ipairs(ids) do
                NodeSetProperty(id, "{property_name}", max_dist)
            end
        end
        """
        
    return ""
