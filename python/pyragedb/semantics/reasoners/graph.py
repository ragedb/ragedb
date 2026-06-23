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
        elif name in ("louvain", "infomap", "label_propagation", "weakly_connected_component"):
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
    on RageDB and stores or creates the results.
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
    edge_concept_lua = "nil"
    edge_type_lua = f'"{graph.edge_type}"'
    src_rel_lua = "nil"
    dst_rel_lua = "nil"
    weight_prop_lua = '"weight"'
    
    if graph.edge_concept:
        edge_concept_lua = f'"{graph.edge_concept._name}"'
        edge_type_lua = "nil"
        if graph.edge_src_relationship:
            src_rel_lua = f'"{graph.edge_src_relationship.name.upper()}"'
        if graph.edge_dst_relationship:
            dst_rel_lua = f'"{graph.edge_dst_relationship.name.upper()}"'
        if graph.edge_weight_relationship:
            weight_prop_lua = f'"{graph.edge_weight_relationship.name}"'

    # Build neighbor traversal function in Lua
    get_neighbors_lua = f"""
    local directed = {str(graph.directed).lower()}
    local weighted = {str(graph.weighted).lower()}
    local edge_type = {edge_type_lua}
    local edge_concept = {edge_concept_lua}
    local src_rel = {src_rel_lua}
    local dst_rel = {dst_rel_lua}
    local weight_prop = {weight_prop_lua}

    local function get_neighbors(u_id)
        local result = {{}}
        if edge_concept == nil then
            local dir = directed and Direction.OUT or Direction.BOTH
            local rels = NodeGetRelationships(u_id, dir, edge_type)
            for _, rel in ipairs(rels) do
                local neighbor_id = (rel.starting_node_id == u_id) and rel.ending_node_id or rel.starting_node_id
                local w = 1.0
                if weighted then
                    w = RelationshipGetProperty(rel.id, weight_prop) or 1.0
                end
                table.insert(result, {{id = neighbor_id, weight = w}})
            end
        else
            -- Intermediate node pattern 3
            local in_rels = NodeGetRelationships(u_id, Direction.IN, src_rel)
            for _, r1 in ipairs(in_rels) do
                local e = r1.starting_node_id
                local out_rels = NodeGetRelationships(e, Direction.OUT, dst_rel)
                for _, r2 in ipairs(out_rels) do
                    local v = r2.ending_node_id
                    local w = 1.0
                    if weighted then
                        w = NodeGetProperty(e, weight_prop) or 1.0
                    end
                    table.insert(result, {{id = v, weight = w}})
                end
            end
            if not directed then
                local in_rels = NodeGetRelationships(u_id, Direction.IN, dst_rel)
                for _, r1 in ipairs(in_rels) do
                    local e = r1.starting_node_id
                    local out_rels = NodeGetRelationships(e, Direction.OUT, src_rel)
                    for _, r2 in ipairs(out_rels) do
                        local v = r2.ending_node_id
                        local w = 1.0
                        if weighted then
                            w = NodeGetProperty(e, weight_prop) or 1.0
                        end
                        table.insert(result, {{id = v, weight = w}})
                    end
                end
            end
        end
        return result
    end
    """

    # Gathers all nodes in the graph
    gather_nodes_lua = f"""
    local node_types = {node_types_lua}
    local nodes = {{}}
    for _, t in ipairs(node_types) do
        local count = AllNodesCount(t)
        local ids = AllNodeIds(t, 0, count)
        for _, id in ipairs(ids) do
            table.insert(nodes, id)
        end
    end
    """

    # Generate core algorithm body
    algo_name = expr.name
    algo_body = ""

    if algo_name == "pagerank":
        damping = expr.kwargs.get("damping", 0.85)
        iterations = expr.kwargs.get("iterations", 20)
        algo_body = f"""
        local pr = {{}}
        local next_pr = {{}}
        local damping = {damping}
        local iterations = {iterations}
        local num_nodes = #nodes

        for _, node_id in ipairs(nodes) do
            pr[node_id] = 1.0 / num_nodes
        end

        for iter = 1, iterations do
            local sink_value = 0.0
            for _, node_id in ipairs(nodes) do
                next_pr[node_id] = (1.0 - damping) / num_nodes
                local neighbors = get_neighbors(node_id)
                if #neighbors == 0 then
                    sink_value = sink_value + damping * pr[node_id] / num_nodes
                end
            end

            for _, u_id in ipairs(nodes) do
                local neighbors = get_neighbors(u_id)
                for _, n in ipairs(neighbors) do
                    local out_deg = #neighbors
                    next_pr[n.id] = next_pr[n.id] + damping * pr[u_id] * n.weight / out_deg
                end
            end

            for _, node_id in ipairs(nodes) do
                pr[node_id] = next_pr[node_id] + sink_value
            end
        end

        for node_id, val in pairs(pr) do
            NodeSetProperty(node_id, "{property_name}", val)
        end
        """

    elif algo_name == "weakly_connected_component":
        algo_body = f"""
        local parent = {{}}
        for _, node_id in ipairs(nodes) do
            parent[node_id] = node_id
        end

        local function find(i)
            local path = {{}}
            while parent[i] ~= i do
                table.insert(path, i)
                i = parent[i]
            end
            for _, node_id in ipairs(path) do
                parent[node_id] = i
            end
            return i
        end

        local function union(i, j)
            local root_i = find(i)
            local root_j = find(j)
            if root_i ~= root_j then
                if root_i < root_j then
                    parent[root_j] = root_i
                else
                    parent[root_i] = root_j
                end
            end
        end

        for _, u_id in ipairs(nodes) do
            local neighbors = get_neighbors(u_id)
            for _, n in ipairs(neighbors) do
                union(u_id, n.id)
            end
        end

        for _, node_id in ipairs(nodes) do
            local comp_id = find(node_id)
            NodeSetProperty(node_id, "{property_name}", comp_id)
        end
        """

    elif algo_name in ("label_propagation", "louvain", "infomap"):
        algo_body = f"""
        local labels = {{}}
        for _, node_id in ipairs(nodes) do
            labels[node_id] = node_id
        end

        local iterations = 10
        for iter = 1, iterations do
            local changed = false
            for _, u_id in ipairs(nodes) do
                local neighbors = get_neighbors(u_id)
                if #neighbors > 0 then
                    local counts = {{}}
                    for _, n in ipairs(neighbors) do
                        local lbl = labels[n.id]
                        counts[lbl] = (counts[lbl] or 0) + n.weight
                    end
                    local max_lbl = labels[u_id]
                    local max_count = 0
                    for lbl, count in pairs(counts) do
                        if count > max_count then
                            max_count = count
                            max_lbl = lbl
                        end
                    end
                    if labels[u_id] ~= max_lbl then
                        labels[u_id] = max_lbl
                        changed = true
                    end
                end
            end
            if not changed then break end
        end

        for _, node_id in ipairs(nodes) do
            NodeSetProperty(node_id, "{property_name}", labels[node_id])
        end
        """

    elif algo_name == "degree_centrality":
        dir_val = expr.kwargs.get("direction", "BOTH")
        algo_body = f"""
        local num_nodes = #nodes
        for _, u_id in ipairs(nodes) do
            local neighbors = get_neighbors(u_id)
            local val = (num_nodes > 1) and (#neighbors / (num_nodes - 1)) or 0.0
            NodeSetProperty(u_id, "{property_name}", val)
        end
        """

    elif algo_name == "eigenvector_centrality":
        iterations = expr.kwargs.get("iterations", 20)
        algo_body = f"""
        local x = {{}}
        for _, node_id in ipairs(nodes) do x[node_id] = 1.0 end

        local iterations = {iterations}
        for iter = 1, iterations do
            local next_x = {{}}
            local norm = 0.0
            for _, u_id in ipairs(nodes) do
                local sum = 0.0
                local neighbors = get_neighbors(u_id)
                for _, n in ipairs(neighbors) do
                    sum = sum + x[n.id] * n.weight
                end
                next_x[u_id] = sum
                norm = norm + sum * sum
            end
            norm = math.sqrt(norm)
            if norm > 0 then
                for _, node_id in ipairs(nodes) do
                    x[node_id] = next_x[node_id] / norm
                end
            else
                break
            end
        end

        for _, node_id in ipairs(nodes) do
            NodeSetProperty(node_id, "{property_name}", x[node_id])
        end
        """

    elif algo_name == "betweenness_centrality":
        algo_body = f"""
        local BC = {{}}
        for _, s in ipairs(nodes) do BC[s] = 0.0 end
        
        for _, s in ipairs(nodes) do
            local S = {{}}
            local P = {{}}
            for _, w in ipairs(nodes) do P[w] = {{}} end
            
            local sigma = {{}}
            for _, w in ipairs(nodes) do sigma[w] = 0.0 end
            sigma[s] = 1.0
            
            local d = {{}}
            for _, w in ipairs(nodes) do d[w] = -1 end
            d[s] = 0
            
            local Q = {{s}}
            while #Q > 0 do
                local v = table.remove(Q, 1)
                table.insert(S, v)
                local neighbors = get_neighbors(v)
                for _, n in ipairs(neighbors) do
                    local w = n.id
                    if d[w] < 0 then
                        table.insert(Q, w)
                        d[w] = d[v] + 1
                    end
                    if d[w] == d[v] + 1 then
                        sigma[w] = sigma[w] + sigma[v]
                        table.insert(P[w], v)
                    end
                end
            end
            
            local delta = {{}}
            for _, w in ipairs(nodes) do delta[w] = 0.0 end
            while #S > 0 do
                local w = S[#S]
                table.remove(S)
                for _, v in ipairs(P[w]) do
                    delta[v] = delta[v] + (sigma[v] / sigma[w]) * (1.0 + delta[w])
                end
                if w ~= s then
                    BC[w] = BC[w] + delta[w]
                end
            end
        end

        for _, node_id in ipairs(nodes) do
            NodeSetProperty(node_id, "{property_name}", BC[node_id])
        end
        """

    elif algo_name in ("jaccard_similarity", "cosine_similarity", "adamic_adar", "preferential_attachment"):
        # Ternary relationship generation
        calc_logic = ""
        if algo_name == "jaccard_similarity":
            calc_logic = """
            local intersect_count = 0
            local union_count = 0
            for nid in pairs(u_neighbors) do
                if v_neighbors[nid] then
                    intersect_count = intersect_count + 1
                end
                union_count = union_count + 1
            end
            for nid in pairs(v_neighbors) do
                if not u_neighbors[nid] then
                    union_count = union_count + 1
                end
            end
            score = (union_count > 0) and (intersect_count / union_count) or 0.0
            """
        elif algo_name == "cosine_similarity":
            calc_logic = """
            local intersect_count = 0
            local u_deg = 0
            local v_deg = 0
            for nid in pairs(u_neighbors) do
                if v_neighbors[nid] then
                    intersect_count = intersect_count + 1
                end
                u_deg = u_deg + 1
            end
            for nid in pairs(v_neighbors) do
                v_deg = v_deg + 1
            end
            score = (u_deg > 0 and v_deg > 0) and (intersect_count / math.sqrt(u_deg * v_deg)) or 0.0
            """
        elif algo_name == "adamic_adar":
            calc_logic = """
            local sum = 0.0
            for nid in pairs(u_neighbors) do
                if v_neighbors[nid] then
                    local deg = NodeGetDegree(nid)
                    if deg > 1 then
                        sum = sum + 1.0 / math.log(deg)
                    end
                end
            end
            score = sum
            """
        elif algo_name == "preferential_attachment":
            calc_logic = """
            local u_deg = 0
            local v_deg = 0
            for nid in pairs(u_neighbors) do u_deg = u_deg + 1 end
            for nid in pairs(v_neighbors) do v_deg = v_deg + 1 end
            score = u_deg * v_deg
            """

        algo_body = f"""
        local neighbor_ids = {{}}
        for _, u_id in ipairs(nodes) do
            local neighbors = get_neighbors(u_id)
            local n_map = {{}}
            for _, n in ipairs(neighbors) do n_map[n.id] = true end
            neighbor_ids[u_id] = n_map
        end

        for i = 1, #nodes do
            local u_id = nodes[i]
            local u_neighbors = neighbor_ids[u_id]
            for j = i + 1, #nodes do
                local v_id = nodes[j]
                local v_neighbors = neighbor_ids[v_id]
                local score = 0.0
                {calc_logic}
                if score > 0.0001 then
                    local props = '{{"score": ' .. score .. '}}'
                    RelationshipAdd("{property_name}", u_id, v_id, props)
                    RelationshipAdd("{property_name}", v_id, u_id, props)
                end
            end
        end
        """

    elif algo_name in ("triangle", "unique_triangle"):
        algo_body = f"""
        local neighbor_ids = {{}}
        for _, u_id in ipairs(nodes) do
            local neighbors = get_neighbors(u_id)
            local n_map = {{}}
            for _, n in ipairs(neighbors) do n_map[n.id] = true end
            neighbor_ids[u_id] = n_map
        end

        local unique = {"true" if algo_name == "unique_triangle" else "false"}

        for i = 1, #nodes do
            local u_id = nodes[i]
            local u_neighbors = neighbor_ids[u_id]
            for v_id, _ in pairs(u_neighbors) do
                local v_neighbors = neighbor_ids[v_id]
                if v_neighbors then
                    for w_id, _ in pairs(v_neighbors) do
                        if u_neighbors[w_id] then
                            if unique then
                                if u_id < v_id and v_id < w_id then
                                    local props = '{{"node3": "' .. tostring(w_id) .. '"}}'
                                    RelationshipAdd("{property_name}", u_id, v_id, props)
                                end
                            else
                                if u_id ~= v_id and v_id ~= w_id and u_id ~= w_id then
                                    local props = '{{"node3": "' .. tostring(w_id) .. '"}}'
                                    RelationshipAdd("{property_name}", u_id, v_id, props)
                                end
                            end
                        end
                    end
                end
            end
        end
        """

    elif algo_name == "triangle_count":
        algo_body = f"""
        for _, u_id in ipairs(nodes) do
            local count = 0
            local neighbors = get_neighbors(u_id)
            for i = 1, #neighbors do
                local v = neighbors[i].id
                for j = i + 1, #neighbors do
                    local w = neighbors[j].id
                    local connected = false
                    local v_neighbors = get_neighbors(v)
                    for _, vn in ipairs(v_neighbors) do
                        if vn.id == w then connected = true; break end
                    end
                    if connected then count = count + 1 end
                end
            end
            NodeSetProperty(u_id, "{property_name}", count)
        end
        """

    elif algo_name == "local_clustering_coefficient":
        algo_body = f"""
        for _, u_id in ipairs(nodes) do
            local count = 0
            local neighbors = get_neighbors(u_id)
            local deg = #neighbors
            for i = 1, #neighbors do
                local v = neighbors[i].id
                for j = i + 1, #neighbors do
                    local w = neighbors[j].id
                    local connected = false
                    local v_neighbors = get_neighbors(v)
                    for _, vn in ipairs(v_neighbors) do
                        if vn.id == w then connected = true; break end
                    end
                    if connected then count = count + 1 end
                end
            end
            local val = (deg > 1) and (2.0 * count / (deg * (deg - 1))) or 0.0
            NodeSetProperty(u_id, "{property_name}", val)
        end
        """

    elif algo_name == "average_clustering_coefficient":
        algo_body = f"""
        local sum = 0.0
        local num_nodes = #nodes
        for _, u_id in ipairs(nodes) do
            local count = 0
            local neighbors = get_neighbors(u_id)
            local deg = #neighbors
            for i = 1, #neighbors do
                local v = neighbors[i].id
                for j = i + 1, #neighbors do
                    local w = neighbors[j].id
                    local connected = false
                    local v_neighbors = get_neighbors(v)
                    for _, vn in ipairs(v_neighbors) do
                        if vn.id == w then connected = true; break end
                    end
                    if connected then count = count + 1 end
                end
            end
            local val = (deg > 1) and (2.0 * count / (deg * (deg - 1))) or 0.0
            sum = sum + val
        end
        local avg = (num_nodes > 0) and (sum / num_nodes) or 0.0
        for _, u_id in ipairs(nodes) do
            NodeSetProperty(u_id, "{property_name}", avg)
        end
        """

    elif algo_name == "reachable":
        algo_body = f"""
        for _, s_id in ipairs(nodes) do
            local visited = {{[s_id] = true}}
            local queue = {{s_id}}
            while #queue > 0 do
                local u = table.remove(queue, 1)
                local neighbors = get_neighbors(u)
                for _, n in ipairs(neighbors) do
                    if not visited[n.id] then
                        visited[n.id] = true
                        table.insert(queue, n.id)
                        RelationshipAdd("{property_name}", s_id, n.id, '{{}}')
                    end
                end
            end
        end
        """

    elif algo_name == "distance":
        algo_body = f"""
        for _, s_id in ipairs(nodes) do
            local dist = {{[s_id] = 0}}
            local queue = {{s_id}}
            while #queue > 0 do
                local u = table.remove(queue, 1)
                local neighbors = get_neighbors(u)
                for _, n in ipairs(neighbors) do
                    if dist[n.id] == nil then
                        dist[n.id] = dist[u] + 1
                        table.insert(queue, n.id)
                        local props = '{{"distance": ' .. dist[n.id] .. '}}'
                        RelationshipAdd("{property_name}", s_id, n.id, props)
                    end
                end
            end
        end
        """

    elif algo_name == "is_connected":
        algo_body = f"""
        local is_connected = true
        if #nodes > 0 then
            local visited = {{[nodes[1]] = true}}
            local queue = {{nodes[1]}}
            local count = 1
            while #queue > 0 do
                local u = table.remove(queue, 1)
                local neighbors = get_neighbors(u)
                for _, n in ipairs(neighbors) do
                    if not visited[n.id] then
                        visited[n.id] = true
                        count = count + 1
                        table.insert(queue, n.id)
                    end
                end
            end
            is_connected = (count == #nodes)
        end
        for _, u_id in ipairs(nodes) do
            NodeSetProperty(u_id, "{property_name}", is_connected)
        end
        """

    elif algo_name == "diameter_range":
        algo_body = f"""
        local max_dist = 0
        for _, s_id in ipairs(nodes) do
            local dist = {{[s_id] = 0}}
            local queue = {{s_id}}
            while #queue > 0 do
                local u = table.remove(queue, 1)
                local neighbors = get_neighbors(u)
                for _, n in ipairs(neighbors) do
                    if dist[n.id] == nil then
                        dist[n.id] = dist[u] + 1
                        if dist[n.id] > max_dist then
                            max_dist = dist[n.id]
                        end
                        table.insert(queue, n.id)
                    end
                end
            end
        end
        for _, u_id in ipairs(nodes) do
            NodeSetProperty(u_id, "{property_name}", max_dist)
        end
        """

    else:
        # Default fallback
        algo_body = f"""
        for _, u_id in ipairs(nodes) do
            NodeSetProperty(u_id, "{property_name}", 0.0)
        end
        """

    return get_neighbors_lua + gather_nodes_lua + algo_body
