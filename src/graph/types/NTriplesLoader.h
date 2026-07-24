/*
 * Copyright RageDB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef RAGEDB_NTRIPLESLOADER_H
#define RAGEDB_NTRIPLESLOADER_H

#include "NTriplesParser.h"
#include "../Shard.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <fstream>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace ragedb {

    namespace ntriples_detail {

        /// The last path segment of an IRI (after the final '#' or '/') -- the label/property/rel name.
        inline std::string local_name(const std::string &iri) {
            size_t pos = std::string::npos;
            auto h = iri.find_last_of('#');
            auto s = iri.find_last_of('/');
            if (h != std::string::npos) pos = h;
            if (s != std::string::npos && (pos == std::string::npos || s > pos)) pos = s;
            return pos == std::string::npos ? iri : iri.substr(pos + 1);
        }

        /// Maps an xsd datatype IRI to a ragedb property type string.
        inline std::string datatype_to_ragedb(const std::string &datatype) {
            std::string ln = local_name(datatype);
            if (ln == "integer" || ln == "int" || ln == "long" || ln == "short" || ln == "nonNegativeInteger")
                return "integer";
            if (ln == "decimal" || ln == "double" || ln == "float") return "double";
            if (ln == "boolean") return "boolean";
            if (ln == "dateTime" || ln == "date") return "date";
            return "string";
        }

        inline std::string json_escape(const std::string &s) {
            std::string out;
            for (char c : s) {
                if (c == '"' || c == '\\') { out.push_back('\\'); out.push_back(c); }
                else if (c == '\n') out += "\\n";
                else if (c == '\t') out += "\\t";
                else if (c == '\r') out += "\\r";
                else out.push_back(c);
            }
            return out;
        }

        /// Renders a literal object as a JSON value: bare for numeric/boolean, quoted otherwise.
        inline std::string json_value(const NTerm &lit) {
            std::string t = lit.datatype.empty() ? "string" : datatype_to_ragedb(lit.datatype);
            if (t == "integer" || t == "double" || t == "boolean") return lit.value;
            return "\"" + json_escape(lit.value) + "\"";
        }

    } // namespace ntriples_detail

    /// The rdf:type predicate -- its object gives a subject its node label.
    inline const std::string kRdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    /// Label given to an IRI/blank node that carries no rdf:type.
    inline const std::string kDefaultLabel = "Resource";

    /**
     * @brief Load parsed RDF triples into the property graph. A subject/IRI-object becomes a node keyed by
     * its IRI (label from rdf:type, else "Resource"); a literal object becomes a typed property; an
     * IRI/blank object becomes a relationship (type = the predicate's local name). Two passes: derive the
     * schema and per-node properties, then declare types before creating nodes and relationships.
     */
    inline seastar::future<uint64_t> load_ntriples(Shard &shard, std::vector<NTriple> triples) {  // by value:
        using namespace ntriples_detail;                      // the coroutine frame owns it past a suspend

        std::map<std::string, std::string> node_label;                         // iri -> label
        std::set<std::string> node_iris;                                       // subjects + IRI/blank objects
        std::map<std::string, std::map<std::string, std::string>> node_props;  // iri -> {prop -> json value}
        std::set<std::string> rel_types;
        struct Edge { std::string subj, rel, obj; };
        std::vector<Edge> edges;

        for (const auto &t : triples) {
            if (t.subject.kind == NTerm::LITERAL) continue;                    // a subject is never a literal
            const std::string &subj = t.subject.value;
            node_iris.insert(subj);
            if (t.predicate.value == kRdfType && t.object.kind == NTerm::IRI) {
                node_label[subj] = local_name(t.object.value);
            } else if (t.object.kind == NTerm::LITERAL) {
                node_props[subj][local_name(t.predicate.value)] = json_value(t.object);
            } else {
                std::string rel = local_name(t.predicate.value);
                rel_types.insert(rel);
                node_iris.insert(t.object.value);
                edges.push_back({subj, rel, t.object.value});
            }
        }

        auto label_of = [&](const std::string &iri) -> std::string {
            auto it = node_label.find(iri);
            return it != node_label.end() ? it->second : kDefaultLabel;
        };

        // (label, property) -> ragedb type, derived from the literal objects (uniform data -> last wins).
        std::map<std::pair<std::string, std::string>, std::string> prop_type;
        for (const auto &t : triples) {
            if (t.subject.kind != NTerm::LITERAL && t.object.kind == NTerm::LITERAL && t.predicate.value != kRdfType) {
                std::string rt = t.object.datatype.empty() ? "string" : datatype_to_ragedb(t.object.datatype);
                prop_type[{label_of(t.subject.value), local_name(t.predicate.value)}] = rt;
            }
        }
        std::set<std::string> labels;
        for (const auto &iri : node_iris) labels.insert(label_of(iri));

        // Pass 2: declare the schema (types + property types + rel types) before creating any node.
        for (const auto &label : labels) {
            co_await shard.NodeTypeInsertPeered(label);
        }
        for (const auto &[lp, rt] : prop_type) {
            co_await shard.NodePropertyTypeAddPeered(lp.first, lp.second, rt);
        }
        for (const auto &rel : rel_types) {
            co_await shard.RelationshipTypeInsertPeered(rel);
        }

        std::map<std::string, uint64_t> iri_to_id;
        for (const auto &iri : node_iris) {
            std::string json = "{";
            bool first = true;
            auto pit = node_props.find(iri);
            if (pit != node_props.end()) {
                for (const auto &[prop, val] : pit->second) {
                    if (!first) json += ",";
                    json += "\"" + prop + "\":" + val;
                    first = false;
                }
            }
            json += "}";
            uint64_t id = co_await shard.NodeAddPeered(label_of(iri), iri, json);
            iri_to_id[iri] = id;
        }

        for (const auto &e : edges) {
            auto f = iri_to_id.find(e.subj);
            auto o = iri_to_id.find(e.obj);
            if (f == iri_to_id.end() || o == iri_to_id.end()) continue;
            co_await shard.RelationshipAddPeered(e.rel, f->second, o->second, "{}");
        }
        co_return static_cast<uint64_t>(iri_to_id.size());  // nodes created
    }

    /**
     * @brief Read an N-Triples/N-Quads file line by line and load it. A one-time bulk load, so a blocking
     * read (as in the CSV loader) is acceptable; the two-pass mapping needs all triples in memory to derive
     * the schema before creating nodes. `shard` must outlive the returned future.
     */
    inline seastar::future<uint64_t> load_ntriples_file(Shard &shard, const std::string &path) {
        std::ifstream in(path);
        if (!in) {
            return seastar::make_exception_future<uint64_t>(std::runtime_error("load_ntriples: cannot open " + path));
        }
        std::vector<NTriple> triples;
        std::string line;
        while (std::getline(in, line)) {
            auto t = parse_ntriple_line(line);
            if (t) triples.push_back(std::move(*t));
        }
        return load_ntriples(shard, std::move(triples));
    }

} // namespace ragedb

#endif // RAGEDB_NTRIPLESLOADER_H
