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

#ifndef RAGEDB_NTRIPLESPARSER_H
#define RAGEDB_NTRIPLESPARSER_H

#include <cctype>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace ragedb {

    /**
     * @brief One parsed RDF term from an N-Triples / N-Quads line.
     *
     * The lexical parsing layer only; mapping terms onto the property-graph model (interning IRIs to
     * nodes, rdf:type -> label, literal object -> property, IRI object -> relationship) is a separate step.
     */
    struct NTerm {
        enum Kind { IRI, BLANK, LITERAL } kind = IRI;
        std::string value;     ///< IRI (without <>), blank-node id (without _:), or a literal's lexical value.
        std::string datatype;  ///< A literal's datatype IRI (empty when absent or language-tagged).
        std::string lang;      ///< A literal's language tag, lowercased (empty when absent).
    };

    /**
     * @brief One RDF statement: subject predicate object [graph]. `graph` is the N-Quads graph-label IRI
     * (empty for N-Triples).
     */
    struct NTriple {
        NTerm subject;
        NTerm predicate;
        NTerm object;
        std::string graph;
    };

    namespace ntriples_detail {

        inline void skip_ws(std::string_view s, size_t &i) {
            while (i < s.size() && (s[i] == ' ' || s[i] == '\t')) ++i;
        }

        /// Appends the UTF-8 encoding of a Unicode code point.
        inline void append_utf8(std::string &out, uint32_t cp) {
            if (cp <= 0x7F) {
                out.push_back(static_cast<char>(cp));
            } else if (cp <= 0x7FF) {
                out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else if (cp <= 0xFFFF) {
                out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            } else {
                out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
                out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
            }
        }

        inline uint32_t hex_run(std::string_view s, size_t &i, int digits) {
            uint32_t cp = 0;
            for (int k = 0; k < digits; ++k) {
                if (i >= s.size()) throw std::runtime_error("N-Triples: truncated \\u escape");
                char c = s[i++];
                uint32_t d;
                if (c >= '0' && c <= '9') d = static_cast<uint32_t>(c - '0');
                else if (c >= 'a' && c <= 'f') d = static_cast<uint32_t>(c - 'a' + 10);
                else if (c >= 'A' && c <= 'F') d = static_cast<uint32_t>(c - 'A' + 10);
                else throw std::runtime_error("N-Triples: bad hex digit in escape");
                cp = (cp << 4) | d;
            }
            return cp;
        }

        /// Reads a `<IRI>` at s[i] ('<' already at i); returns the IRI content with \\uXXXX resolved.
        inline std::string read_iri(std::string_view s, size_t &i) {
            ++i; // consume '<'
            std::string out;
            while (i < s.size()) {
                char c = s[i++];
                if (c == '>') return out;
                if (c == '\\') {
                    if (i >= s.size()) break;
                    char e = s[i++];
                    if (e == 'u') append_utf8(out, hex_run(s, i, 4));
                    else if (e == 'U') append_utf8(out, hex_run(s, i, 8));
                    else out.push_back(e);
                } else {
                    out.push_back(c);
                }
            }
            throw std::runtime_error("N-Triples: unterminated IRI");
        }

        /// Reads a `"literal"` at s[i] ('"' already at i); returns the unescaped lexical value.
        inline std::string read_literal_value(std::string_view s, size_t &i) {
            ++i; // consume opening '"'
            std::string out;
            while (i < s.size()) {
                char c = s[i++];
                if (c == '"') return out;
                if (c == '\\') {
                    if (i >= s.size()) break;
                    char e = s[i++];
                    switch (e) {
                        case 't': out.push_back('\t'); break;
                        case 'b': out.push_back('\b'); break;
                        case 'n': out.push_back('\n'); break;
                        case 'r': out.push_back('\r'); break;
                        case 'f': out.push_back('\f'); break;
                        case '"': out.push_back('"'); break;
                        case '\'': out.push_back('\''); break;
                        case '\\': out.push_back('\\'); break;
                        case 'u': append_utf8(out, hex_run(s, i, 4)); break;
                        case 'U': append_utf8(out, hex_run(s, i, 8)); break;
                        default: out.push_back(e); break;
                    }
                } else {
                    out.push_back(c);
                }
            }
            throw std::runtime_error("N-Triples: unterminated literal");
        }

        /// Reads a blank node `_:id` at s[i] ('_' already at i); returns the id (without `_:`).
        inline std::string read_blank(std::string_view s, size_t &i) {
            ++i; // '_'
            if (i >= s.size() || s[i] != ':') throw std::runtime_error("N-Triples: expected ':' in blank node");
            ++i; // ':'
            size_t start = i;
            while (i < s.size() && s[i] != ' ' && s[i] != '\t' && s[i] != '.') ++i;
            return std::string(s.substr(start, i - start));
        }

        /// Reads a subject/predicate/graph term (IRI or blank node -- never a literal).
        inline NTerm read_node_term(std::string_view s, size_t &i) {
            skip_ws(s, i);
            if (i >= s.size()) throw std::runtime_error("N-Triples: expected a term");
            NTerm t;
            if (s[i] == '<') { t.kind = NTerm::IRI; t.value = read_iri(s, i); }
            else if (s[i] == '_') { t.kind = NTerm::BLANK; t.value = read_blank(s, i); }
            else throw std::runtime_error("N-Triples: expected an IRI or blank node");
            return t;
        }

    } // namespace ntriples_detail

    /**
     * @brief Parse one N-Triples or N-Quads line. Returns nullopt for a blank line or a `#` comment;
     * throws std::runtime_error on a malformed statement. Handles IRIs, blank nodes, literals with an
     * optional `@lang` or `^^<datatype>`, standard string escapes, and an optional N-Quads graph label.
     */
    inline std::optional<NTriple> parse_ntriple_line(std::string_view line) {
        using namespace ntriples_detail;
        size_t i = 0;
        skip_ws(line, i);
        if (i >= line.size() || line[i] == '#') return std::nullopt;

        NTriple tr;
        tr.subject = read_node_term(line, i);
        tr.predicate = read_node_term(line, i);

        // Object: IRI, blank node, or literal.
        skip_ws(line, i);
        if (i >= line.size()) throw std::runtime_error("N-Triples: expected an object");
        if (line[i] == '<') {
            tr.object.kind = NTerm::IRI;
            tr.object.value = read_iri(line, i);
        } else if (line[i] == '_') {
            tr.object.kind = NTerm::BLANK;
            tr.object.value = read_blank(line, i);
        } else if (line[i] == '"') {
            tr.object.kind = NTerm::LITERAL;
            tr.object.value = read_literal_value(line, i);
            if (i < line.size() && line[i] == '@') {
                ++i;
                size_t start = i;
                while (i < line.size() && line[i] != ' ' && line[i] != '\t' && line[i] != '.') ++i;
                tr.object.lang = std::string(line.substr(start, i - start));
                for (auto &c : tr.object.lang) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            } else if (i + 1 < line.size() && line[i] == '^' && line[i + 1] == '^') {
                i += 2;
                skip_ws(line, i);
                if (i >= line.size() || line[i] != '<') throw std::runtime_error("N-Triples: expected <datatype> after ^^");
                tr.object.datatype = read_iri(line, i);
            }
        } else {
            throw std::runtime_error("N-Triples: expected an IRI, blank node, or literal object");
        }

        // Optional N-Quads graph label, then the terminating '.'.
        skip_ws(line, i);
        if (i < line.size() && (line[i] == '<' || line[i] == '_')) {
            NTerm g = read_node_term(line, i);
            tr.graph = g.value;
        }
        skip_ws(line, i);
        if (i >= line.size() || line[i] != '.') throw std::runtime_error("N-Triples: expected '.' terminator");
        return tr;
    }

} // namespace ragedb

#endif // RAGEDB_NTRIPLESPARSER_H
