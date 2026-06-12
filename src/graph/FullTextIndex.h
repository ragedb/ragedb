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

#ifndef RAGEDB_FULLTEXTINDEX_H
#define RAGEDB_FULLTEXTINDEX_H

#include <iresearch/analysis/analyzer.hpp>
// Upstream iresearch uses segmentation_token_stream instead of segmentation_tokenizer
#include <iresearch/analysis/segmentation_token_stream.hpp>
#include <iresearch/analysis/token_streams.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/search/bm25.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/score.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <algorithm>
#include <cctype>

namespace ragedb {

    // StoredIdField represents a stored field containing a 64-bit unsigned integer (external ID).
    // It implements both lowercase and uppercase naming conventions of standard iresearch field/attribute concepts.
    struct StoredIdField {
        uint64_t external_id;

        // Name of the stored column
        std::string_view name() const noexcept { return "external_id"; }
        std::string_view Name() const noexcept { return "external_id"; }

        // Serializes the 64-bit integer directly to standard data_output stream
        bool write(irs::data_output& out) const {
            out.write_bytes(reinterpret_cast<const irs::byte_type*>(&external_id), sizeof(external_id));
            return true;
        }
        bool Write(irs::data_output& out) const {
            return write(out);
        }
    };

    // IdField indexes the external ID as a string term in field 0 to support document deletions.
    // It implements both lowercase and uppercase naming conventions of standard iresearch.
    struct IdField {
        irs::field_id id{0};
        std::string text;
        // Upstream iresearch uses string_token_stream for identity/string fields
        mutable irs::string_token_stream tokenizer;

        irs::field_id Id() const noexcept { return id; }
        irs::field_id id_val() const noexcept { return id; }

        std::string_view name() const noexcept { return "id"; }
        std::string_view Name() const noexcept { return "id"; }

        irs::IndexFeatures index_features() const noexcept { return irs::IndexFeatures::FREQ; }
        irs::IndexFeatures GetIndexFeatures() const noexcept { return irs::IndexFeatures::FREQ; }

        irs::features_t features() const noexcept { return {}; }

        // Returns reference to token_stream base class
        irs::token_stream& get_tokens() const {
            tokenizer.reset(text);
            return tokenizer;
        }
        irs::token_stream& GetTokens() const {
            return get_tokens();
        }
    };

    // TextField indexes standard text (using the segmentation tokenizer) for full text search.
    // It implements both lowercase and uppercase naming conventions of standard iresearch.
    struct TextField {
        irs::field_id id{2};
        std::string text;
        // Upstream uses lowercase analyzer class name
        mutable irs::analysis::analyzer::ptr tokenizer;

        TextField() {
            // Instantiate standard segmentation_token_stream tokenizer from upstream iresearch
            tokenizer = std::make_unique<irs::analysis::segmentation_token_stream>(
                irs::analysis::segmentation_token_stream::options_t{}
            );
        }

        irs::field_id Id() const noexcept { return id; }

        std::string_view name() const noexcept { return "text"; }
        std::string_view Name() const noexcept { return "text"; }

        irs::IndexFeatures index_features() const noexcept {
            return irs::IndexFeatures::FREQ | irs::IndexFeatures::POS;
        }
        irs::IndexFeatures GetIndexFeatures() const noexcept {
            return index_features();
        }

        irs::features_t features() const noexcept { return {}; }

        // Returns reference to token_stream base class
        irs::token_stream& get_tokens() const {
            tokenizer->reset(text);
            return *tokenizer;
        }
        irs::token_stream& GetTokens() const {
            return get_tokens();
        }
    };

    // FullTextIndex provides a standard iresearch full-text search index for a property.
    // It is completely decoupled from DuckDB and SereneDB database components.
    class FullTextIndex {
    public:
        // Constructor: initializes memory-based directory, formats, scorer and tokenizer.
        FullTextIndex() {
            // Initialize standard formats
            irs::formats::init();
            // Upstream default formats usually include "1_5simd" or "1_5".
            // We search for "1_5simd" first and fall back to "1_5".
            format = irs::formats::get("1_5simd");
            if (!format) {
                format = irs::formats::get("1_5");
            }
            // Standard BM25 scorer
            scorer = std::make_unique<irs::BM25>();
            // Standard segmentation tokenizer from upstream iresearch
            tokenizer = std::make_unique<irs::analysis::segmentation_token_stream>(
                irs::analysis::segmentation_token_stream::options_t{}
            );

            // Construct standard iresearch index writer
            irs::IndexWriterOptions options;
            writer = irs::IndexWriter::Make(dir, format, irs::OM_CREATE, options);
        }

        // Inserts a text value associated with a document's external_id into the index.
        void insert(std::string_view text, uint64_t external_id) {
            auto ctx = writer->GetBatch();

            // Create id field for document indexing (so we can delete by ID later)
            IdField id_field;
            id_field.text = std::to_string(external_id);

            // Create stored ID field (so we can retrieve external ID from the search results)
            StoredIdField stored_id_field{external_id};

            // Create text field for text indexing
            TextField text_field;
            text_field.text = std::string(text);

            auto doc = ctx.Insert();
            doc.Insert<irs::Action::INDEX>(id_field);
            doc.Insert<irs::Action::STORE>(stored_id_field);
            doc.Insert<irs::Action::INDEX>(text_field);

            ctx.Commit();
            writer->Commit();
        }

        // Removes a document from the index by searching for its external_id in the index.
        void remove(uint64_t external_id) {
            std::string id_str = std::to_string(external_id);
            auto filter = std::make_unique<irs::by_term>();
            *filter->mutable_field() = "id"; // ID field is indexed as field name "id"
            filter->mutable_options()->term.assign(
                reinterpret_cast<const irs::byte_type*>(id_str.data()),
                id_str.size()
            );

            auto ctx = writer->GetBatch();
            ctx.Remove(*filter);
            ctx.Commit();
            writer->Commit();
        }

        // Searches the index using a text query, returning matched external_ids with their BM25 score.
        // If fuzzy search is enabled, uses Levenshtein edit distance of 1.
        std::vector<std::pair<uint64_t, double>> search(const std::string& query_str, bool fuzzy = false) {
            std::vector<std::pair<uint64_t, double>> search_results;

            // Tokenize the input query using standard segmentation tokenizer
            std::vector<std::string> query_tokens;
            if (tokenizer->reset(query_str)) {
                auto* term_attr = irs::get<irs::term_attribute>(*tokenizer);
                while (tokenizer->next()) {
                    if (term_attr) {
                        std::string term(reinterpret_cast<const char*>(term_attr->value.data()), term_attr->value.size());
                        if (!term.empty()) {
                            query_tokens.push_back(std::move(term));
                        }
                    }
                }
            }

            if (query_tokens.empty()) {
                return search_results;
            }

            // Combine token match requirements with Or disjunction
            auto filter = std::make_unique<irs::Or>();

            for (const auto& term : query_tokens) {
                if (fuzzy) {
                    auto& edit_filter = filter->add<irs::by_edit_distance>();
                    *edit_filter.mutable_field() = "text"; // Text field is indexed under field name "text"
                    auto* opts = edit_filter.mutable_options();
                    opts->term.assign(reinterpret_cast<const irs::byte_type*>(term.data()), term.size());
                    opts->max_distance = 2;
                    opts->max_terms = 1024;
                } else {
                    auto& term_filter = filter->add<irs::by_term>();
                    *term_filter.mutable_field() = "text"; // Text field is indexed under field name "text"
                    auto* opts = term_filter.mutable_options();
                    opts->term.assign(reinterpret_cast<const irs::byte_type*>(term.data()), term.size());
                }
            }

            // Get a snapshot reader of the committed index
            auto reader = writer->GetSnapshot();

            // Prepare the BM25 scorer
            irs::Scorers prepared_scorers = irs::Scorers::Prepare(scorer.get());

            // Prepare the filter query against the snapshot reader
            auto prepared_filter = filter->prepare({.index = reader, .scorers = prepared_scorers});
            if (!prepared_filter) {
                return search_results;
            }

            // Execute the query on each segment of the reader and collect scored results
            for (const auto& segment : reader) {
                auto docs = prepared_filter->execute({.segment = segment, .scorers = prepared_scorers});
                if (!docs) {
                    continue;
                }

                auto* doc = irs::get<irs::document>(*docs);
                const auto* score = irs::get<irs::score>(*docs);
                const auto* col_reader = segment.column("external_id");
                if (!col_reader) {
                    continue;
                }

                auto doc_it = col_reader->iterator(irs::ColumnHint::kNormal);
                if (!doc_it) {
                    continue;
                }

                while (docs->next()) {
                    // Seek the stored column reader to the matched document ID
                    if (doc_it->seek(docs->value()) == docs->value()) {
                        auto* payload_attr = irs::get<irs::payload>(*doc_it);
                        if (payload_attr && payload_attr->value.size() == sizeof(uint64_t)) {
                            uint64_t ext_id = 0;
                            std::memcpy(&ext_id, payload_attr->value.data(), sizeof(uint64_t));

                            // Extract the BM25 score value
                            double score_val = 0.0;
                            if (score) {
                                float temp_score = 0.0f;
                                (*score)(&temp_score);
                                score_val = static_cast<double>(temp_score);
                            }
                            search_results.push_back({ext_id, score_val});
                        }
                    }
                }
            }

            // Sort all search results descending by score
            std::sort(search_results.begin(), search_results.end(), [](const auto& a, const auto& b) {
                return a.second > b.second;
            });

            // Limit to top 100 matches
            constexpr size_t kTopK = 100;
            if (search_results.size() > kTopK) {
                search_results.resize(kTopK);
            }

            return search_results;
        }

    private:
        irs::memory_directory dir;
        irs::format::ptr format;
        std::unique_ptr<irs::BM25> scorer;
        irs::analysis::analyzer::ptr tokenizer;
        irs::IndexWriter::ptr writer;
    };

}

#endif // RAGEDB_FULLTEXTINDEX_H
