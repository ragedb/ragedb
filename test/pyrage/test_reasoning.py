import unittest
import json
from pyragedb.semantics import Model, String, Integer

class TestReasoning(unittest.TestCase):
    def test_rules_based_reasoning(self):
        m = Model("rules_test_model")
        Ticket = m.Concept("Ticket")
        m.Property(f"{Ticket} has priority {String:priority}")
        m.Property(f"{Ticket} has response_minutes {Integer:response_minutes}")
        m.Property(f"{Ticket} has severity {Integer:severity}")
        m.Property(f"{Ticket} has escalations {Integer:escalations}")

        # Single-placeholder presence property definition
        has_response_prop = m.Property(f"{Ticket} has first response")
        self.assertEqual(has_response_prop.name, "has_first_response")

        from pyragedb.semantics.std import strings, math, datetime as dt, aggregates

        # 1. Test fallback / defaulting
        fallback_expr = Ticket.priority | "unknown"
        q_fallback = m.select(fallback_expr.alias("priority_fallback"))
        gql_fallback = q_fallback.to_gql()
        self.assertIn("coalesce(ticket.priority, 'unknown') AS priority_fallback", gql_fallback)

        # 2. Test presence checks as boolean
        presence_expr = Ticket.priority.as_bool()
        q_presence = m.select(presence_expr.alias("has_priority"))
        gql_presence = q_presence.to_gql()
        self.assertIn("(ticket.priority IS NOT NULL) AS has_priority", gql_presence)

        # 3. Test string standard library operations
        s_lower = strings.lower(Ticket.priority)
        s_upper = strings.upper(Ticket.priority)
        s_strip = strings.strip(Ticket.priority)
        s_concat = strings.concat(Ticket.priority, "-tag")
        s_join = strings.join([Ticket.priority, "status"], " | ")
        s_replace = strings.replace(Ticket.priority, "p0", "P0")
        s_like = strings.like(Ticket.priority, "%outage%")
        s_startswith = strings.startswith(Ticket.priority, "p0")
        s_endswith = strings.endswith(Ticket.priority, "urgent")
        s_split = strings.split_part(Ticket.priority, "-", 1)

        q_strings = m.select(
            s_lower.alias("lower"),
            s_upper.alias("upper"),
            s_strip.alias("strip"),
            s_concat.alias("concat"),
            s_join.alias("join"),
            s_replace.alias("replace"),
            s_split.alias("split")
        ).where(s_like, s_startswith, s_endswith)

        gql_strings = q_strings.to_gql()
        self.assertIn("lower(ticket.priority) AS lower", gql_strings)
        self.assertIn("upper(ticket.priority) AS upper", gql_strings)
        self.assertIn("trim(ticket.priority) AS strip", gql_strings)
        self.assertIn("(ticket.priority + '-tag') AS concat", gql_strings)
        self.assertIn("ticket.priority + ' | ' + 'status' AS join", gql_strings)
        self.assertIn("replace(ticket.priority, 'p0', 'P0') AS replace", gql_strings)
        self.assertIn("split(ticket.priority, '-')[1] AS split", gql_strings)
        self.assertIn("ticket.priority LIKE '%outage%'", gql_strings)
        self.assertIn("ticket.priority STARTS WITH 'p0'", gql_strings)
        self.assertIn("ticket.priority ENDS WITH 'urgent'", gql_strings)

        # 4. Test arithmetic and math operations
        raw_priority = Ticket.severity * 20 + Ticket.escalations * 10 - Ticket.response_minutes / 2
        clip_priority = math.clip(raw_priority, 0.0, 100.0)
        round_price = math.round(raw_priority, 2)
        floor_price = math.floor(raw_priority)
        ceil_price = math.ceil(raw_priority)

        q_math = m.select(
            clip_priority.alias("clip"),
            round_price.alias("round"),
            floor_price.alias("floor"),
            ceil_price.alias("ceil")
        )

        gql_math = q_math.to_gql()
        self.assertIn("(((ticket.severity * 20) + (ticket.escalations * 10)) - (ticket.response_minutes / 2))", gql_math)
        self.assertIn("CASE WHEN (((ticket.severity * 20) + (ticket.escalations * 10)) - (ticket.response_minutes / 2)) < 0.0 THEN 0.0 WHEN (((ticket.severity * 20) + (ticket.escalations * 10)) - (ticket.response_minutes / 2)) > 100.0 THEN 100.0 ELSE (((ticket.severity * 20) + (ticket.escalations * 10)) - (ticket.response_minutes / 2)) END", gql_math)
        self.assertIn("round", gql_math)
        self.assertIn("floor", gql_math)
        self.assertIn("ceil", gql_math)

        # 5. Test Date & Datetime objects
        d_val = dt.date(2026, 2, 26)
        dt_val = dt.datetime(2026, 2, 1, 10, tz="UTC")
        self.assertEqual(d_val, "2026-02-26")
        self.assertEqual(dt_val, "2026-02-01T10:00:00Z")

        # 6. Test aggregates with per() and where() grouping
        Comment = m.Concept("Comment")
        Ticket.has_comment = m.Relationship(f"{Ticket} has comment {Comment:comment}")
        comment_count = aggregates.count(Comment).per(Ticket).where(Ticket.has_comment(Comment))

        q_agg = m.select(Ticket.id, comment_count.alias("comment_count"))
        gql_agg = q_agg.to_gql()
        self.assertIn("count(comment) AS comment_count", gql_agg)
        self.assertIn("(ticket)-[:COMMENT]->(comment:Comment)", gql_agg)

    def test_advanced_aggregation_and_grouping(self):
        m = Model("advanced_agg_test_model")
        Account = m.Concept("Account")
        Queue = m.Concept("Queue")
        Ticket = m.Concept("Ticket")
        Comment = m.Concept("Comment")
        Tag = m.Concept("Tag")
        
        m.Relationship(f"{Ticket} belongs to {Account:account}")
        m.Relationship(f"{Ticket} assigned to {Queue:queue}")
        m.Relationship(f"{Ticket} has comment {Comment:has_comment}")
        m.Relationship(f"{Comment} authored by {Tag:author}")
        m.Property(f"{Ticket} status is {String:status}")
        m.Property(f"{Account} id is {Integer:id}")
        m.Property(f"{Account} open ticket count is {Integer:count}")
        m.Property(f"{Queue} name is {String:name}")

        from pyragedb.semantics.std import aggregates as agg
        from pyragedb.semantics import distinct

        # 1. Test aggregates.per(...) initiator
        open_tickets_per_account_queue = (
            agg.per(Account, Queue)
            .count(Ticket)
            .where(Ticket.account == Account, Ticket.queue == Queue, Ticket.status == "open")
        )
        self.assertEqual(open_tickets_per_account_queue.op, "count")
        self.assertEqual(open_tickets_per_account_queue.target, Ticket)
        self.assertIn(Account, open_tickets_per_account_queue.group_by_concepts)
        self.assertIn(Queue, open_tickets_per_account_queue.group_by_concepts)

        # Test compilation of reusable grouping syntax
        q_agg_per = m.select(Account.id, Queue.name, open_tickets_per_account_queue.alias("ticket_count"))
        gql_agg_per = q_agg_per.to_gql()
        self.assertIn("count(ticket) AS ticket_count", gql_agg_per)
        self.assertIn("ticket.status = 'open'", gql_agg_per)

        # 2. Test distinct count compilation
        unique_commenters_per_ticket = (
            agg.count(distinct(Ticket.has_comment.author))
            .per(Ticket)
        )
        gql_distinct = m.select(Ticket.id, unique_commenters_per_ticket.alias("uniques")).to_gql()
        self.assertIn("count(DISTINCT tag) AS uniques", gql_distinct)

        # 3. Test distinct projections in select list
        q_distinct_select = m.select(distinct(Ticket.status, Account.id))
        gql_distinct_select = q_distinct_select.to_gql()
        self.assertIn("RETURN DISTINCT ticket.status, account.id", gql_distinct_select)

        # 4. Test rank builder GQL compilation (should render null but matching sort keys)
        open_ticket_rank = agg.rank(
            agg.desc(Account.open_ticket_count),
            agg.asc(Account.id),
        )
        q_rank = m.select(Account.id, open_ticket_rank.alias("rank_val"))
        gql_rank = q_rank.to_gql()
        self.assertIn("null AS rank_val", gql_rank)
        self.assertIn("account.open_ticket_count AS __sort_key_0", gql_rank)
        self.assertIn("account.id AS __sort_key_1", gql_rank)

        # 5. Test global top-K compiling natively to GQL ORDER BY & LIMIT
        open_ticket_top = agg.top(
            2,
            agg.desc(Account.open_ticket_count),
            agg.asc(Account.id),
        )
        q_top = m.select(Account.id).where(open_ticket_top)
        gql_top = q_top.to_gql()
        self.assertIn("ORDER BY account.open_ticket_count DESC, account.id ASC", gql_top)
        self.assertIn("LIMIT 2", gql_top)

        # 6. Test Python post-processing (using mock exec_gql on Model to return JSON)
        original_exec = m.exec_gql
        try:
            mock_data = [
                {"account.id": 1, "__sort_key_0": 5, "__sort_key_1": 1, "rank_val": None},
                {"account.id": 3, "__sort_key_0": 5, "__sort_key_1": 3, "rank_val": None},
                {"account.id": 2, "__sort_key_0": 10, "__sort_key_1": 2, "rank_val": None},
            ]
            m.exec_gql = lambda query: json.dumps(mock_data)
            
            processed = q_rank.to_dict()
            
            self.assertEqual(len(processed), 3)
            self.assertEqual(processed[0]["account.id"], 2)
            self.assertEqual(processed[0]["rank_val"], 1)
            self.assertEqual(processed[1]["account.id"], 1)
            self.assertEqual(processed[1]["rank_val"], 2)
            self.assertEqual(processed[2]["account.id"], 3)
            self.assertEqual(processed[2]["rank_val"], 3)
            
            self.assertNotIn("__sort_key_0", processed[0])
            self.assertNotIn("__sort_key_1", processed[0])
        finally:
            m.exec_gql = original_exec

        # 7. Test Grouped Top-K Post-processing
        open_ticket_top_grouped = agg.top(
            2,
            agg.desc(Account.open_ticket_count),
            agg.asc(Account.id),
        ).per(Queue)
        q_top_grouped = m.select(Queue.name, Account.id).where(open_ticket_top_grouped)
        
        original_exec_top = m.exec_gql
        try:
            mock_data_top = [
                {"queue.name": "support", "account.id": 1, "__sort_key_0": 5, "__sort_key_1": 1, "__group_key_2": "support"},
                {"queue.name": "support", "account.id": 3, "__sort_key_0": 5, "__sort_key_1": 3, "__group_key_2": "support"},
                {"queue.name": "support", "account.id": 2, "__sort_key_0": 10, "__sort_key_1": 2, "__group_key_2": "support"},
                {"queue.name": "sales", "account.id": 10, "__sort_key_0": 2, "__sort_key_1": 10, "__group_key_2": "sales"},
                {"queue.name": "sales", "account.id": 20, "__sort_key_0": 8, "__sort_key_1": 20, "__group_key_2": "sales"},
                {"queue.name": "sales", "account.id": 30, "__sort_key_0": 8, "__sort_key_1": 30, "__group_key_2": "sales"},
            ]
            m.exec_gql = lambda query: json.dumps(mock_data_top)
            
            processed_top = q_top_grouped.to_dict()
            self.assertEqual(len(processed_top), 4)
            
            support_group = [r for r in processed_top if r["queue.name"] == "support"]
            self.assertEqual(len(support_group), 2)
            self.assertEqual(support_group[0]["account.id"], 2)
            self.assertEqual(support_group[1]["account.id"], 1)
            
            sales_group = [r for r in processed_top if r["queue.name"] == "sales"]
            self.assertEqual(len(sales_group), 2)
            self.assertEqual(sales_group[0]["account.id"], 20)
            self.assertEqual(sales_group[1]["account.id"], 30)
        finally:
            m.exec_gql = original_exec_top

        # 8. Test to_df() delegation with mock pandas
        import sys
        from unittest.mock import MagicMock
        mock_pandas = MagicMock()
        sys.modules["pandas"] = mock_pandas
        try:
            m.exec_gql = lambda query: json.dumps([])
            q_rank.to_df()
            mock_pandas.DataFrame.assert_called_once()
        finally:
            if "pandas" in sys.modules:
                del sys.modules["pandas"]

if __name__ == "__main__":
    unittest.main()
