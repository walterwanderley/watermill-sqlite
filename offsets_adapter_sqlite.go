package sql

import (
	"fmt"

	sql "github.com/ThreeDotsLabs/watermill-sql/v3/pkg/sql"
)

// DefaultSQLiteOffsetsAdapter is adapter for storing offsets for SQLite databases.
//
// DefaultSQLiteOffsetsAdapter is designed to support multiple subscribers with exactly once delivery
// and guaranteed order.
//
// We aren't using FOR UPDATE in NextOffsetQuery because SQLite already lock the entire database. Use _txlock=immediate.
//
// When another consumer is trying to consume the same message, deadlock should occur in ConsumedMessageQuery.
// After deadlock, consumer will consume next message.
type DefaultSQLiteOffsetsAdapter struct {
	// GenerateMessagesOffsetsTableName may be used to override how the messages/offsets table name is generated.
	GenerateMessagesOffsetsTableName func(topic string) string
}

func (a DefaultSQLiteOffsetsAdapter) SchemaInitializingQueries(topic string) []sql.Query {
	return []sql.Query{
		{
			Query: `
				CREATE TABLE IF NOT EXISTS ` + a.MessagesOffsetsTable(topic) + ` (
				consumer_group TEXT NOT NULL,
				offset_acked INTEGER,
				offset_consumed INTEGER NOT NULL,
				PRIMARY KEY(consumer_group)
			)`,
		},
	}
}

func (a DefaultSQLiteOffsetsAdapter) AckMessageQuery(topic string, row sql.Row, consumerGroup string) sql.Query {
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, offset_acked, consumer_group)
		VALUES ($1, $2, $3) ON CONFLICT(consumer_group) DO UPDATE SET offset_consumed = $1, offset_acked = $2`

	return sql.Query{Query: ackQuery, Args: []any{row.Offset, row.Offset, consumerGroup}}
}

func (a DefaultSQLiteOffsetsAdapter) NextOffsetQuery(topic, consumerGroup string) sql.Query {
	return sql.Query{
		Query: `SELECT COALESCE(
				(SELECT offset_acked
				 FROM ` + a.MessagesOffsetsTable(topic) + `
				 WHERE consumer_group = $1
				), 0)`,
		Args: []any{consumerGroup},
	}
}

func (a DefaultSQLiteOffsetsAdapter) MessagesOffsetsTable(topic string) string {
	if a.GenerateMessagesOffsetsTableName != nil {
		return a.GenerateMessagesOffsetsTableName(topic)
	}
	return fmt.Sprintf("`watermill_offsets_%s`", topic)
}

func (a DefaultSQLiteOffsetsAdapter) ConsumedMessageQuery(topic string, row sql.Row, consumerGroup string, consumerULID []byte) sql.Query {
	// offset_consumed is not queried anywhere, it's used only to detect race conditions with NextOffsetQuery.
	ackQuery := `INSERT INTO ` + a.MessagesOffsetsTable(topic) + ` (offset_consumed, consumer_group)
		VALUES ($1, $2) ON CONFLICT(consumer_group) DO UPDATE SET offset_consumed = $1`
	return sql.Query{Query: ackQuery, Args: []interface{}{row.Offset, consumerGroup}}
}

func (a DefaultSQLiteOffsetsAdapter) BeforeSubscribingQueries(topic, consumerGroup string) []sql.Query {
	return nil
}
