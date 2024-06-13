package rootmap

import (
	"database/sql"
	"fmt"

	"github.com/wkalt/migrate"
)

func initialMigration(tx *sql.Tx) error {
	stmt := `
	create table if not exists tables (
		id integer primary key,
		database text not null,
		producer text not null,
		topic text not null
	);

	create unique index tables_producer_topic_unique on tables(database, producer, topic);

	create table if not exists rootmap (
		id integer primary key,
		table_id integer not null references tables(id),
		version bigint not null,
		storage_prefix text not null,
		node_id text not null,
		timestamp text not null
	);

	create index rootmap_table_id_idx on rootmap(table_id);

	create table if not exists truncations (
		table_id int not null references tables(id),
		timestamp text not null default current_timestamp,
		version bigint not null
	);

	create index truncations_table_id_timestamp_idx on truncations(table_id, timestamp);

	create view latest_truncations as
	select t1.*
	from truncations t1
	left join truncations t2
	on t1.table_id = t2.table_id and t1.timestamp < t2.timestamp
	where t2.table_id is null;
	`
	_, err := tx.Exec(stmt)
	if err != nil {
		return fmt.Errorf("failed to execute initial migration: %w", err)
	}
	return nil
}

func Migrate(db *sql.DB) error {
	migrations := map[int]migrate.Migration{
		1: initialMigration,
	}
	if err := migrate.Migrate(db, migrations); err != nil {
		return fmt.Errorf("migration failure: %w", err)
	}
	return nil
}
