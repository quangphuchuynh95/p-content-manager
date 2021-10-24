<?php

namespace Quangphuc\PContentManager\QueryFactory;

use Quangphuc\QueryFactory\QueryFactory;
use Quangphuc\QueryFactory\QueryFactoryHelper;

class CollectionQueryFactory extends QueryFactory {
    protected $table = 'pcm_collection';
    protected $pk = 'collection_id';

    public function insertOne($properties, $fields) {
        $properties = QueryFactoryHelper::escapeValue($properties, 'json');
        foreach ($fields as &$field) {
            $field = QueryFactoryHelper::escapeValue($field, 'json');
        }
        return "SELECT * FROM pcm_create_collection($properties, ARRAY[" . implode(',', $fields) ."])";
    }

    public function updateOne($id, $properties, $fields) {
        $id = QueryFactoryHelper::escapeValue($id);
        $properties = QueryFactoryHelper::escapeValue($properties, 'json');
        foreach ($fields as &$field) {
            $field = QueryFactoryHelper::escapeValue($field, 'json');
        }
        return "SELECT * FROM pcm_update_collection($id, $properties, ARRAY[" . implode(',', $fields) ."])";
    }

    public function initDatabase() {
        return <<<__sql__
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

            CREATE TABLE IF NOT EXISTS pcm_collection (
                collection_id smallserial PRIMARY KEY,
                collection_slug varchar UNIQUE NOT NULL,
                collection_name varchar NOT NULL
            );
            
            CREATE OR REPLACE FUNCTION pcm_create_table_for_collection()
                RETURNS TRIGGER
                LANGUAGE plpgsql
            AS $$
            DECLARE
                _table_name varchar;
                _sql text;
            BEGIN
                _table_name := 'pcm__' || new.collection_slug;
                _sql := 'CREATE TABLE ' || quote_ident(_table_name) || '(' || quote_ident(_table_name || '_id') || ' serial PRIMARY KEY)';
                EXECUTE _sql;
                RETURN new;
            END
            $$;
            
            CREATE OR REPLACE FUNCTION pcm_drop_table_for_collection()
                RETURNS TRIGGER
                LANGUAGE plpgsql
            AS $$
            DECLARE
                _table_name varchar;
                _sql text;
            BEGIN
                _table_name := 'pcm__' || old.collection_slug;
                _sql := 'DROP TABLE ' || quote_ident(_table_name);
                EXECUTE _sql;
                RETURN new;
            END
            $$;
            
            CREATE OR REPLACE FUNCTION pcm_update_table_for_collection()
                RETURNS TRIGGER
                LANGUAGE plpgsql
            AS $$
            DECLARE
                _sql text;
            BEGIN
                IF old.collection_slug != new.collection_slug THEN
                    _sql := 'ALTER TABLE ' || quote_ident('pcm__' || old.collection_slug)
                        || ' RENAME TO ' || quote_ident('pcm__' || new.collection_slug);
                    EXECUTE _sql;
                END IF;
                RETURN new;
            END
            $$;
            
            CREATE TRIGGER pcm_trigger_after_insert_collection
                AFTER INSERT ON pcm_collection
                FOR EACH ROW
                EXECUTE FUNCTION pcm_create_table_for_collection();
            
            CREATE TRIGGER pcm_trigger_after_delete_collection
                AFTER DELETE ON pcm_collection
                FOR EACH ROW
                EXECUTE FUNCTION pcm_drop_table_for_collection();
            
            CREATE TRIGGER pcm_trigger_after_update_collection
                AFTER UPDATE ON pcm_collection
                FOR EACH ROW
                EXECUTE FUNCTION pcm_update_table_for_collection();
            
            
            -- --------------------------------------------------------------------
            
            CREATE TYPE pcm_field_type AS ENUM ('text', 'varchar', 'int', 'numeric');
            
            CREATE TABLE IF NOT EXISTS pcm_collection_field (
                collection_id smallint REFERENCES pcm_collection(collection_id) ON DELETE CASCADE,
                field_slug varchar,
                field_name varchar NOT NULL,
                field_type pcm_field_type NOT NULL,
                PRIMARY KEY(collection_id, field_slug)
            );
            
            CREATE OR REPLACE FUNCTION pcm_create_field_for_collection()
                RETURNS TRIGGER
                LANGUAGE plpgsql
            AS $$
            DECLARE
                _table_name varchar;
                _sql text;
            BEGIN
                SELECT ('pcm__' || collection_slug) INTO _table_name
                FROM pcm_collection
                WHERE collection_id = new.collection_id;
                _sql := 'ALTER TABLE ' || quote_ident(_table_name) || ' ADD COLUMN ' || quote_ident(new.field_slug) || ' ' || new.field_type::text;
                EXECUTE _sql;
                RETURN new;
            END
            $$;
            
            CREATE OR REPLACE FUNCTION pcm_drop_field_for_collection()
                RETURNS TRIGGER
                LANGUAGE plpgsql
            AS $$
            DECLARE
                _table_name varchar;
                _sql text;
            BEGIN
                SELECT ('pcm__' || collection_slug) INTO _table_name
                FROM pcm_collection
                WHERE collection_id = old.collection_id;
                IF _table_name IS NOT NULL THEN
                    _sql := 'ALTER TABLE ' || quote_ident(_table_name) || ' DROP COLUMN ' || quote_ident(old.field_slug);
                    EXECUTE _sql;
                END IF;
                RETURN new;
            END
            $$;
            
            CREATE OR REPLACE FUNCTION pcm_update_field_for_collection()
                RETURNS TRIGGER
                LANGUAGE plpgsql
            AS $$
            DECLARE
                _table_name varchar;
                _sql text;
            BEGIN
                SELECT ('pcm__' || collection_slug) INTO _table_name
                FROM pcm_collection
                WHERE collection_id = new.collection_id;
                IF old.field_slug != new.field_slug THEN
                    _sql := 'ALTER TABLE ' || quote_ident(_table_name)
                        || ' RENAME ' || quote_ident(old.field_slug)
                        || ' TO ' || quote_ident(new.field_slug);
                    EXECUTE _sql;
                END IF;
                IF old.field_type != new.field_type THEN
                    _sql := 'ALTER TABLE ' || quote_ident(_table_name)
                        || ' ALTER COLUMN ' || quote_ident(new.field_slug)
                        || ' TYPE ' || new.field_type
                        || ' USING ' || quote_ident(new.field_slug::text) || '::' || new.field_type;
                    EXECUTE _sql;
                END IF;
                RETURN new;
            END
            $$;
            
            CREATE TRIGGER pcm_trigger_after_insert_collection_field
                AFTER INSERT ON pcm_collection_field
                FOR EACH ROW
            EXECUTE FUNCTION pcm_create_field_for_collection();
            
            CREATE TRIGGER pcm_trigger_after_delete_collection_field
                AFTER DELETE ON pcm_collection_field
                FOR EACH ROW
            EXECUTE FUNCTION pcm_drop_field_for_collection();
            
            CREATE TRIGGER pcm_trigger_after_update_collection_field
                AFTER UPDATE ON pcm_collection_field
                FOR EACH ROW
            EXECUTE FUNCTION pcm_update_field_for_collection();
            
            CREATE OR REPLACE FUNCTION pcm_create_collection(properties json, fields json[])
                RETURNS json
                LANGUAGE plpgsql
            AS $$
                DECLARE
                    field json;
                    new_collection record;
                BEGIN
                    INSERT INTO pcm_collection
                        (collection_slug, collection_name)
                    VALUES
                        (properties->>'collection_slug'::varchar, properties->>'collection_name'::varchar)
                    RETURNING * INTO new_collection;
                    INSERT INTO pcm_collection_field
                        (collection_id, field_slug, field_name, field_type)
                    SELECT new_collection.collection_id,
                           (f->>'slug')::varchar,
                           (f->>'name')::varchar,
                           (f->>'type')::pcm_field_type
                    FROM unnest(fields) f;
                    RETURN row_to_json(new_collection);
                END;
            $$;
            
            CREATE OR REPLACE FUNCTION pcm_update_collection(id smallint, properties json, fields json[])
                RETURNS json
                LANGUAGE plpgsql
            AS $$
            DECLARE
                field json;
                new_collection record;
            BEGIN
                UPDATE pcm_collection
                    SET collection_slug = properties->>'collection_slug'::varchar,
                        collection_name = properties->>'collection_name'::varchar
                WHERE collection_id = id;
                INSERT INTO pcm_collection_field
                    (collection_id, field_slug, field_name, field_type)
                SELECT id,
                    (f->>'slug')::varchar,
                    (f->>'name')::varchar,
                    (f->>'type')::pcm_field_type
                FROM unnest(fields) f
                ON CONFLICT DO UPDATE SET field_slug = excluded.field_slug,
                                          field_name = excluded.field_name,
                                          field_type = excluded.field_type;
                RETURN row_to_json(new_collection);
            END;
            $$;

        __sql__;
    }
}
