namespace lib {
    
    using @absolute_import = @@__future__.absolute_import;
    
    using re;
    
    using StringIO = cStringIO.StringIO;
    
    using date = datetime.date;
    
    using datetime = datetime.datetime;
    
    using timedelta = datetime.timedelta;
    
    using AsIs = psycopg2.extensions.AsIs;
    
    using Binary = psycopg2.extensions.Binary;
    
    using QuotedString = psycopg2.extensions.QuotedString;
    
    using timezone = pytz.timezone;
    
    using System.Collections.Generic;
    
    using System;
    
    using System.Linq;
    
    public static class postgres_writer {
        
        // Base class for :py:class:`mysql2pgsql.lib.postgres_file_writer.PostgresFileWriter`
        //     and :py:class:`mysql2pgsql.lib.postgres_db_writer.PostgresDbWriter`.
        //     
        public class PostgresWriter
            : object {
            
            public Dictionary<object, object> column_types;
            
            public string index_prefix;
            
            public object is_gpdb;
            
            public string log_detail;
            
            public None tz;
            
            public string tz_offset;
            
            public PostgresWriter(object file_options, object tz = false) {
                var index_prefix = file_options.get("index_prefix");
                this.column_types = new Dictionary<object, object> {
                };
                this.log_detail = String.Format("\n%s\n", file_options["destination"]["postgres"]["database"]);
                this.is_gpdb = file_options.get("is_gpdb");
                this.index_prefix = index_prefix ? index_prefix : "";
                if (tz) {
                    this.tz = timezone("UTC");
                    this.tz_offset = "+00:00";
                } else {
                    this.tz = null;
                    this.tz_offset = "";
                }
            }
            
            static PostgresWriter() {
                @" 'UPPER_ID' is different with '""column""' in CREATE statement:
        'UPPER_ID'   will create column with name 'upper_id'
        '""UPPER_ID""' will create column with name 'UPPER_ID'
    ";
                @"QuotedString API: http://initd.org/psycopg/docs/extensions.html?highlight=quotedstring#psycopg2.extensions.QuotedString
       ERROR: 
           UnicodeEncodeError: 'latin-1' codec can't encode characters in position 18-19: ordinal not in range(256)
           UnicodeDecodeError: 'ascii' codec can't decode byte 0xe5 in position 16: ordinal not in range(128)
    ";
                @"Exclude PRIMARY KEY, create with write_indexes";
            }
            
            public virtual object column_description(object column) {
                return String.Format("\"%s\" %s", column["name"], this.column_type_info(column));
            }
            
            public virtual object column_type(object column) {
                var hash_key = hash(frozenset(column.items()));
                this.column_types[hash_key] = this.column_type_info(column).split(" ")[0];
                return this.column_types[hash_key];
            }
            
            // 
            //         
            public virtual object column_type_info(object column) {
                var null = column["null"] ? "" : " NOT NULL";
                Func<object, object> get_type = column => {
                    var t = v => !(v == null);
                    var @default = t(column["default"]) ? String.Format(" DEFAULT %s", QuotedString(column["default"]).getquoted()) : null;
                    if (column["type"] == "char") {
                        @default = t(@default) ? String.Format("%s::char", @default) : null;
                        return Tuple.Create(@default, String.Format("character(%s)", column["length"]));
                    } else if (column["type"] == "varchar") {
                        @default = t(@default) ? String.Format("%s::character varying", @default) : null;
                        return Tuple.Create(@default, String.Format("character varying(%s)", column["length"]));
                    } else if (column["type"] == "json") {
                        @default = null;
                        return Tuple.Create(@default, "json");
                    } else if (column["type"] == "integer") {
                        @default = t(@default) ? String.Format(" DEFAULT %s", t(column["default"]) ? column["default"] : "NULL") : null;
                        return Tuple.Create(@default, "integer");
                    } else if (column["type"] == "bigint") {
                        @default = t(@default) ? String.Format(" DEFAULT %s", t(column["default"]) ? column["default"] : "NULL") : null;
                        return Tuple.Create(@default, "bigint");
                    } else if (column["type"] == "tinyint") {
                        @default = t(@default) ? String.Format(" DEFAULT %s", t(column["default"]) ? column["default"] : "NULL") : null;
                        return Tuple.Create(@default, "smallint");
                    } else if (column["type"] == "boolean") {
                        @default = t(@default) ? String.Format(" DEFAULT %s", Convert.ToInt32(column["default"]) == 1 ? "true" : "false") : null;
                        return Tuple.Create(@default, "boolean");
                    } else if (column["type"] == "float") {
                        @default = t(@default) ? String.Format(" DEFAULT %s", t(column["default"]) ? column["default"] : "NULL") : null;
                        return Tuple.Create(@default, "real");
                    } else if (column["type"] == "float unsigned") {
                        @default = t(@default) ? String.Format(" DEFAULT %s", t(column["default"]) ? column["default"] : "NULL") : null;
                        return Tuple.Create(@default, "real");
                    } else if (Tuple.Create("numeric", "decimal").Contains(column["type"])) {
                        @default = t(@default) ? String.Format(" DEFAULT %s", t(column["default"]) ? column["default"] : "NULL") : null;
                        return Tuple.Create(@default, String.Format("numeric(%s, %s)", column["length"] || 20, column["decimals"] || 0));
                    } else if (column["type"] == "double precision") {
                        @default = t(@default) ? String.Format(" DEFAULT %s", t(column["default"]) ? column["default"] : "NULL") : null;
                        return Tuple.Create(@default, "double precision");
                    } else if (column["type"] == "datetime" || column["type"].startswith("datetime(")) {
                        @default = null;
                        if (this.tz) {
                            return Tuple.Create(@default, "timestamp with time zone");
                        } else {
                            return Tuple.Create(@default, "timestamp without time zone");
                        }
                    } else if (column["type"] == "date") {
                        @default = null;
                        return Tuple.Create(@default, "date");
                    } else if (column["type"] == "timestamp") {
                        if (column["default"] == null) {
                            @default = null;
                        } else if (column["default"].Contains("current_timestamp()")) {
                            @default = " DEFAULT CURRENT_TIMESTAMP";
                        } else if (column["default"].Contains("CURRENT_TIMESTAMP")) {
                            @default = " DEFAULT CURRENT_TIMESTAMP";
                        } else if (column["default"].Contains("0000-00-00 00:00")) {
                            if (this.tz) {
                                @default = String.Format(" DEFAULT '1970-01-01T00:00:00.000000%s'", this.tz_offset);
                            } else if (column["default"].Contains("0000-00-00 00:00:00")) {
                                @default = " DEFAULT '1970-01-01 00:00:00'";
                            } else {
                                @default = " DEFAULT '1970-01-01 00:00'";
                            }
                        }
                        if (this.tz) {
                            return Tuple.Create(@default, "timestamp with time zone");
                        } else {
                            return Tuple.Create(@default, "timestamp without time zone");
                        }
                    } else if (column["type"] == "time" || column["type"].startswith("time(")) {
                        @default = t(@default) ? " DEFAULT NOW()" : null;
                        if (this.tz) {
                            return Tuple.Create(@default, "time with time zone");
                        } else {
                            return Tuple.Create(@default, "time without time zone");
                        }
                    } else if (Tuple.Create("blob", "binary", "longblob", "mediumblob", "tinyblob", "varbinary").Contains(column["type"])) {
                        return Tuple.Create(@default, "bytea");
                    } else if (column["type"].startswith("binary(") || column["type"].startswith("varbinary(")) {
                        return Tuple.Create(@default, "bytea");
                    } else if (Tuple.Create("tinytext", "mediumtext", "longtext", "text").Contains(column["type"])) {
                        return Tuple.Create(@default, "text");
                    } else if (column["type"].startswith("enum")) {
                        @default = t(@default) ? String.Format(" %s::character varying", @default) : null;
                        var @enum = re.sub(@"^enum\(|\)$", "", column["type"]);
                        // TODO: will work for "'.',',',''''" but will fail for "'.'',','.'"
                        var max_enum_size = max((from e in @enum.split("','")
                            select e.replace("''", "'").Count).ToList());
                        return Tuple.Create(@default, String.Format(" character varying(%s) check(\"%s\" in (%s))", max_enum_size, column["name"], @enum));
                    } else if (column["type"].startswith("bit(")) {
                        return Tuple.Create(column["default"] ? String.Format(" DEFAULT %s", column["default"].upper()) : column["default"], String.Format("varbit(%s)", re.search(@"\((\d+)\)", column["type"]).group(1)));
                    } else if (column["type"].startswith("set(")) {
                        if (@default) {
                            @default = String.Format(" DEFAULT ARRAY[%s]::text[]", ",".join(from v in re.search(@"'(.*)'", @default).group(1).split(",")
                                select QuotedString(v).getquoted()));
                        }
                        return Tuple.Create(@default, "text[]");
                    } else {
                        throw new Exception(String.Format("unknown %s", column["type"]));
                    }
                };
                var _tup_1 = get_type(column);
                var @default = _tup_1.Item1;
                var column_type = _tup_1.Item2;
                @"Refactor for GPDB.";
                if (!this.is_gpdb && column.get("auto_increment", null)) {
                    return String.Format("%s DEFAULT nextval(\'\"%s_%s_seq\"\'::regclass) NOT NULL", column_type, column["table_name"], column["name"]);
                }
                return String.Format("%s%s%s", column_type, !(@default == null) ? @default : "", null);
            }
            
            public virtual object table_comments(object table) {
                var comments = new List<object>();
                if (table.comment) {
                    @"comments.append('COMMENT ON TABLE %s is %s;' % (table.name, QuotedString(table.comment).getquoted()))
               comments.append('COMMENT ON TABLE %s is %s;' % (table.name, ""'""+table.comment+""'""))";
                    var table_comment = QuotedString(table.comment.encode("utf8")).getquoted();
                    comments.append("COMMENT ON TABLE {} is {};".format(table.name, table_comment));
                }
                foreach (var column in table.columns) {
                    if (column["comment"]) {
                        @"comments.append('COMMENT ON COLUMN %s.%s is %s;' % (table.name, column['name'], QuotedString(column['comment']).getquoted()))
                   comments.append('COMMENT ON COLUMN %s.%s is %s;' % (table.name, column['name'], ""'""+column['comment'].decode('utf8')+""'""))";
                        comments.append("COMMENT ON COLUMN {}.{} is {};".format(table.name, column["name"], QuotedString(column["comment"]).getquoted()));
                    }
                }
                return comments;
            }
            
            // Examines row data from MySQL and alters
            //         the values when necessary to be compatible with
            //         sending to PostgreSQL via the copy command
            //         
            public virtual object process_row(object table, object row) {
                foreach (var _tup_1 in table.columns.Select((_p_1,_p_2) => Tuple.Create(_p_2, _p_1))) {
                    var index = _tup_1.Item1;
                    var column = _tup_1.Item2;
                    var hash_key = hash(frozenset(column.items()));
                    var column_type = this.column_types.Contains(hash_key) ? this.column_types[hash_key] : this.column_type(column);
                    if (row[index] == null && (!column_type.Contains("timestamp") || !column["default"])) {
                        row[index] = "\N";
                    } else if (row[index] == null && column["default"]) {
                        if (this.tz) {
                            row[index] = "1970-01-01T00:00:00.000000" + this.tz_offset;
                        } else {
                            row[index] = "1970-01-01 00:00:00";
                        }
                    } else if (column_type.Contains("bit")) {
                        row[index] = bin(ord(row[index]))[2];
                    } else if (row[index] is str || row[index] is unicode || row[index] is basestring) {
                        if (column_type == "bytea") {
                            row[index] = row[index] ? Binary(row[index]).getquoted()[1:: - 8] : row[index];
                        } else if (column_type.Contains("text[")) {
                            row[index] = String.Format("{%s}", ",".join(from v in row[index].split(",")
                                select String.Format("\"%s\"", v.replace("\"", @"\"""))));
                        } else {
                            row[index] = row[index].replace("\\", @"\\").replace("\n", @"\n").replace("\t", @"\t").replace("\r", @"\r").replace("\0", "");
                        }
                    } else if (column_type == "boolean") {
                        // We got here because you used a tinyint(1), if you didn't want a bool, don't use that type
                        row[index] = !Tuple.Create(null, 0).Contains(row[index]) ? "t" : row[index] == 0 ? "f" : row[index];
                    } else if (row[index] is date || row[index] is datetime) {
                        if (row[index] is datetime && this.tz) {
                            try {
                                if (row[index].tzinfo) {
                                    row[index] = row[index].astimezone(this.tz).isoformat();
                                } else {
                                    row[index] = new datetime(tzinfo: this.tz, row[index].timetuple()[::6]).isoformat();
                                }
                            } catch (Exception) {
                                Console.WriteLine(e.message);
                            }
                        } else {
                            row[index] = row[index].isoformat();
                        }
                    } else if (row[index] is timedelta) {
                        row[index] = datetime.utcfromtimestamp(_get_total_seconds(row[index])).time().isoformat();
                    } else {
                        row[index] = AsIs(row[index]).getquoted();
                    }
                }
            }
            
            public virtual object table_attributes(object table) {
                var primary_keys = new List<object>();
                object serial_key = null;
                object maxval = null;
                var columns = StringIO();
                foreach (var column in table.columns) {
                    if (column["auto_increment"]) {
                        serial_key = column["name"];
                        maxval = column["maxval"] < 1 ? 1 : column["maxval"] + 1;
                    }
                    if (column["primary_key"]) {
                        primary_keys.append(column["name"]);
                    }
                    columns.write(String.Format("  %s,\n", this.column_description(column)));
                }
                return Tuple.Create(primary_keys, serial_key, maxval, columns.getvalue()[:: - 2]);
            }
            
            public virtual object truncate(object table) {
                object serial_key = null;
                object maxval = null;
                foreach (var column in table.columns) {
                    if (column["auto_increment"]) {
                        serial_key = column["name"];
                        maxval = column["maxval"] < 1 ? 1 : column["maxval"] + 1;
                    }
                }
                var truncate_sql = String.Format("TRUNCATE \"%s\" CASCADE;", table.name);
                object serial_key_sql = null;
                if (serial_key) {
                    serial_key_sql = String.Format("SELECT pg_catalog.setval(pg_get_serial_sequence(%(table_name)s, %(serial_key)s), %(maxval)s, true);", new Dictionary<object, object> {
                        {
                            "table_name",
                            QuotedString(String.Format("\"%s\"", table.name)).getquoted()},
                        {
                            "serial_key",
                            QuotedString(serial_key).getquoted()},
                        {
                            "maxval",
                            maxval}});
                }
                return Tuple.Create(truncate_sql, serial_key_sql);
            }
            
            public virtual object write_table(object table) {
                var _tup_1 = this.table_attributes(table);
                var primary_keys = _tup_1.Item1;
                var serial_key = _tup_1.Item2;
                var maxval = _tup_1.Item3;
                var columns = _tup_1.Item4;
                var serial_key_sql = new List<object>();
                var table_sql = new List<object>();
                var table_comment_sql = new List<object>();
                if (serial_key) {
                    var serial_key_seq = String.Format("%s_%s_seq", table.name, serial_key);
                    serial_key_sql.append(String.Format("DROP SEQUENCE IF EXISTS \"%s\" CASCADE;", serial_key_seq));
                    serial_key_sql.append(String.Format(@"CREATE SEQUENCE ""%s"" INCREMENT BY 1
                                  NO MAXVALUE NO MINVALUE CACHE 1;", serial_key_seq));
                    serial_key_sql.append(String.Format("SELECT pg_catalog.setval(\'\"%s\"\', %s, true);", serial_key_seq, maxval));
                }
                @" 'CREATE TABLE schema.table' is different with 'CREATE TABLE ""schema.table""':
            'CREATE TABLE schema1.table1'   will create table in schema1
            'CREATE TABLE ""schema1.table1""' will create 'schema1.table1' in selected or public schema

            If use SQL Key Word in scripts, necessarily with double quate, like ""user"".
        ";
                table_sql.append(String.Format("DROP TABLE IF EXISTS \"%s\" CASCADE;", table.name));
                table_sql.append(String.Format("CREATE TABLE \"%s\" (\n%s\n)\nWITHOUT OIDS;", table.name.encode("utf8"), columns));
                if (!this.is_gpdb) {
                    table_comment_sql.extend(this.table_comments(table));
                }
                return Tuple.Create(table_sql, serial_key_sql, table_comment_sql);
            }
            
            public virtual object write_indexes(object table) {
                object unique;
                var index_sql = new List<object>();
                var primary_index = (from idx in table.indexes
                    where idx.get("primary", null)
                    select idx).ToList();
                var index_prefix = this.index_prefix;
                if (primary_index) {
                    index_sql.append(String.Format("ALTER TABLE \"%(table_name)s\" ADD CONSTRAINT \"%(index_name)s_pkey\" PRIMARY KEY(%(column_names)s);", new Dictionary<object, object> {
                        {
                            "table_name",
                            table.name},
                        {
                            "index_name",
                            String.Format("%s%s_%s", index_prefix, table.name, "_".join(primary_index[0]["columns"]))},
                        {
                            "column_names",
                            ", ".join(from col in primary_index[0]["columns"]
                                select String.Format("\"%s\"", col))}}));
                    this.process_log("    create index: " + table.name + "|" + ",".join(primary_index[0]["columns"]) + "|PRIMARY");
                }
                if (this.is_gpdb) {
                    foreach (var index in table.indexes) {
                        if (index.Contains("primary")) {
                            continue;
                        }
                        unique = index.get("unique", null) ? "UNIQUE " : "";
                        this.process_log("    ignore index: " + table.name + "|" + ",".join(index["columns"]) + unique ? "|UNIQUE" : "");
                    }
                    return index_sql;
                }
                @"For Greenplum Database(base on PSQL):
               psycopg2.ProgrammingError: UNIQUE index must contain all columns in the distribution key
           Detail refer to:
               https://stackoverflow.com/questions/40987460/how-should-i-deal-with-my-unique-constraints-during-my-data-migration-from-postg
               
               http://gpdb.docs.pivotal.io/4320/ref_guide/sql_commands/CREATE_INDEX.html
               EXCERPT: In Greenplum Database, unique indexes are allowed only if the columns of the index key are the same as (or a superset of) 
                     the Greenplum distribution key. On partitioned tables, a unique index is only supported within an individual partition 
                     - not across all partitions.
        ";
                foreach (var index in table.indexes) {
                    if (index.Contains("primary")) {
                        continue;
                    }
                    unique = index.get("unique", null) ? "UNIQUE " : "";
                    var index_name = String.Format("%s%s_%s", index_prefix, table.name, "_".join(index["columns"]));
                    index_sql.append(String.Format("DROP INDEX IF EXISTS \"%s\" CASCADE;", index_name));
                    index_sql.append(String.Format("CREATE %(unique)sINDEX \"%(index_name)s\" ON \"%(table_name)s\" (%(column_names)s);", new Dictionary<object, object> {
                        {
                            "unique",
                            unique},
                        {
                            "index_name",
                            index_name},
                        {
                            "table_name",
                            table.name},
                        {
                            "column_names",
                            ", ".join(from col in index["columns"]
                                select String.Format("\"%s\"", col))}}));
                    this.process_log("    create index: " + table.name + "|" + ",".join(index["columns"]) + unique ? "|UNIQUE" : "");
                }
                return index_sql;
            }
            
            public virtual object write_constraints(object table) {
                var constraint_sql = new List<object>();
                if (this.is_gpdb) {
                    foreach (var key in table.foreign_keys) {
                        this.process_log("    ignore constraints: " + table.name + "|" + key["column"] + "| ref:" + key["ref_table"] + "." + key["ref_column"]);
                    }
                    return constraint_sql;
                }
                foreach (var key in table.foreign_keys) {
                    constraint_sql.append(String.Format(@"ALTER TABLE ""%(table_name)s"" ADD FOREIGN KEY (""%(column_name)s"")
            REFERENCES ""%(ref_table_name)s""(%(ref_column_name)s);", new Dictionary<object, object> {
                        {
                            "table_name",
                            table.name},
                        {
                            "column_name",
                            key["column"]},
                        {
                            "ref_table_name",
                            key["ref_table"]},
                        {
                            "ref_column_name",
                            key["ref_column"]}}));
                    this.process_log("    create constraints: " + table.name + "|" + key["column"] + "| ref:" + key["ref_table"] + "." + key["ref_column"]);
                }
                return constraint_sql;
            }
            
            public virtual object write_triggers(object table) {
                var trigger_sql = new List<object>();
                if (this.is_gpdb) {
                    foreach (var key in table.triggers) {
                        this.process_log("    ignore triggers: " + table.name + "|" + key["name"] + "|" + key["event"] + "|" + key["timing"]);
                    }
                    return trigger_sql;
                }
                foreach (var key in table.triggers) {
                    trigger_sql.append(String.Format(@"CREATE OR REPLACE FUNCTION %(fn_trigger_name)s RETURNS TRIGGER AS $%(trigger_name)s$
            BEGIN
                %(trigger_statement)s
            RETURN NULL;
            END;
            $%(trigger_name)s$ LANGUAGE plpgsql;", new Dictionary<object, object> {
                        {
                            "table_name",
                            table.name},
                        {
                            "trigger_time",
                            key["timing"]},
                        {
                            "trigger_event",
                            key["event"]},
                        {
                            "trigger_name",
                            key["name"]},
                        {
                            "fn_trigger_name",
                            "fn_" + key["name"] + "()"},
                        {
                            "trigger_statement",
                            key["statement"]}}));
                    trigger_sql.append(String.Format(@"CREATE TRIGGER %(trigger_name)s %(trigger_time)s %(trigger_event)s ON %(table_name)s
            FOR EACH ROW
            EXECUTE PROCEDURE fn_%(trigger_name)s();", new Dictionary<object, object> {
                        {
                            "table_name",
                            table.name},
                        {
                            "trigger_time",
                            key["timing"]},
                        {
                            "trigger_event",
                            key["event"]},
                        {
                            "trigger_name",
                            key["name"]}}));
                    this.process_log("    create triggers: " + table.name + "|" + key["name"] + "|" + key["event"] + "|" + key["timing"]);
                }
                return trigger_sql;
            }
            
            public virtual object process_log(object log) {
                Console.WriteLine(log);
                this.log_detail += log + "\n";
            }
            
            public virtual object close() {
                throw new NotImplementedException();
            }
            
            public virtual object write_contents(object table, object reader) {
                throw new NotImplementedException();
            }
        }
        
        // Original fix for Py2.6: https://github.com/mozilla/mozdownload/issues/73
        public static object _get_total_seconds(object dt) {
            // Keep backward compatibility with Python 2.6 which doesn't have this method
            if (hasattr(datetime, "total_seconds")) {
                return dt.total_seconds();
            } else {
                return (dt.microseconds + (dt.seconds + dt.days * 24 * 3600) * Math.Pow(10, 6)) / Math.Pow(10, 6);
            }
        }
    }
}
