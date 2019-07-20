namespace lib {
    
    using @with_statement = @@__future__.with_statement;
    
    using @absolute_import = @@__future__.absolute_import;
    
    using time;
    
    using closing = contextlib.closing;
    
    using psycopg2;
    
    using PostgresWriter = postgres_writer.PostgresWriter;
    
    using System.Collections.Generic;
    
    using System.Collections;
    
    using System.Linq;
    
    using System;
    
    public static class postgres_db_writer {
        
        // Class used to stream DDL and/or data
        //     from a MySQL server to a PostgreSQL.
        // 
        //     :Parameters:
        //       - `db_options`: :py:obj:`dict` containing connection specific variables
        //       - `verbose`: whether or not to log progress to :py:obj:`stdout`
        // 
        //     
        public class PostgresDbWriter
            : PostgresWriter {
            
            public object conn;
            
            public Dictionary<string, string> db_options;
            
            public string execute_error_log;
            
            public None schema;
            
            public object verbose;
            
            // A file-like class to support streaming
            //         table data directly to :py:meth:`pscopg2.copy_from`.
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            //           - `data`:
            //           - `processor`:
            //           - `verbose`: whether or not to log progress to :py:obj:`stdout`
            //         
            public class FileObjFaker
                : object {
                
                public object data;
                
                public int idx;
                
                public int prev_idx;
                
                public int prev_val_len;
                
                public object processor;
                
                public int start_time;
                
                public object table;
                
                public object verbose;
                
                public FileObjFaker(object table, object data, object processor, object verbose = false) {
                    this.data = iter(data);
                    this.table = table;
                    this.processor = processor;
                    this.verbose = verbose;
                    if (verbose) {
                        this.idx = 1;
                        this.start_time = time.time();
                        this.prev_val_len = 0;
                        this.prev_idx = 0;
                    }
                }
                
                public virtual object readline(Hashtable kwargs, params object [] args) {
                    try {
                        var row = this.data.next().ToList();
                    } catch (StopIteration) {
                        if (this.verbose) {
                            Console.WriteLine("");
                        }
                        return "";
                    } finally {
                        if (this.verbose) {
                            if (this.idx % 20000 == 0) {
                                var now = time.time();
                                var elapsed = now - this.start_time;
                                var val = String.Format("%.2f rows/sec [%s] ", (this.idx - this.prev_idx) / elapsed, this.idx);
                                print_row_progress(String.Format("%s%s", "\b" * this.prev_val_len, val));
                                this.prev_val_len = val.Count + 3;
                                this.start_time = now;
                                this.prev_idx = this.idx + 0;
                            }
                            this.idx += 1;
                        }
                    }
                }
                
                public virtual object read(Hashtable kwargs, params object [] args) {
                    return this.readline(args, kwargs);
                }
            }
            
            public PostgresDbWriter(object db_options, object verbose = false, Hashtable kwargs, params object [] args)
                : base(kwargs) {
                this.execute_error_log = "";
                this.verbose = verbose;
                this.db_options = new Dictionary<object, object> {
                    {
                        "host",
                        db_options["hostname"].ToString()},
                    {
                        "port",
                        db_options.get("port", 5432)},
                    {
                        "database",
                        db_options["database"].ToString()},
                    {
                        "password",
                        db_options.get("password", null).ToString() || ""},
                    {
                        "user",
                        db_options["username"].ToString()}};
                if (db_options["database"].ToString().Contains(":")) {
                    var _tup_1 = this.db_options["database"].split(":");
                    this.db_options["database"] = _tup_1.Item1;
                    this.schema = _tup_1.Item2;
                } else {
                    this.schema = null;
                }
                this.open();
            }
            
            public virtual object open() {
                this.conn = psycopg2.connect(this.db_options);
                using (var cur = closing(this.conn.cursor())) {
                    if (this.schema) {
                        cur.execute(String.Format("SET search_path TO %s", this.schema));
                    }
                    cur.execute("SET client_encoding = \'UTF8\'");
                    if (this.conn.server_version >= 80200) {
                        cur.execute("SET standard_conforming_strings = off");
                    }
                    cur.execute("SET check_function_bodies = false");
                    cur.execute("SET client_min_messages = warning");
                }
            }
            
            public virtual object query(object sql, object args = Tuple.Create("<Empty>"), object one = false) {
                using (var cur = closing(this.conn.cursor())) {
                    cur.execute(sql, args);
                    return one ? cur.fetchone() : cur;
                }
            }
            
            public virtual object execute(object sql, object args = Tuple.Create("<Empty>"), object many = false) {
                using (var cur = closing(this.conn.cursor())) {
                    try {
                        if (many) {
                            cur.executemany(sql, args);
                        } else {
                            cur.execute(sql, args);
                        }
                    } catch (Exception) {
                        this.execute_error_log += "\n######POSTGRES SCRIPTS:######\n " + sql + "\n######ERROR:######\n " + e.ToString();
                        Console.WriteLine("ERROR: " + e.ToString());
                    }
                    this.conn.commit();
                }
            }
            
            public virtual object copy_from(object file_obj, object table_name, object columns) {
                using (var cur = closing(this.conn.cursor())) {
                    cur.copy_from(file_obj, table: table_name, columns: columns);
                }
                this.conn.commit();
            }
            
            // Closes connection to the PostgreSQL server
            public virtual object close() {
                this.conn.close();
            }
            
            public virtual object exists(object relname) {
                var rc = this.query("SELECT COUNT(!) FROM pg_class WHERE relname = %s", Tuple.Create(relname), one: true);
                return rc && Convert.ToInt32(rc[0]) == 1;
            }
            
            // Send DDL to truncate the specified `table`
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object truncate(object table) {
                var _tup_1 = super(PostgresDbWriter, this).truncate(table);
                var truncate_sql = _tup_1.Item1;
                var serial_key_sql = _tup_1.Item2;
                this.execute(truncate_sql);
                if (serial_key_sql) {
                    this.execute(serial_key_sql);
                }
            }
            
            // Send DDL to create the specified `table`
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object write_table(object table) {
                var _tup_1 = super(PostgresDbWriter, this).write_table(table);
                var table_sql = _tup_1.Item1;
                var serial_key_sql = _tup_1.Item2;
                var table_comment_sql = _tup_1.Item3;
                foreach (var sql in serial_key_sql + table_sql) {
                    this.execute(sql);
                }
                @"Execute comment with the error encoding(sometimes):
        UnicodeDecodeError: 'ascii' codec can't decode byte 0xe7 in position 94: ordinal not in range(128)
        ";
                foreach (var sql in table_comment_sql) {
                    this.execute(sql);
                }
            }
            
            // Send DDL to create the specified `table` indexes
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object write_indexes(object table) {
                var index_sql = super(PostgresDbWriter, this).write_indexes(table);
                foreach (var sql in index_sql) {
                    this.execute(sql);
                }
            }
            
            // Send DDL to create the specified `table` triggers
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object write_triggers(object table) {
                var index_sql = super(PostgresDbWriter, this).write_triggers(table);
                foreach (var sql in index_sql) {
                    this.execute(sql);
                }
            }
            
            // Send DDL to create the specified `table` constraints
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object write_constraints(object table) {
                var constraint_sql = super(PostgresDbWriter, this).write_constraints(table);
                foreach (var sql in constraint_sql) {
                    this.execute(sql);
                }
            }
            
            // Write the contents of `table`
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            //           - `reader`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader` object that allows reading from the data source.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object write_contents(object table, object reader) {
                var f = new FileObjFaker(table, reader.read(table), this.process_row, this.verbose);
                this.copy_from(f, String.Format("\"%s\"", table.name), (from c in table.columns
                    select String.Format("\"%s\"", c["name"])).ToList());
            }
        }
    }
}
