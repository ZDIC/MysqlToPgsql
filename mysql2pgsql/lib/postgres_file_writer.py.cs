namespace lib {
    
    using @absolute_import = @@__future__.absolute_import;
    
    using time;
    
    using PostgresWriter = postgres_writer.PostgresWriter;
    
    using System.Collections;
    
    using System.Collections.Generic;
    
    using System;
    
    using System.Linq;
    
    public static class postgres_file_writer {
        
        // Class used to ouput the PostgreSQL
        //     compatable DDL and/or data to the specified
        //     output :py:obj:`file` from a MySQL server.
        // 
        //     :Parameters:
        //       - `output_file`: the output :py:obj:`file` to send the DDL and/or data
        //       - `verbose`: whether or not to log progress to :py:obj:`stdout`
        // 
        //     
        public class PostgresFileWriter
            : PostgresWriter {
            
            public object f;
            
            public None verbose;
            
            public None verbose = null;
            
            public PostgresFileWriter(object output_file, object verbose = false, Hashtable kwargs, params object [] args)
                : base(kwargs) {
                this.verbose = verbose;
                this.f = output_file;
                this.f.write(@"
-- MySQL 2 PostgreSQL dump" + "\n" +@"
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
");
            }
            
            // Write DDL to truncate the specified `table`
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object truncate(object table) {
                var _tup_1 = super(PostgresFileWriter, this).truncate(table);
                var truncate_sql = _tup_1.Item1;
                var serial_key_sql = _tup_1.Item2;
                this.f.write(String.Format(@"
" + "\n" +@"-- TRUNCATE %(table_name)s;
%(truncate_sql)s
", new Dictionary<object, object> {
                    {
                        "table_name",
                        table.name},
                    {
                        "truncate_sql",
                        truncate_sql}}));
                if (serial_key_sql) {
                    this.f.write(String.Format(@"
%(serial_key_sql)s
", new Dictionary<object, object> {
                        {
                            "serial_key_sql",
                            serial_key_sql}}));
                }
            }
            
            // Write DDL to create the specified `table`.
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object write_table(object table) {
                var _tup_1 = super(PostgresFileWriter, this).write_table(table);
                var table_sql = _tup_1.Item1;
                var serial_key_sql = _tup_1.Item2;
                var table_comment_sql = _tup_1.Item3;
                if (serial_key_sql) {
                    this.f.write(String.Format(@"
%(serial_key_sql)s
", new Dictionary<object, object> {
                        {
                            "serial_key_sql",
                            "\n".join(serial_key_sql)}}));
                }
                this.f.write(String.Format(@"
" + "\n" +@"-- Table: %(table_name)s
%(table_sql)s
", new Dictionary<object, object> {
                    {
                        "table_name",
                        table.name},
                    {
                        "table_sql",
                        "\n".join(table_sql + table_comment_sql)}}));
            }
            
            // Write DDL of `table` indexes to the output file
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object write_indexes(object table) {
                var indexes_sql = super(PostgresFileWriter, this).write_indexes(table);
                if (indexes_sql) {
                    this.f.write("\n-- INDEXes:\n" + "\n".join(indexes_sql));
                }
            }
            
            // Write DDL of `table` constraints to the output file
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object write_constraints(object table) {
                var constraints_sql = super(PostgresFileWriter, this).write_constraints(table);
                if (constraints_sql) {
                    this.f.write("\n\n-- CONSTRAINTs:\n" + "\n".join(constraints_sql));
                }
            }
            
            // Write TRIGGERs existing on `table` to the output file
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object write_triggers(object table) {
                var triggers_sql = super(PostgresFileWriter, this).write_triggers(table);
                if (triggers_sql) {
                    this.f.write("\n-- TRIGGERs:\n" + "\n".join(triggers_sql));
                }
            }
            
            // Write the data contents of `table` to the output file.
            // 
            //         :Parameters:
            //           - `table`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader.Table` object that represents the table to read/write.
            //           - `reader`: an instance of a :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader` object that allows reading from the data source.
            // 
            //         Returns None
            //         
            [status_logger]
            public virtual object write_contents(object table, object reader) {
                object prev_row_count;
                object prev_val_len;
                object start_time;
                // start variable optimiztions
                var pr = this.process_row;
                var f_write = this.f.write;
                var verbose = this.verbose;
                // end variable optimiztions
                f_write(String.Format(@"
--
-- Data for Name: %(table_name)s; Type: TABLE DATA;
--

COPY ""%(table_name)s"" (%(column_names)s) FROM stdin;
", new Dictionary<object, object> {
                    {
                        "table_name",
                        table.name},
                    {
                        "column_names",
                        ", ".join(from col in table.columns
                            select String.Format("\"%s\"", col["name"]))}}));
                if (verbose) {
                    var tt = time.time;
                    start_time = tt();
                    prev_val_len = 0;
                    prev_row_count = 0;
                }
                foreach (var _tup_1 in enumerate(reader.read(table), 1)) {
                    var i = _tup_1.Item1;
                    var row = _tup_1.Item2;
                    row = row.ToList();
                    pr(table, row);
                    try {
                        f_write(String.Format("%s\n", "\t".join(row)));
                    } catch (UnicodeDecodeError) {
                        f_write(String.Format("%s\n", "\t".join(from r in row
                            select r.decode("utf-8"))));
                    }
                    if (verbose) {
                        if (i % 20000 == 0) {
                            var now = tt();
                            var elapsed = now - start_time;
                            var val = String.Format("%.2f rows/sec [%s] ", (i - prev_row_count) / elapsed, i);
                            print_row_progress(String.Format("%s%s", "\b" * prev_val_len, val));
                            prev_val_len = val.Count + 3;
                            start_time = now;
                            prev_row_count = i;
                        }
                    }
                }
                f_write("\\.\n\n");
                if (verbose) {
                    Console.WriteLine("");
                }
            }
            
            // Closes the output :py:obj:`file`
            public virtual object close() {
                this.f.close();
            }
        }
    }
}
