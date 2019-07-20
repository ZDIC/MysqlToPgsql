namespace lib {
    
    using @absolute_import = @@__future__.absolute_import;
    
    using System.Collections.Generic;
    
    using System.Linq;
    
    public static class converter {
        
        public class Converter
            : object {
            
            public object exclude_tables;
            
            public object file_options;
            
            public object force_truncate;
            
            public object index_prefix;
            
            public object only_tables;
            
            public object reader;
            
            public object supress_data;
            
            public object supress_ddl;
            
            public object verbose;
            
            public object writer;
            
            public Converter(object reader, object writer, object file_options, object verbose = false) {
                this.verbose = verbose;
                this.reader = reader;
                this.writer = writer;
                this.file_options = file_options;
                this.exclude_tables = file_options.get("exclude_tables", new List<object>());
                this.only_tables = file_options.get("only_tables", new List<object>());
                this.supress_ddl = file_options.get("supress_ddl", null);
                this.supress_data = file_options.get("supress_data", null);
                this.force_truncate = file_options.get("force_truncate", null);
                this.index_prefix = file_options.get("index_prefix", "");
            }
            
            public virtual object convert() {
                if (this.verbose) {
                    print_start_table(">>>>>>>>>> STARTING <<<<<<<<<<\n\n");
                }
                var tables = (from t in (from t in this.reader.tables
                    where !this.exclude_tables.Contains(t.name)
                    select t)
                    where !this.only_tables || this.only_tables.Contains(t.name)
                    select t).ToList();
                if (this.only_tables) {
                    tables.sort(key: t => this.only_tables.index(t.name));
                }
                if (!this.supress_ddl) {
                    if (this.verbose) {
                        print_start_table("START CREATING TABLES");
                    }
                    foreach (var table in tables) {
                        this.writer.write_table(table);
                    }
                    if (this.verbose) {
                        print_start_table("DONE CREATING TABLES");
                    }
                }
                if (this.force_truncate && this.supress_ddl) {
                    if (this.verbose) {
                        print_start_table("START TRUNCATING TABLES");
                    }
                    foreach (var table in tables) {
                        this.writer.truncate(table);
                    }
                    if (this.verbose) {
                        print_start_table("DONE TRUNCATING TABLES");
                    }
                }
                if (!this.supress_data) {
                    if (this.verbose) {
                        print_start_table("START WRITING TABLE DATA");
                    }
                    foreach (var table in tables) {
                        this.writer.write_contents(table, this.reader);
                    }
                    if (this.verbose) {
                        print_start_table("DONE WRITING TABLE DATA");
                    }
                }
                if (!this.supress_ddl) {
                    if (this.verbose) {
                        print_start_table("START CREATING INDEXES, CONSTRAINTS, AND TRIGGERS");
                    }
                    foreach (var table in tables) {
                        this.writer.write_indexes(table);
                    }
                    foreach (var table in tables) {
                        this.writer.write_constraints(table);
                    }
                    foreach (var table in tables) {
                        this.writer.write_triggers(table);
                    }
                    if (this.verbose) {
                        print_start_table("DONE CREATING INDEXES, CONSTRAINTS, AND TRIGGERS");
                    }
                }
                if (this.verbose) {
                    print_start_table("\n\n>>>>>>>>>> FINISHED <<<<<<<<<<");
                }
                this.writer.close();
            }
        }
    }
}
