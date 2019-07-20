namespace lib {
    
    using @with_statement = @@__future__.with_statement;
    
    using @absolute_import = @@__future__.absolute_import;
    
    using re;
    
    using closing = contextlib.closing;
    
    using MySQLdb;
    
    using MySQLdb.cursors;
    
    using System.Collections.Generic;
    
    using System;
    
    using System.Linq;
    
    public static class mysql_reader {
        
        public static object re_column_length = re.compile(@"\((\d+)\)");
        
        public static object re_column_precision = re.compile(@"\((\d+),(\d+)\)");
        
        public static object re_key_1 = re.compile(@"CONSTRAINT `(\w+)` FOREIGN KEY \(`(\w+)`\) REFERENCES `(\w+)` \(`(\w+)`\)");
        
        public static object re_key_2 = re.compile(@"KEY `(\w+)` \((.*)\)");
        
        public static object re_key_3 = re.compile(@"PRIMARY KEY +\((.*)\)");
        
        // 
        //     Class that wraps MySQLdb functions that auto reconnects
        //     thus (hopefully) preventing the frustrating
        //     "server has gone away" error. Also adds helpful
        //     helper functions.
        //     
        public class DB {
            
            public object conn;
            
            public Dictionary<string, object> options;
            
            public None conn = null;
            
            public DB(object options) {
                var args = new Dictionary<object, object> {
                    {
                        "user",
                        options.get("username", "root").ToString()},
                    {
                        "db",
                        options["database"]},
                    {
                        "use_unicode",
                        true},
                    {
                        "charset",
                        "utf8"}};
                if (options.get("password", null)) {
                    args["passwd"] = options.get("password", null).ToString();
                }
                if (options.get("socket", null)) {
                    args["unix_socket"] = options["socket"].ToString();
                } else {
                    args["host"] = options.get("hostname", "localhost").ToString();
                    args["port"] = options.get("port", 3306);
                    args["compress"] = options.get("compress", true);
                }
                this.options = args;
            }
            
            public virtual object connect() {
                this.conn = MySQLdb.connect(this.options);
            }
            
            public virtual object close() {
                this.conn.close();
            }
            
            public virtual object cursor(object cursorclass = MySQLdb.cursors.Cursor) {
                try {
                    return this.conn.cursor(cursorclass);
                } catch {
                    this.connect();
                    return this.conn.cursor(cursorclass);
                }
            }
            
            public virtual object list_tables() {
                return this.query("SHOW TABLES;");
            }
            
            public virtual object query(object sql, object args = Tuple.Create("<Empty>"), object one = false, object large = false) {
                return one ? this.query_one(sql, args) : this.query_many(sql, args, large);
            }
            
            public virtual object query_one(object sql, object args) {
                using (var cur = closing(this.cursor())) {
                    try {
                        cur.execute(sql, args);
                    } catch {
                        Console.WriteLine(sql);
                    }
                }
            }
            
            public virtual object query_many(object sql, object args, object large) {
                using (var cur = closing(this.cursor(large ? MySQLdb.cursors.SSCursor : MySQLdb.cursors.Cursor))) {
                    try {
                        cur.execute(sql, args);
                    } catch {
                        Console.WriteLine(sql);
                    }
                }
            }
        }
        
        public class MysqlReader
            : object {
            
            public DB db;
            
            public object exclude_tables;
            
            public object only_tables;
            
            //  Refer to: https://stackoverflow.com/questions/8624408/why-is-innodbs-show-table-status-so-unreliable
            //         The official MySQL 5.1 documentation acknowledges that InnoDB does not give accurate statistics with SHOW TABLE STATUS. 
            //             Whereas MYISAM tables specifically keep an internal cache of meta-data such as number of rows etc, the InnoDB engine
            //              stores both table data and indexes in */var/lib/mysql/ibdata**
            // 
            //         Inconsistent table row numbers are reported by SHOW TABLE STATUS because InnoDB dynamically estimates the 'Rows' value 
            //             by sampling a range of the table data (in */var/lib/mysql/ibdata**) and then extrapolates the approximate number of rows. 
            //             So much so that the InnoDB documentation acknowledges row number inaccuracy of up to 50% when using SHOW TABLE STATUS.
            //             So use SELECT COUNT(*) FROM TABLE_NAME.
            //         
            public class Table
                : object {
                
                public object _columns;
                
                public object _comment;
                
                public List<object> _foreign_keys;
                
                public List<object> _indexes;
                
                public object _name;
                
                public object _rows;
                
                public object _schema;
                
                public List<object> _triggers;
                
                public object reader;
                
                public Table(object reader, object name) {
                    this.reader = reader;
                    this._schema = reader.db.options["db"];
                    this._name = name.lower();
                    this._indexes = new List<object>();
                    this._foreign_keys = new List<object>();
                    this._triggers = new List<object>();
                    this._columns = this._load_columns();
                    this._comment = "";
                    this._load_indexes();
                    this._load_triggers();
                    this._rows = 0;
                    var table_status = this._load_table_status();
                    this._comment = table_status[17];
                    this._rows = this._load_table_rows();
                }
                
                // Normalize MySQL `data_type`
                public virtual object _convert_type(object data_type) {
                    if (data_type.startswith("varchar")) {
                        return "varchar";
                    } else if (data_type.startswith("char")) {
                        return "char";
                    } else if (Tuple.Create("bit(1)", "tinyint(1)", "tinyint(1) unsigned").Contains(data_type)) {
                        return "boolean";
                    } else if (re.search(@"^smallint.* unsigned", data_type) || data_type.startswith("mediumint")) {
                        return "integer";
                    } else if (data_type.startswith("smallint")) {
                        return "tinyint";
                    } else if (data_type.startswith("tinyint") || data_type.startswith("year(")) {
                        return "tinyint";
                    } else if (data_type.startswith("bigint") && data_type.Contains("unsigned")) {
                        return "numeric";
                    } else if (re.search(@"^int.* unsigned", data_type) || data_type.startswith("bigint") && !data_type.Contains("unsigned")) {
                        return "bigint";
                    } else if (data_type.startswith("int")) {
                        return "integer";
                    } else if (data_type.startswith("float")) {
                        return "float";
                    } else if (data_type.startswith("decimal")) {
                        return "decimal";
                    } else if (data_type.startswith("double")) {
                        return "double precision";
                    } else {
                        return data_type;
                    }
                }
                
                public virtual object _load_columns() {
                    object res;
                    var fields = new List<object>();
                    foreach (var row in this.reader.db.query(String.Format("SHOW FULL COLUMNS FROM `%s`", this.name))) {
                        res = Tuple.Create("<Empty>");
                        foreach (var field in row) {
                            if (type(field) == unicode) {
                                res += field.encode("utf8");
                            } else {
                                res += field;
                            }
                        }
                        var length_match = re_column_length.search(res[1]);
                        var precision_match = re_column_precision.search(res[1]);
                        var length = length_match ? length_match.group(1) : precision_match ? precision_match.group(1) : null;
                        var name = res[0].lower();
                        var comment = res[8];
                        var field_type = this._convert_type(res[1]);
                        var desc = new Dictionary<object, object> {
                            {
                                "name",
                                name},
                            {
                                "table_name",
                                this.name},
                            {
                                "type",
                                field_type},
                            {
                                "length",
                                length ? Convert.ToInt32(length) : null},
                            {
                                "decimals",
                                precision_match ? precision_match.group(2) : null},
                            {
                                "null",
                                res[3] == "YES" || field_type.startswith("enum") || Tuple.Create("date", "datetime", "timestamp").Contains(field_type)},
                            {
                                "primary_key",
                                res[4] == "PRI"},
                            {
                                "auto_increment",
                                res[6] == "auto_increment"},
                            {
                                "default",
                                !(res[5] == "NULL") ? res[5] : null},
                            {
                                "comment",
                                comment},
                            {
                                "select",
                                !field_type.startswith("enum") ? String.Format("`%s`", name) : String.Format("CASE `%(name)s` WHEN \"\" THEN NULL ELSE `%(name)s` END", new Dictionary<object, object> {
                                    {
                                        "name",
                                        name}})}};
                        fields.append(desc);
                    }
                    foreach (var field in from f in fields
                        where f["auto_increment"]
                        select f) {
                        res = this.reader.db.query(String.Format("SELECT MAX(`%s`) FROM `%s`;", field["name"], this.name), one: true);
                        field["maxval"] = res[0] ? Convert.ToInt32(res[0]) : 0;
                    }
                    return fields;
                }
                
                public virtual object _load_table_status() {
                    return this.reader.db.query(String.Format("SHOW TABLE STATUS WHERE Name=\"%s\"", this.name), one: true);
                }
                
                public virtual object _load_table_rows() {
                    var rows = this.reader.db.query(String.Format("SELECT COUNT(*) FROM `%s`;", this.name), one: true);
                    return rows[0] ? Convert.ToInt32(rows[0]) : 0;
                }
                
                public virtual object _load_indexes() {
                    var explain = this.reader.db.query(String.Format("SHOW CREATE TABLE `%s`", this.name), one: true);
                    explain = explain[1];
                    foreach (var line in explain.split("\n")) {
                        if (!line.Contains(" KEY ")) {
                            continue;
                        }
                        var index = new Dictionary<object, object> {
                        };
                        var match_data = re_key_1.search(line);
                        if (match_data) {
                            index["name"] = match_data.group(1);
                            index["column"] = match_data.group(2).lower();
                            index["ref_table"] = match_data.group(3);
                            index["ref_column"] = match_data.group(4);
                            this._foreign_keys.append(index);
                            continue;
                        }
                        match_data = re_key_2.search(line);
                        if (match_data) {
                            index["name"] = match_data.group(1);
                            index["columns"] = (from col in match_data.group(2).split(",")
                                select re.search(@"`(\w+)`", col.lower()).group(1)).ToList();
                            index["unique"] = line.Contains("UNIQUE");
                            this._indexes.append(index);
                            continue;
                        }
                        match_data = re_key_3.search(line);
                        if (match_data) {
                            index["primary"] = true;
                            index["columns"] = (from col in match_data.group(1).split(",")
                                select re.sub(@"\(\d+\)", "", col.lower().replace("`", ""))).ToList();
                            this._indexes.append(index);
                            continue;
                        }
                    }
                }
                
                public virtual object _load_triggers() {
                    var explain = this.reader.db.query(String.Format("SHOW TRIGGERS WHERE `table` = \'%s\'", this.name));
                    foreach (var row in explain) {
                        if (object.ReferenceEquals(type(row), tuple)) {
                            var trigger = new Dictionary<object, object> {
                            };
                            trigger["name"] = row[0];
                            trigger["event"] = row[1];
                            trigger["statement"] = row[3];
                            trigger["timing"] = row[4];
                            trigger["statement"] = re.sub("^BEGIN", "", trigger["statement"]);
                            trigger["statement"] = re.sub("^END", "", trigger["statement"], flags: re.MULTILINE);
                            trigger["statement"] = re.sub("`", "", trigger["statement"]);
                            this._triggers.append(trigger);
                        }
                    }
                }
                
                public object schema {
                    get {
                        return this._schema;
                    }
                }
                
                public object name {
                    get {
                        return this._name;
                    }
                }
                
                public object columns {
                    get {
                        return this._columns;
                    }
                }
                
                public object rows {
                    get {
                        return this._rows;
                    }
                }
                
                public object comment {
                    get {
                        return this._comment;
                    }
                }
                
                public object indexes {
                    get {
                        return this._indexes;
                    }
                }
                
                public object foreign_keys {
                    get {
                        return this._foreign_keys;
                    }
                }
                
                public object triggers {
                    get {
                        return this._triggers;
                    }
                }
                
                public object query_for {
                    get {
                        return String.Format("SELECT %(column_names)s FROM `%(table_name)s`", new Dictionary<object, object> {
                            {
                                "table_name",
                                this.name},
                            {
                                "column_names",
                                ", ".join(from c in this.columns
                                    select c["select"])}});
                    }
                }
            }
            
            public MysqlReader(object options) {
                this.db = new DB(options.file_options["mysql"]);
                this.exclude_tables = options.file_options.get("exclude_tables", new List<object>());
                this.only_tables = options.file_options.get("only_tables", new List<object>());
            }
            
            public object tables {
                get {
                    return from t in from t in this.db.list_tables()
                        where !this.exclude_tables.Contains(t[0])
                        select t
                        where !this.only_tables || this.only_tables.Contains(t[0])
                        select new Table(this, t[0]);
                }
            }
            
            public virtual object read(object table) {
                return this.db.query(table.query_for, large: true);
            }
            
            public virtual object close() {
                this.db.close();
            }
        }
    }
}
