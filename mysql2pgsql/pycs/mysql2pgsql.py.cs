
using @absolute_import = @@__future__.absolute_import;

using codecs;

using time;

using os;

using print_red = lib.print_red;

using MysqlReader = lib.mysql_reader.MysqlReader;

using PostgresFileWriter = lib.postgres_file_writer.PostgresFileWriter;

using PostgresDbWriter = lib.postgres_db_writer.PostgresDbWriter;

using Converter = lib.converter.Converter;

using Config = lib.config.Config;

using ConfigurationFileInitialized = lib.errors.ConfigurationFileInitialized;

using System;

public static class mysql2pgsql {
    
    public class Mysql2Pgsql
        : object {
        
        public object execute_error_log;
        
        public object file_options;
        
        public object log_detail;
        
        public string log_head;
        
        public object run_options;
        
        public string satistics_info;
        
        public object total_rows;
        
        public Mysql2Pgsql(object options) {
            this.run_options = options;
            this.total_rows = 0;
            this.satistics_info = "";
            this.log_detail = "";
            this.execute_error_log = "";
            this.log_head = "##########################%s\n##TOTAL Database Rows:[%s]##\n%s##########################";
            try {
                this.file_options = new Config(options.file, true).options;
            } catch (ConfigurationFileInitialized) {
                print_red(e.message);
                throw new ConfigurationFileInitialized();
            }
        }
        
        public virtual object convert() {
            var postgres_options = this.file_options["destination"]["postgres"];
            var postgres_database = postgres_options["database"];
            if (!postgres_database.ToString().Contains(":")) {
                Console.WriteLine(String.Format("\nIMPORT DESTINATION:%s:public\n", postgres_options["database"]));
            } else {
                postgres_database = postgres_database.split(":")[0];
            }
            var start_time = time.time();
            var get_dbinfo = this.file_options["mysql"]["getdbinfo"];
            var same_schame = postgres_options["sameschame"];
            foreach (var database in this.file_options["mysql"]["database"].split(",")) {
                this.file_options["mysql"]["database"] = database;
                if (same_schame) {
                    this.file_options["destination"]["postgres"]["database"] = postgres_database + ":" + database;
                }
                if (get_dbinfo) {
                    this.getMysqlReader();
                } else {
                    this.convert_db();
                }
            }
            var end_time = time.time();
            @"DATABASE SATISTICS INFO OUTPUT INTO FILE [START]";
            var pound_sign = "#" * this.total_rows.ToString().Count;
            var path_log_file = os.getcwd() + os.sep + String.Format("%s_database_sync_info.txt", this._get_time_str());
            Console.WriteLine(String.Format("DATABASE SATISTICS INFO OUTPUT INTO: \n%s\n", path_log_file));
            var logFile = this._get_file(path_log_file);
            logFile.write(String.Format(this.log_head, pound_sign, this.total_rows.ToString(), pound_sign));
            logFile.write(String.Format("\n##Process Time:%s s.##", round(end_time - start_time, 2)));
            logFile.write("\n\nDATABASE SATISTICS INFO:" + this.satistics_info);
            if (!get_dbinfo) {
                logFile.write("\nINDEXES, CONSTRAINTS, AND TRIGGERS DETAIL:" + this.log_detail);
            }
            if (this.execute_error_log) {
                Console.WriteLine("\nPOSTGRES EXECUTE ERROR LOG: \n" + this.execute_error_log);
            } else {
                Console.WriteLine("POSTGRES EXECUTE ERROR LOG: OH YEAH~ NO ERRORS!");
            }
            logFile.close();
            @"DATABASE SATISTICS INFO OUTPUT INTO FILE [FINISH]";
        }
        
        public virtual object getMysqlReader() {
            var reader = new MysqlReader(this);
            @"""Deal data satistics info:";
            var satistics_rows_info = "\n" + this.file_options["mysql"].get("database") + ":%s|TOTAL\n";
            var total_rows = 0;
            foreach (var table in reader.tables) {
                total_rows += table.rows;
                satistics_rows_info += "    " + table.name + String.Format(":%s\n", table.rows);
            }
            this.satistics_info += satistics_rows_info % total_rows;
            this.total_rows += total_rows;
            return reader;
        }
        
        public virtual object convert_db() {
            object writer;
            var reader = this.getMysqlReader();
            if (this.file_options["destination"]["file"]) {
                var filename = this.file_options["destination"]["file"];
                if (filename.endswith(".sql")) {
                    filename = filename[0:: - 4] + "-" + reader.db.options["db"] + ".sql";
                } else {
                    filename += "-" + reader.db.options["db"] + ".sql";
                }
                writer = new PostgresFileWriter(this._get_file(filename), this.run_options.verbose, this.file_options, tz: this.file_options.get("timezone"));
            } else {
                writer = new PostgresDbWriter(this.file_options["destination"]["postgres"], this.run_options.verbose, this.file_options, tz: this.file_options.get("timezone"));
            }
            new Converter(reader, writer, this.file_options, this.run_options.verbose).convert();
            this.log_detail += writer.log_detail;
            if (!this.file_options["destination"]["file"]) {
                this.execute_error_log += writer.execute_error_log;
            } else {
                Console.WriteLine(String.Format("SQLs SCRIPTS OUTPUT INTO: \n%s\%s\n", os.getcwd(), filename));
            }
        }
        
        public virtual object _get_file(object file_path) {
            return codecs.open(file_path, "wb", "utf-8");
        }
        
        public virtual object _get_time_str() {
            var now = Convert.ToInt32(time.time());
            var timeStruct = time.localtime(now);
            var strTime = time.strftime("%Y-%m-%d_%H%M%S", timeStruct);
            return strTime;
        }
    }
}
