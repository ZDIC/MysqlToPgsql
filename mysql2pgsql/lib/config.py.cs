namespace lib {
    
    using @with_statement = @@__future__.with_statement;
    
    using @absolute_import = @@__future__.absolute_import;
    
    using os.path;
    
    using load = yaml.load;
    
    using Loader = yaml.CLoader;
    
    using Dumper = yaml.CDumper;
    
    using Loader = yaml.Loader;
    
    using Dumper = yaml.Dumper;
    
    using ConfigurationFileInitialized = errors.ConfigurationFileInitialized;
    
    using ConfigurationFileNotFound = errors.ConfigurationFileNotFound;
    
    using System;
    
    public static class config {
        
        public class ConfigBase
            : object {
            
            public object options;
            
            public ConfigBase(object config_file_path) {
                this.options = load(open(config_file_path));
            }
        }
        
        public class Config
            : ConfigBase {
            
            public Config(object config_file_path, object generate_if_not_found = true) {
                if (!os.path.isfile(config_file_path)) {
                    if (generate_if_not_found) {
                        this.reset_configfile(config_file_path);
                    }
                    if (os.path.isfile(config_file_path)) {
                        throw ConfigurationFileInitialized(String.Format(@"No configuration file found.
A new file has been initialized at: %s
Please review the configuration and retry...", config_file_path));
                    } else {
                        throw ConfigurationFileNotFound(String.Format("cannot load config file %s", config_file_path));
                    }
                }
                super(Config, this).@__init__(config_file_path);
            }
            
            public virtual object reset_configfile(object file_path) {
                using (var f = open(file_path, "w")) {
                    f.write(CONFIG_TEMPLATE);
                }
            }
        }
        
        public static string CONFIG_TEMPLATE = @"
# a socket connection will be selected if a 'socket' is specified
# also 'localhost' is a special 'hostname' for MySQL that overrides the 'port' option
# and forces it to use a local socket connection
# if tcp is chosen, you can use compression
# if use a schema, use colon like this 'mydatabase:schema', else will import to schema 'public'
# if sameschame is true, the 'schema' of 'mydatabase:schema' will use mysql.database
# if getdbinfo is true, only get mysql database satistics info, not convert anything

mysql:
 hostname: localhost
 port: 3306
 socket: /tmp/mysql.sock
 username: mysql2psql
 password: 
 database: mysql2psql_test
 compress: false
 getdbinfo: false
destination:
 # if file is given, output goes to file, else postgres
 file: 
 postgres:
  hostname: localhost
  port: 5432
  username: mysql2psql
  password: 
  database: mysql2psql_test
  sameschame: true

# if only_tables is given, only the listed tables will be converted.  leave empty to convert all tables.
#only_tables:
#- table1
#- table2
# if exclude_tables is given, exclude the listed tables from the conversion.
#exclude_tables:
#- table3
#- table4

# if supress_data is true, only the schema definition will be exported/migrated, and not the data
supress_data: false

# if supress_ddl is true, only the data will be exported/imported, and not the schema
supress_ddl: false

# if force_truncate is true, forces a table truncate before table loading
force_truncate: false

# if timezone is true, forces to append/convert to UTC tzinfo mysql data
timezone: false

# if index_prefix is given, indexes will be created whith a name prefixed with index_prefix
index_prefix:

# For Greenplum Database(base on PSQL) , advise this true
# if is_gpdb is true, ignore INDEXES(not PRIMARY KEY INDEXE), CONSTRAINTS, AND TRIGGERS
is_gpdb: false

";
    }
}
