namespace lib {
    
    using @absolute_import = @@__future__.absolute_import;
    
    using sys;
    
    using wraps = functools.wraps;
    
    using MysqlReader = mysql_reader.MysqlReader;
    
    using cprint = termcolor.cprint;
    
    using System;
    
    using System.Linq;
    
    using System.Collections.Generic;
    
    using System.Collections;
    
    using System.Diagnostics;
    
    public static class @__init__ {
        
        public static object print_row_progress(object val) {
            try {
                cprint(String.Format("  %s", val), "cyan", end: " ");
            } catch (NameError) {
                Console.Write(String.Format("  %s", val));
            }
            sys.stdout.flush();
        }
        
        public static object print_start_table(object val) {
            try {
                cprint(val, "magenta");
            } catch (NameError) {
                Console.WriteLine(val);
            }
        }
        
        public static object print_table_actions(object val) {
            try {
                cprint(String.Format("  %s", val), "green");
            } catch (NameError) {
                Console.WriteLine(String.Format("  %s", val));
            }
        }
        
        public static object find_first(object items, object func) {
            return next(from item in items
                where func(item)
                select item, null);
        }
        
        public static object print_red(object val) {
            try {
                cprint(val, "red");
            } catch (NameError) {
                Console.WriteLine(val);
            }
        }
        
        public static object status_logger(object f) {
            var start_template = "START  - %s";
            var finish_template = "FINISH - %s";
            var truncate_template = "TRUNCATING TABLE %s";
            var create_template = "CREATING TABLE %s";
            var constraints_template = "ADDING CONSTRAINTS ON %s";
            var write_contents_template = "WRITING DATA TO %s";
            var index_template = "ADDING INDEXES TO %s";
            var trigger_template = "ADDING TRIGGERS TO %s";
            var statuses = new Dictionary<object, object> {
                {
                    "truncate",
                    new Dictionary<object, object> {
                        {
                            "start",
                            start_template % truncate_template},
                        {
                            "finish",
                            finish_template % truncate_template}}},
                {
                    "write_table",
                    new Dictionary<object, object> {
                        {
                            "start",
                            start_template % create_template},
                        {
                            "finish",
                            finish_template % create_template}}},
                {
                    "write_constraints",
                    new Dictionary<object, object> {
                        {
                            "start",
                            start_template % constraints_template},
                        {
                            "finish",
                            finish_template % constraints_template}}},
                {
                    "write_contents",
                    new Dictionary<object, object> {
                        {
                            "start",
                            start_template % write_contents_template},
                        {
                            "finish",
                            finish_template % write_contents_template}}},
                {
                    "write_indexes",
                    new Dictionary<object, object> {
                        {
                            "start",
                            start_template % index_template},
                        {
                            "finish",
                            finish_template % index_template}}},
                {
                    "write_triggers",
                    new Dictionary<object, object> {
                        {
                            "start",
                            start_template % trigger_template},
                        {
                            "finish",
                            finish_template % trigger_template}}}};
            Func<object, object, object> decorated_function = (kwargs,args) => {
                object table;
                if (getattr(args[0], "verbose", false)) {
                    if (kwargs.Contains("table")) {
                        table = kwargs["table"];
                    } else {
                        table = find_first(args.ToList() + kwargs.values(), c => object.ReferenceEquals(c.@__class__, MysqlReader.Table));
                    }
                    Debug.Assert(table);
                    print_table_actions(statuses[f.func_name]["start"] % table.name);
                    var ret = f(args, kwargs);
                    print_table_actions(statuses[f.func_name]["finish"] % table.name);
                    return ret;
                } else {
                    return f(args, kwargs);
                }
            };
            return decorated_function;
        }
    }
}
