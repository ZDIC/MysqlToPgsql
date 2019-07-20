namespace lib {
    
    public static class errors {
        
        public class GeneralException
            : Exception {
        }
        
        public class ConfigurationException
            : Exception {
        }
        
        public class UninitializedValueError
            : GeneralException {
        }
        
        public class ConfigurationFileNotFound
            : ConfigurationException {
        }
        
        public class ConfigurationFileInitialized
            : ConfigurationException {
        }
    }
}
