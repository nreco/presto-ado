using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data.Common;

namespace NReco.PrestoAdo {
    public class PrestoConnectionStringBuilder : DbConnectionStringBuilder {
        public PrestoConnectionStringBuilder() {
        }

        public PrestoConnectionStringBuilder(string connectionString) {
            ConnectionString = connectionString;
        }

        public string Host {
            get => TryGetValue("Host", out var value) ? value as string : "localhost";
            set => this["Host"] = value;
        }

        public ushort Port {
            get => TryGetValue("Port", out var value) && value is string @string && ushort.TryParse(@string, out var @ushort) ? @ushort : (ushort)8080;
            set => this["Port"] = value;
        }

        public bool TrinoHeaders {
            get => TryGetValue("TrinoHeaders", out var value) ? IsYesTrueOne(value) : false;
            set => this["TrinoHeaders"] = value;
        }

        public string User {
            get => TryGetValue("User", out var value) ? value as string : String.Empty;
            set => this["User"] = value;
        }

        public string Password {
            get => TryGetValue("Password", out var value) ? value as string : string.Empty;
            set => this["Password"] = value;
        }

        public string Catalog {
            get => TryGetValue("Catalog", out var value) ? value as string : null;
            set => this["Catalog"] = value;
        }

        public string Schema {
            get => TryGetValue("Schema", out var value) ? value as string : "default";
            set => this["Schema"] = value;
        }

        public int CheckInterval {
            get => TryGetValue("CheckInterval", out var value) && value is string @string && int.TryParse(@string, out var @int) ? @int : (int)800;
            set => this["CheckInterval"] = value;
        }

        public bool IgnoreSslErrors {
            get => TryGetValue("IgnoreSslErrors", out var value) ? IsYesTrueOne(value) : false;
            set => this["IgnoreSslErrors"] = value;
        }

        public bool UseSsl {
            get => TryGetValue("UseSsl", out var value) ? "true".Equals(value as string, StringComparison.OrdinalIgnoreCase) : false;
            set => this["UseSsl"] = value;
        }

        public int ClientRequestTimeout {
            get => TryGetValue("ClientRequestTimeout", out var value) && value is string @string && int.TryParse(@string, out var @int) ? @int : -1;
            set => this["ClientRequestTimeout"] = value;
        }

        bool IsYesTrueOne(object val) {
            var valStr = Convert.ToString(val);
			return valStr == "1"
				|| "one".Equals(valStr, StringComparison.OrdinalIgnoreCase)
				|| "true".Equals(valStr, StringComparison.OrdinalIgnoreCase);
		}
	}
}
