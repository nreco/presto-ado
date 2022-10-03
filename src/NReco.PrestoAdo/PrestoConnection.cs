using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data.Common;
using System.Data;
using BAMCIS.PrestoClient;
using BAMCIS.PrestoClient.Model.Statement;
using BAMCIS.PrestoClient.Model.Client;
using System.Threading;
using System.Text;

namespace NReco.PrestoAdo {

    public class PrestoConnection : DbConnection {

        private PrestoClientSessionConfig config;
        private PrestodbClient client;
        private string connectionString;
        private ConnectionState state = ConnectionState.Closed;

        public PrestoConnection()
            : this(string.Empty) {
        }

        public PrestoConnection(string connectionString) {
            ConnectionString = connectionString;
        }

        public sealed override string ConnectionString {
            get {
                return connectionString;
            }
            set {
                var builder = new PrestoConnectionStringBuilder(value);
                config = new PrestoClientSessionConfig() {
                    Host = builder.Host,
                    Port = builder.Port,
                    Catalog = builder.Catalog,
                    Schema = builder.Schema,
                    CheckInterval = builder.CheckInterval,
                    IgnoreSslErrors = builder.IgnoreSslErrors,
                    UseSsl = builder.UseSsl,
                    ClientRequestTimeout = builder.ClientRequestTimeout,
                    EnableLegacyHeaderCompatibility = !builder.TrinoHeaders
                };
	            if (!String.IsNullOrEmpty(builder.User))
				    config.User = builder.User;
				if (!String.IsNullOrEmpty(builder.Password))
                    config.Password = builder.Password;

                client = new PrestodbClient(config);
            }
        }

        public override ConnectionState State => state;

        public override string Database => config?.Catalog;

        public override void Close() => state = ConnectionState.Closed;

        public override string DataSource => "";

        public override string ServerVersion => "";

        public override void Open() => state = ConnectionState.Open;

        public override void ChangeDatabase(string databaseName) {
            throw new NotSupportedException();
        }

        public override async Task OpenAsync(System.Threading.CancellationToken token) {
            if (State == ConnectionState.Open)
                return;
            state = ConnectionState.Open;
        }

        public new PrestoCommand CreateCommand() => new PrestoCommand(this);

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => CreateCommand();

        internal async Task<PrestoCommandResults> ExecuteQueryAsync(PrestoCommand cmd, CancellationToken ct) {
            var sqlText = cmd.CommandText;
            var cmdParams = new Dictionary<string, string>(cmd.Parameters.Count);
            foreach (DbParameter p in cmd.Parameters) {
                cmdParams[p.ParameterName] = "'"+Convert.ToString(p.Value, System.Globalization.CultureInfo.InvariantCulture).Replace("'", "''")+"'";
            }
            ExecuteQueryV1Request request = new ExecuteQueryV1Request(SubstituteParameters(sqlText, cmdParams));
            ExecuteQueryV1Response queryResponse = await client.ExecuteQueryV1(request, ct);
            return new PrestoCommandResults(queryResponse.Columns, () => queryResponse.Data);
        }

        private static string SubstituteParameters(string query, IDictionary<string, string> parameters) {
            var builder = new StringBuilder(query.Length);
            var paramStartPos = query.IndexOf('{');
            var paramEndPos = -1;
            while (paramStartPos != -1) {
                builder.Append(query.Substring(paramEndPos + 1, paramStartPos - paramEndPos - 1));

                paramStartPos += 1;
                paramEndPos = query.IndexOf('}', paramStartPos);
                var param = query.Substring(paramStartPos, paramEndPos - paramStartPos);

                if (!parameters.TryGetValue(param, out var value)) {
                    // doesn't match a parameter. leave as is
                    value = "{" + param + "}";
                }

                builder.Append(value);
                paramStartPos = query.IndexOf('{', paramEndPos);
            }
            builder.Append(query.Substring(paramEndPos + 1, query.Length - paramEndPos - 1));
            return builder.ToString();
        }

        internal class PrestoCommandResults {

            internal IReadOnlyList<Column> Columns { get; private set; }

            Func<IEnumerable<List<object>>> getData;

            public PrestoCommandResults(IReadOnlyList<Column> cols, Func<IEnumerable<List<object>>> getData) {
                Columns = cols;
                this.getData = getData;
			}

            public IEnumerable<List<object>> GetData() {
                return getData();
			}
		}

    }


}
