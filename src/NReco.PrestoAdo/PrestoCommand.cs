using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data.Common;
using System.Data;
using System.Threading;
using System.Text;

namespace NReco.PrestoAdo {
    public class PrestoCommand : DbCommand {

        private readonly CancellationTokenSource cts = new CancellationTokenSource();
        private readonly PrestoDbParameterCollection commandParameters = new PrestoDbParameterCollection();
        private PrestoConnection connection;

        public PrestoCommand() {
        }

        public PrestoCommand(PrestoConnection connection) {
            this.connection = connection;
        }

        public override string CommandText { get; set; }

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection DbConnection {
            get => connection;
            set => connection = (PrestoConnection)value;
        }

        protected override DbParameterCollection DbParameterCollection => commandParameters;

        protected override DbTransaction DbTransaction { get; set; }

        public new void Dispose() {
            cts?.Dispose();
            base.Dispose();
        }

        public override void Cancel() => cts.Cancel();

        public override int ExecuteNonQuery() => ExecuteNonQueryAsync(CancellationToken.None).GetAwaiter().GetResult();

        public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken) {
            if (connection == null)
                throw new InvalidOperationException("Connection is not set");

            using (var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken)) {
                var result = await connection.ExecuteQueryAsync(this, linkedCancellationTokenSource.Token).ConfigureAwait(false);
            }
            // ExecuteNonQuery may be used for INSERTs, let's return 1 affected record
            return 1;
        }

        public override object ExecuteScalar() => ExecuteScalarAsync(CancellationToken.None).GetAwaiter().GetResult();

        public override async Task<object> ExecuteScalarAsync(CancellationToken cancellationToken) {
            using (var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken)) {
                using (var reader = await ExecuteDbDataReaderAsync(CommandBehavior.Default, linkedCancellationTokenSource.Token).ConfigureAwait(false)) {
                    return reader.Read() ? reader.GetValue(0) : null;
                }
            }
        }

        public override void Prepare() { /* ClickHouse has no notion of prepared statements */ }

        public new PrestoDbParameter CreateParameter() => new PrestoDbParameter();

        protected override DbParameter CreateDbParameter() => CreateParameter();

        protected override void Dispose(bool disposing) {
            if (disposing) {
                cts.Dispose();
            }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => ExecuteDbDataReaderAsync(behavior, CancellationToken.None).GetAwaiter().GetResult();

        protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) {
            if (connection == null)
                throw new InvalidOperationException("Connection is not set");

            using (var linkedCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, cancellationToken)) {
                var sqlBuilder = new StringBuilder(CommandText);
                switch (behavior) {
                    case CommandBehavior.SingleRow:
                    case CommandBehavior.SingleResult:
                        sqlBuilder.Append(" LIMIT 1");
                        break;
                    case CommandBehavior.SchemaOnly:
                        sqlBuilder.Append(" LIMIT 0");
                        break;
                    default:
                        break;
                }
                var result = await connection.ExecuteQueryAsync(this, linkedCancellationTokenSource.Token).ConfigureAwait(false);
                return new PrestoDbDataReader(result);
            }
        }
    }
}
