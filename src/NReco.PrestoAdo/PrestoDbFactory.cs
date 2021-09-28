using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data.Common;

namespace NReco.PrestoAdo {

    public sealed class PrestoDbFactory : DbProviderFactory {
        public static readonly PrestoDbFactory Instance = new PrestoDbFactory();

        /// <summary>
        /// Returns a strongly typed <see cref="DbCommand"/> instance.
        /// </summary>
        public override DbCommand CreateCommand() => new PrestoCommand();

        /// <summary>
        /// Returns a strongly typed <see cref="DbConnection"/> instance.
        /// </summary>
        public override DbConnection CreateConnection() => new PrestoConnection();

        /// <summary>
        /// Returns a strongly typed <see cref="DbParameter"/> instance.
        /// </summary>
        public override DbParameter CreateParameter() => new PrestoDbParameter();

        /// <summary>
        /// Returns a strongly typed <see cref="DbConnectionStringBuilder"/> instance.
        /// </summary>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new PrestoConnectionStringBuilder();

    }

}
