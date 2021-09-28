using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace NReco.PrestoAdo {

    public class PrestoDbParameter : DbParameter {
        public override DbType DbType { get; set; }

        public override ParameterDirection Direction { get => ParameterDirection.Input; set { } }

        public override bool IsNullable { get; set; }

        public override string ParameterName { get; set; }

        public override int Size { get; set; }

        public override string SourceColumn { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override object Value { get; set; }

        public override void ResetDbType() { }

        public override string ToString() => $"{ParameterName}:{Value}";
    }
}
