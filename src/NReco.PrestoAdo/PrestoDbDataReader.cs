using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Threading.Tasks;
using System.Data.Common;
using System.Data;
using BAMCIS.PrestoClient;
using BAMCIS.PrestoClient.Serialization;
using BAMCIS.PrestoClient.Model.Client;
using System.Threading;

namespace NReco.PrestoAdo {

    public class PrestoDbDataReader : DbDataReader {

        PrestoConnection.PrestoCommandResults qResults;
        Column[] Columns;
        IEnumerator<List<object>> DataEnumerator;

        protected List<object> CurrentRow { get; set; }

        protected string[] FieldNames { get; set; }

        IReadOnlyDictionary<string, int> FieldNameToIndex;

        internal PrestoDbDataReader(PrestoConnection.PrestoCommandResults results) {
            qResults = results;
            Columns = qResults.Columns.ToArray();
            FieldNames = Columns.Select(c => c.Name).ToArray();
            var fldNameToIdx = new Dictionary<string, int>(FieldNames.Length);
            for (int i = 0; i < FieldNames.Length; i++)
                fldNameToIdx[FieldNames[i]] = i;
            FieldNameToIndex = fldNameToIdx;
        }

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => this[GetOrdinal(name)];

        public override int Depth { get; }

        public override int FieldCount => Columns.Length;

        public override bool IsClosed => false;

        public sealed override bool HasRows => true;

        public override int RecordsAffected { get; }

        public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetValue(ordinal), CultureInfo.InvariantCulture);

        public override byte GetByte(int ordinal) => (byte)GetValue(ordinal);

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

        public override char GetChar(int ordinal) => (char)GetValue(ordinal);

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => throw new NotImplementedException();

        public override string GetDataTypeName(int ordinal) => Columns[ordinal].Type;

        public override DateTime GetDateTime(int ordinal) => (DateTime)GetValue(ordinal);

        public override decimal GetDecimal(int ordinal) => (decimal)GetValue(ordinal);

        public override double GetDouble(int ordinal) => (double)GetValue(ordinal);

        public override IEnumerator GetEnumerator() => CurrentRow.GetEnumerator();

        public override Type GetFieldType(int ordinal) {
            var col = Columns[ordinal];
            if (PrestoTypeMapping.Types.TryGetValue(col.Type.ToLower(), out var mappingInfo))
                return mappingInfo.DotNetType;
            return typeof(object);
        }

        public override float GetFloat(int ordinal) => (float)GetValue(ordinal);

        public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);

        public override short GetInt16(int ordinal) => (short)GetValue(ordinal);

        public override int GetInt32(int ordinal) => (int)GetValue(ordinal);

        public override long GetInt64(int ordinal) => (long)GetValue(ordinal);

        public override string GetName(int ordinal) => FieldNames[ordinal];

        public override int GetOrdinal(string name) {
            if (FieldNameToIndex.TryGetValue(name, out var idx))
                return idx;
            throw new IndexOutOfRangeException($"Unknown column '{name}'");
        }

        public override string GetString(int ordinal) => (string)GetValue(ordinal);

        public override object GetValue(int ordinal) => CurrentRow[ordinal];

        public override int GetValues(object[] values) {
            if (CurrentRow == null) {
                throw new InvalidOperationException();
            }
            CurrentRow.CopyTo(values, 0);
            return CurrentRow.Count;
        }

        public override bool IsDBNull(int ordinal) => GetValue(ordinal) is DBNull || GetValue(ordinal) is null;

        public override bool NextResult() => false;

        public override void Close() => Dispose();

        public override T GetFieldValue<T>(int ordinal) => (T)GetValue(ordinal);

        DataTable _SchemaTable = null;

        public override DataTable GetSchemaTable() {
            return _SchemaTable ?? (_SchemaTable = BuildSchemaTable());
        }

        internal DataTable BuildSchemaTable() {
            var schemaTable = new DataTable("SchemaTable");

            var ColumnName = new DataColumn(SchemaTableColumn.ColumnName, typeof(string));
            var ColumnOrdinal = new DataColumn(SchemaTableColumn.ColumnOrdinal, typeof(int));
            var ColumnSize = new DataColumn(SchemaTableColumn.ColumnSize, typeof(int));
            var NumericPrecision = new DataColumn(SchemaTableColumn.NumericPrecision, typeof(short));
            var NumericScale = new DataColumn(SchemaTableColumn.NumericScale, typeof(short));

            var DataType = new DataColumn(SchemaTableColumn.DataType, typeof(Type));
            var DataTypeName = new DataColumn("DataTypeName", typeof(string));

            var IsLong = new DataColumn(SchemaTableColumn.IsLong, typeof(bool));
            var AllowDBNull = new DataColumn(SchemaTableColumn.AllowDBNull, typeof(bool));

            var IsUnique = new DataColumn(SchemaTableColumn.IsUnique, typeof(bool));
            var IsKey = new DataColumn(SchemaTableColumn.IsKey, typeof(bool));
            var IsAutoIncrement = new DataColumn(SchemaTableOptionalColumn.IsAutoIncrement, typeof(bool));

            var BaseCatalogName = new DataColumn(SchemaTableOptionalColumn.BaseCatalogName, typeof(string));
            var BaseSchemaName = new DataColumn(SchemaTableColumn.BaseSchemaName, typeof(string));
            var BaseTableName = new DataColumn(SchemaTableColumn.BaseTableName, typeof(string));
            var BaseColumnName = new DataColumn(SchemaTableColumn.BaseColumnName, typeof(string));

            var BaseServerName = new DataColumn(SchemaTableOptionalColumn.BaseServerName, typeof(string));
            var IsAliased = new DataColumn(SchemaTableColumn.IsAliased, typeof(bool));
            var IsExpression = new DataColumn(SchemaTableColumn.IsExpression, typeof(bool));

            var columns = schemaTable.Columns;

            columns.Add(ColumnName);
            columns.Add(ColumnOrdinal);
            columns.Add(ColumnSize);
            columns.Add(NumericPrecision);
            columns.Add(NumericScale);
            columns.Add(IsUnique);
            columns.Add(IsKey);
            columns.Add(BaseServerName);
            columns.Add(BaseCatalogName);
            columns.Add(BaseColumnName);
            columns.Add(BaseSchemaName);
            columns.Add(BaseTableName);
            columns.Add(DataType);
            columns.Add(DataTypeName);
            columns.Add(AllowDBNull);
            columns.Add(IsAliased);
            columns.Add(IsExpression);
            columns.Add(IsAutoIncrement);
            columns.Add(IsLong);

            for (int i = 0; i < Columns.Length; i++) {
                var schemaRow = schemaTable.NewRow();

                schemaRow[ColumnName] = Columns[i].Name;
                schemaRow[ColumnOrdinal] = i;
                schemaRow[ColumnSize] = DBNull.Value;
                schemaRow[NumericPrecision] = DBNull.Value;
                schemaRow[NumericScale] = DBNull.Value;
                schemaRow[BaseServerName] = DBNull.Value;
                schemaRow[BaseCatalogName] = DBNull.Value;
                schemaRow[BaseColumnName] = Columns[i].Name;
                schemaRow[BaseSchemaName] = DBNull.Value;
                schemaRow[BaseTableName] = DBNull.Value;
                schemaRow[DataType] = GetFieldType(i);
                schemaRow[DataTypeName] = Columns[i].Type;
                schemaRow[IsAliased] = false;
                schemaRow[IsExpression] = false;
                schemaRow[IsLong] = DBNull.Value;
                schemaRow[IsKey] = false;
                schemaRow[AllowDBNull] = true;
                schemaRow[IsAutoIncrement] = false;

                schemaTable.Rows.Add(schemaRow);
            }
            return schemaTable;
        }

        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);

        // Custom extension
        public ushort GetUInt16(int ordinal) => (ushort)GetValue(ordinal);

        // Custom extension
        public uint GetUInt32(int ordinal) => (uint)GetValue(ordinal);

        // Custom extension
        public ulong GetUInt64(int ordinal) => (ulong)GetValue(ordinal);

        public override bool Read() {
            if (DataEnumerator == null)
                DataEnumerator = qResults.GetData().GetEnumerator();
            if (!DataEnumerator.MoveNext()) {
                CurrentRow = null;
                
                return false;
            }
            CurrentRow = DataEnumerator.Current;
            return true;
        }

        protected override void Dispose(bool disposing) {
            if (DataEnumerator != null) {
                DataEnumerator.Dispose();
                DataEnumerator = null;
            }
        }

    }

}
