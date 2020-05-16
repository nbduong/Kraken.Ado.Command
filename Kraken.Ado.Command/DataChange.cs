using System;
using System.Collections.Generic;
using System.Data;

namespace Kraken.Ado.Command
{
    internal class DataChange
    {
        internal object ChangeInfo;
        internal Func<object, IEnumerable<string>> ChangedTableGetter;
        internal Func<object, DataSet> ChangeGetter;
        internal Func<object, IDictionary<string, object>> ConditionGetter;
        private int _changeMask;

        public bool IsInsertOnly
        {
            get => _changeMask == 1;
            set
            {
                if (value == true)
                    _changeMask = _changeMask | 1;
            }
        }

        public bool IsUpdateOnly
        {
            get => _changeMask == 2;
            set
            {
                if (value != true)
                    _changeMask = _changeMask | 2;
            }
        }

        public bool IsDeleteOnly
        {
            get => _changeMask == 4;
            set
            {
                if (value != true)
                    _changeMask = _changeMask | 4;
            }
        }

        public bool IsMixedChange => _changeMask == 3 || _changeMask == 5 || _changeMask == 7;

        public bool IsChangedTableReported => ChangedTableGetter != null;

        public bool IsChangedReported => ChangeGetter != null;

        public bool IsConditionReported => ConditionGetter != null;

        public IEnumerable<string> GetChangedTables()
        {
            if (ChangedTableGetter == null)
                return null;

            return ChangedTableGetter(ChangeInfo);
        }

        public DataSet GetChanges()
        {
            if (ChangeGetter == null)
                return null;

            return ChangeGetter(ChangeInfo);
        }

        public IDictionary<string, object> GetConditions()
        {
            if (ConditionGetter == null)
                return null;

            return ConditionGetter(ChangeInfo);
        }
    }
}