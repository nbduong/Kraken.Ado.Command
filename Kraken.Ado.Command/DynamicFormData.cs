using System;
using System.Collections.Generic;
using System.Dynamic;

namespace Kraken.Ado.Command
{
    public class DynamicFormData : DynamicObject
    {
        private readonly Dictionary<string, object> _fields = new Dictionary<string, object>();

        public int Count => _fields.Keys.Count;

        public void Add(string name, string val = null)
        {
            if (!_fields.ContainsKey(name))
            {
                _fields.Add(name, val);
            }
            else
            {
                _fields[name] = val;
            }
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            if (_fields.ContainsKey(binder.Name))
            {
                result = _fields[binder.Name];
                return true;
            }
            return base.TryGetMember(binder, out result);
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            if (!_fields.ContainsKey(binder.Name))
            {
                _fields.Add(binder.Name, value);
            }
            else
            {
                _fields[binder.Name] = value;
            }
            return true;
        }

        public override bool TryInvokeMember(InvokeMemberBinder binder, object[] args, out object result)
        {
            if (_fields.ContainsKey(binder.Name) &&
                _fields[binder.Name] is Delegate)
            {
                var del = _fields[binder.Name] as Delegate;
                result = del.DynamicInvoke(args);
                return true;
            }
            return base.TryInvokeMember(binder, args, out result);
        }
    }
}