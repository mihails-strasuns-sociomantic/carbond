// Copyright 2015 Iain Buclaw
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

module ceres.metadata;

// MetaValue is currently implemented as a Variant.
// Metadata is currently implemented as a built-in AA.
private
{
  import std.json;
  import std.variant;
  alias MetaValue = Variant;
}

/// Represents an arbitrary key-value store as tree metadata
alias CeresMetadata = MetaValue[string];


///
inout(T) get(S, string, T)(S metadata, string prop, lazy inout(T) defaultValue)
{
  auto p = prop in metadata;
  return p ? p.coerce!(T) : defaultValue;
}

///
string jsonDump(CeresMetadata metadata)
{
  import std.array : appender, replaceSlice;
  auto app = appender!(string);

  app.put("{ ");
  foreach(key, value; metadata)
  {
    app.put(`"`); app.put(key); app.put(`" : `);
    bool isString = value.convertsTo!(string);
    if (isString) app.put(`"`);
    app.put(value.toString());
    if (isString) app.put(`"`);
    app.put(", ");
  }
  return app.data.replaceSlice(app.data[$-2..$], " }");
}

///
CeresMetadata jsonLoad(string json)
{
  CeresMetadata metadata;
  foreach(string key, val; parseJSON(json))
  {
    final switch (val.type)
    {
      case JSON_TYPE.STRING:
        metadata[key] = val.str;
        break;
      case JSON_TYPE.INTEGER:
        metadata[key] = val.integer;
        break;
      case JSON_TYPE.UINTEGER:
        metadata[key] = val.uinteger;
        break;
      case JSON_TYPE.FLOAT:
        metadata[key] = val.floating;
        break;
      case JSON_TYPE.OBJECT:
        metadata[key] = val.object;
        break;
      case JSON_TYPE.ARRAY:
        metadata[key] = val.array;
        break;
      case JSON_TYPE.TRUE:
        metadata[key] = true;
        break;
      case JSON_TYPE.FALSE:
        metadata[key] = false;
        break;
      case JSON_TYPE.NULL:
        metadata[key] = null;
        break;
    }
  }
  return metadata;
}

// vim: set sw=2 sts=2 tw=120 et cin :
