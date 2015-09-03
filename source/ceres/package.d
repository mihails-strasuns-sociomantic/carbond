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

module ceres;

public import ceres.tree;
public import ceres.metadata;
public import ceres.node;

// Ceres requires D 2.066 or newer
static assert(__VERSION__ >= 2066);

///
enum defaultTimeStep = 60;

///
shared uint maxSliceGap = 80;

///
enum datapointSize = double.sizeof;

///
struct SliceData
{
  long startTime;
  long timeStep;

  // Implements .sort
  int opCmp(const SliceData other) const
  {
    if (this.startTime < other.startTime)
      return -1;
    else if (this.startTime > other.startTime)
      return 1;
    return 0;
  }
}

///
struct Datapoint
{
  long timestamp;
  double value;
}

///
class CorruptNode : Exception
{
  @safe pure nothrow this(string msg, string file = __FILE__, size_t line = __LINE__)
  {
    super(msg, file, line);
  }
}

///
class NodeNotFound : Exception
{
  @safe pure nothrow this(string msg, string file = __FILE__, size_t line = __LINE__)
  {
    super(msg, file, line);
  }
}

///
class NodeDeleted : Exception
{
  @safe pure nothrow this(string msg, string file = __FILE__, size_t line = __LINE__)
  {
    super(msg, file, line);
  }
}

///
class SliceGapTooLarge : Exception
{
  @safe pure nothrow this(string msg = "For internal use only", string file = __FILE__, size_t line = __LINE__)
  {
    super(msg, file, line);
  }
}

///
class SliceDeleted : Exception
{
  @safe pure nothrow this(string msg, string file = __FILE__, size_t line = __LINE__)
  {
    super(msg, file, line);
  }
}

// vim: set sw=2 sts=2 tw=120 et cin :
